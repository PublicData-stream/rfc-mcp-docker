use std::fmt::Write;

use tokio_postgres::Transaction;

use super::{Evaluation, Parameter, Plan};
use crate::{
    SearchError,
    query::{Direction, Family, Field, Match, Predicate},
};

pub(crate) async fn configure_native(tx: &Transaction<'_>) -> Result<(), SearchError> {
    tx.batch_execute(
        "SELECT pg_catalog.set_config('work_mem', least(setting::integer,4096)::text, true)
           FROM pg_catalog.pg_settings WHERE name='work_mem';
         SET LOCAL hash_mem_multiplier = 1;
         SET LOCAL max_parallel_workers_per_gather = 0",
    )
    .await?;
    Ok(())
}

// The order and aliases match the hybrid executor's Source projection.
pub(crate) const HYDRATE: &str = "SELECT m.rfc_id::integer,
    CASE WHEN octet_length(m.title)+coalesce(octet_length(m.stream),0)<=262144 THEN m.title END,
    CASE WHEN octet_length(m.stream)<=262144 THEN m.stream END,
    d.txt_digest::bytea,d.byte_length::bigint,d.encoding,d.notice_policy,d.notice_state::text,
    o.source_url,m.document_version::bytea,
    coalesce(p.method,o.verification_method) AS verification_method,
    p.algorithm AS verification_algorithm,sr.identity::bytea AS policy_identity,
    sr.evidence::text AS policy_evidence,m.metadata_identity::bytea AS policy_metadata,
    coalesce(cap.denied,0) AS policy_denied
    FROM rfc.generation_documents m
    JOIN rfc.document_versions d ON d.identity=m.document_version
    JOIN rfc.source_observations o ON o.identity=m.txt_observation
    LEFT JOIN rfc.generation_source_evidence p ON (p.generation_id,p.rfc_id)=(m.generation_id,m.rfc_id)
    LEFT JOIN rfc.source_rights sr ON $3::boolean AND (sr.generation_id,sr.rfc_id)=(m.generation_id,m.rfc_id)
    LEFT JOIN rfc.source_restrictions cap ON $3::boolean AND cap.rfc_id=m.rfc_id
    WHERE m.generation_id=$1 AND m.rfc_id=ANY($2::integer[])
    ORDER BY m.rfc_id";

const MEMBERS: &str = "WITH members AS MATERIALIZED (
    SELECT m.rfc_id,m.document_version";

const TEXT_STATUS: &str = ",
    (coalesce(cap.denied,0) & 8)=0 AND sr.evidence->>'search'='true'
        AND d.index_version='rfc-original-bytes-pg19-1'
        AND d.line_starts IS NOT NULL AND d.encoding='utf8' AS text_available,
    d.indexing_state='complete' AS text_complete";

const TITLE_TERMS: &str = "ARRAY(
    SELECT term.lexeme FROM pg_catalog.ts_debug('pg_catalog.simple'::regconfig,m.title) token
    CROSS JOIN LATERAL pg_catalog.unnest(token.lexemes) term(lexeme)
    WHERE octet_length(token.token)<2048 AND octet_length(term.lexeme) BETWEEN 1 AND 2047
    ) COLLATE pg_catalog.\"C\"";

const RIGHTS: &str = "
    JOIN rfc.document_versions d ON d.identity=m.document_version
    LEFT JOIN rfc.source_rights sr ON (sr.generation_id,sr.rfc_id)=(m.generation_id,m.rfc_id)
    LEFT JOIN rfc.source_restrictions cap ON cap.rfc_id=m.rfc_id";

const POSTINGS: &str = "
    eligible_versions AS (SELECT DISTINCT document_version FROM members WHERE text_available),
    lexeme_hits AS (
        SELECT p.document_version,q.slot
        FROM query_lexemes q
        JOIN rfc.document_lexemes p ON p.lexeme=q.lexeme COLLATE pg_catalog.\"C\"
        JOIN eligible_versions v ON v.document_version=p.document_version
        GROUP BY p.document_version,q.slot,q.required
        HAVING count(*)=q.required
    ),
    text_hits AS (
        SELECT document_version,array_agg(slot) AS slots
        FROM lexeme_hits GROUP BY document_version
    )";

const RELATION_INPUT: &str = "
    SELECT slot,target,kind,flags >= 5 AS incoming,
        CASE kind
            WHEN 0 THEN 'all_references' WHEN 1 THEN 'classified_references'
            WHEN 2 THEN 'classified_references' WHEN 3 THEN 'updates' WHEN 4 THEN 'obsoletes'
        END AS family,
        CASE kind
            WHEN 0 THEN ARRAY['normative_reference','informative_reference','unclassified_reference']
            WHEN 1 THEN ARRAY['normative_reference']
            WHEN 2 THEN ARRAY['informative_reference']
            WHEN 3 THEN ARRAY['updates']
            WHEN 4 THEN ARRAY['obsoletes']
        END AS kinds";

const RELATIONS: &str = "
    ),
    edges AS (
        SELECT e.source_rfc AS rfc_id,q.slot,e.kind=ANY(q.kinds) AS matched
        FROM relation_input q
        JOIN rfc.rfc_relations e ON e.generation_id=$1 AND e.target_rfc=q.target
        WHERE NOT q.incoming AND e.source_rfc>$2::integer
          AND (e.kind=ANY(q.kinds) OR (q.kind IN (1,2) AND e.kind='unclassified_reference'))
        UNION ALL
        SELECT e.target_rfc,q.slot,e.kind=ANY(q.kinds) AS matched
        FROM relation_input q
        JOIN rfc.rfc_relations e ON e.generation_id=$1 AND e.source_rfc=q.target
        WHERE q.incoming AND e.target_rfc>$2::integer
          AND (e.kind=ANY(q.kinds) OR (q.kind IN (1,2) AND e.kind='unclassified_reference'))
    ),
    relation_hits AS (
        SELECT rfc_id,array_agg(DISTINCT slot) FILTER (WHERE matched) AS positive,
            array_agg(DISTINCT slot) FILTER (WHERE NOT matched) AS unclassified
        FROM edges GROUP BY rfc_id
    ),
    document_coverage AS (
        SELECT rfc_id,array_agg(family) AS complete
        FROM rfc.relation_coverage
        WHERE generation_id=$1 AND scope='document' AND state='complete' AND rfc_id>$2::integer
        GROUP BY rfc_id
    ),
    inverse_coverage AS (
        SELECT ARRAY(SELECT q.slot
            FROM relation_input q JOIN rfc.relation_coverage c
            ON c.generation_id=$1 AND c.scope='document' AND c.rfc_id=q.target
                AND c.family=q.family AND c.state='complete'
            WHERE q.incoming) AS complete
    )";

const OBSOLESCENCE: &str = "
    obsoleted AS (
        SELECT DISTINCT target_rfc AS rfc_id FROM rfc.rfc_relations
        WHERE generation_id=$1 AND kind='obsoletes' AND target_rfc>$2::integer
    ),
    catalog_coverage AS (
        SELECT EXISTS (SELECT FROM rfc.relation_coverage
            WHERE generation_id=$1 AND scope='catalog' AND rfc_id IS NULL
                AND family='obsoletes' AND state='complete') AS complete
    )";

const CANDIDATE_COVERAGE: &str = "
    FROM f),
    selected AS MATERIALIZED (
        SELECT rfc_id, CASE WHEN $4::boolean THEN predicates END AS predicates
        FROM evaluated WHERE matches IS TRUE
        ORDER BY rfc_id LIMIT ($3::bigint+1)
    )";

// Subtracting one puts the match itself in the interval ending at its RFC ID.
const BOUNDARIES: &str = ",
    candidates AS (
        SELECT *, row_number() OVER (ORDER BY rfc_id)-1 AS bucket FROM selected
    ),
    boundaries AS (SELECT ARRAY(SELECT rfc_id::bigint FROM selected ORDER BY rfc_id) AS ids)";

const EVALUATED_COVERAGE: &str = ",
    coverage AS (
        SELECT width_bucket(rfc_id::bigint-1, ids) AS bucket,
            count(*) AS evaluated, count(*) FILTER (WHERE matches IS NULL) AS unknown,
            count(*) FILTER (WHERE unknown_text) AS unknown_text,
            count(*) FILTER (WHERE unknown_relationships) AS unknown_relationships
        FROM evaluated CROSS JOIN boundaries GROUP BY 1
    )";

const TITLE_COVERAGE: &str = ",
    coverage AS (
        SELECT width_bucket(m.rfc_id::bigint-1, ids) AS bucket, count(*) AS evaluated,
            0::bigint AS unknown, 0::bigint AS unknown_text, 0::bigint AS unknown_relationships
        FROM rfc.generation_documents m CROSS JOIN boundaries
        WHERE m.generation_id=$1 AND m.rfc_id>$2::integer
          AND m.rfc_id<=coalesce(ids[($3::bigint+1)::integer]::integer,2147483647)
        GROUP BY 1
    )";

const RETURN_INTERVALS: &str = "
    SELECT k.rfc_id::integer,k.predicates,c.evaluated,c.unknown,c.unknown_text,c.unknown_relationships
    FROM coverage c LEFT JOIN candidates k USING (bucket)
    ORDER BY c.bucket";

impl Plan {
    pub(crate) fn compile_native(&self) -> Evaluation {
        let mut sql = String::from(MEMBERS);
        let mut lexemes = Vec::new();
        let mut lexeme_slots = Vec::new();
        let mut lexeme_counts = Vec::new();
        let mut relation_slots = Vec::new();
        let mut relation_targets = Vec::new();
        let mut relation_flags = Vec::new();
        let mut title_exact = false;
        let mut title_terms = false;
        let mut obsolescence = false;
        for (index, leaf) in self.leaves.iter().enumerate() {
            let slot = (index + 1) as i32;
            match &leaf.predicate {
                Predicate::Text(Field::Fulltext, Match::Token, _) => {
                    for term in &leaf.terms {
                        lexemes.push(term.clone());
                        lexeme_slots.push(slot);
                        lexeme_counts.push(leaf.terms.len() as i32);
                    }
                }
                Predicate::Text(Field::Title, Match::Exact, _) => title_exact = true,
                Predicate::Text(Field::Title, Match::Token, _) => title_terms = true,
                Predicate::Relation(family, direction, target) => {
                    relation_slots.push(slot);
                    relation_targets.push(i32::try_from(target.get()).unwrap());
                    let kind = match family {
                        Family::References => 0,
                        Family::Normative => 1,
                        Family::Informative => 2,
                        Family::Updates => 3,
                        Family::Obsoletes => 4,
                    };
                    relation_flags.push(kind + 5 * i32::from(*direction == Direction::Incoming));
                }
                Predicate::Obsoleted(_) => obsolescence = true,
                Predicate::Text(Field::Fulltext, Match::Exact, _) => unreachable!(),
            }
        }
        let fulltext = !lexemes.is_empty();
        let relationships = !relation_slots.is_empty();
        if !fulltext && !relationships && !obsolescence {
            return self.compile_titles(title_terms);
        }

        if title_exact {
            sql.push_str(",m.title");
        }
        if title_terms {
            sql.push(',');
            sql.push_str(TITLE_TERMS);
            sql.push_str(" AS title_terms");
        }
        if fulltext {
            sql.push_str(TEXT_STATUS);
        }
        sql.push_str(" FROM rfc.generation_documents m");
        if fulltext {
            sql.push_str(RIGHTS);
        }
        sql.push_str(" WHERE m.generation_id=$1 AND m.rfc_id>$2::integer)");

        let mut params = Vec::new();
        if fulltext {
            sql.push_str(
                ",query_lexemes AS (
                SELECT * FROM ROWS FROM (pg_catalog.unnest($5::text[]),
                    pg_catalog.unnest($6::integer[]),pg_catalog.unnest($7::integer[]))
                q(lexeme,slot,required)),",
            );
            sql.push_str(POSTINGS);
            params.extend([
                Parameter::Terms(lexemes),
                Parameter::Numbers(lexeme_slots),
                Parameter::Numbers(lexeme_counts),
            ]);
        }
        if relationships {
            let next = params.len() + 5;
            sql.push_str(",relation_input AS (");
            sql.push_str(RELATION_INPUT);
            write!(
                sql,
                " FROM ROWS FROM (pg_catalog.unnest(${next}::integer[]),
                   pg_catalog.unnest(${}::integer[]),pg_catalog.unnest(${}::integer[]))
                   q(slot,target,flags) CROSS JOIN LATERAL (SELECT flags % 5 AS kind) k",
                next + 1,
                next + 2
            )
            .unwrap();
            sql.push_str(RELATIONS);
            params.extend([
                Parameter::Numbers(relation_slots),
                Parameter::Numbers(relation_targets),
                Parameter::Numbers(relation_flags),
            ]);
        }
        if obsolescence {
            sql.push(',');
            sql.push_str(OBSOLESCENCE);
        }

        let mut text_unknown = Vec::new();
        let mut relationship_unknown = Vec::new();
        sql.push_str(",f AS (SELECT m.rfc_id,ARRAY[");
        for (index, leaf) in self.leaves.iter().enumerate() {
            let slot = index + 1;
            if index > 0 {
                sql.push(',');
            }
            let unknown = format!("f.predicates[{slot}] IS NULL");
            match &leaf.predicate {
                Predicate::Text(field, mode, value) => {
                    text_unknown.push(unknown);
                    match (field, mode) {
                        (Field::Title, _) => {
                            write_title(&mut sql, &mut params, *mode, value, &leaf.terms, "m")
                        }
                        (Field::Fulltext, Match::Token) => {
                            write!(sql, "CASE WHEN m.text_available THEN CASE
                                WHEN {slot}=ANY(t.slots) THEN true WHEN m.text_complete THEN false END END").unwrap();
                        }
                        (Field::Fulltext, Match::Exact) => unreachable!(),
                    }
                }
                Predicate::Relation(family, direction, _) => {
                    relationship_unknown.push(unknown);
                    let complete = match direction {
                        Direction::Incoming => format!("{slot}=ANY(ic.complete)"),
                        Direction::Outgoing => {
                            let family = match family {
                                Family::References => "all_references",
                                Family::Normative | Family::Informative => "classified_references",
                                Family::Updates => "updates",
                                Family::Obsoletes => "obsoletes",
                            };
                            format!("'{family}'=ANY(dc.complete)")
                        }
                    };
                    write!(sql, "CASE WHEN {slot}=ANY(r.positive) THEN true
                        WHEN {complete} AND NOT coalesce({slot}=ANY(r.unclassified),false) THEN false END").unwrap();
                }
                Predicate::Obsoleted(value) => {
                    relationship_unknown.push(unknown);
                    if !value {
                        sql.push_str("NOT ");
                    }
                    sql.push_str("(CASE WHEN o.rfc_id IS NOT NULL THEN true WHEN cc.complete THEN false END)");
                }
            }
        }
        sql.push_str("] AS predicates FROM members m");
        if fulltext {
            sql.push_str(" LEFT JOIN text_hits t USING (document_version)");
        }
        if relationships {
            sql.push_str(
                " LEFT JOIN relation_hits r USING (rfc_id)
                LEFT JOIN document_coverage dc USING (rfc_id) CROSS JOIN inverse_coverage ic",
            );
        }
        if obsolescence {
            sql.push_str(" LEFT JOIN obsoleted o USING (rfc_id) CROSS JOIN catalog_coverage cc");
        }
        write!(
            sql,
            "),evaluated AS MATERIALIZED (SELECT f.*,{} AS matches,
            ({}) AS unknown_text,({}) AS unknown_relationships",
            self.expression,
            unknown_sql(&text_unknown),
            unknown_sql(&relationship_unknown)
        )
        .unwrap();
        sql.push_str(CANDIDATE_COVERAGE);
        sql.push_str(BOUNDARIES);
        sql.push_str(EVALUATED_COVERAGE);
        sql.push_str(RETURN_INTERVALS);
        Evaluation { sql, params }
    }

    fn compile_titles(&self, tokenize: bool) -> Evaluation {
        // Titles are non-null catalog metadata. Every leaf has a definite
        // truth value, so coverage needs only range counts after lookahead.
        let mut sql = String::from(
            "WITH selected AS MATERIALIZED (
            SELECT m.rfc_id,CASE WHEN $4::boolean THEN f.predicates END AS predicates
            FROM rfc.generation_documents m",
        );
        if tokenize {
            write!(
                sql,
                " CROSS JOIN LATERAL (SELECT {TITLE_TERMS} AS title_terms OFFSET 0) t"
            )
            .unwrap();
        }
        sql.push_str(" CROSS JOIN LATERAL (SELECT ARRAY[");
        let mut params = Vec::new();
        for (index, leaf) in self.leaves.iter().enumerate() {
            if index > 0 {
                sql.push(',');
            }
            let Predicate::Text(Field::Title, mode, value) = &leaf.predicate else {
                unreachable!();
            };
            write_title(&mut sql, &mut params, *mode, value, &leaf.terms, "t");
        }
        write!(
            sql,
            "] AS predicates OFFSET 0) f
            WHERE m.generation_id=$1 AND m.rfc_id>$2::integer AND {}
            ORDER BY m.rfc_id LIMIT ($3::bigint+1))",
            self.expression
        )
        .unwrap();
        sql.push_str(BOUNDARIES);
        sql.push_str(TITLE_COVERAGE);
        sql.push_str(RETURN_INTERVALS);
        Evaluation { sql, params }
    }
}

fn write_title(
    sql: &mut String,
    params: &mut Vec<Parameter>,
    mode: Match,
    value: &str,
    terms: &[String],
    terms_source: &str,
) {
    let parameter = params.len() + 5;
    match mode {
        Match::Exact => {
            params.push(Parameter::Literal(value.into()));
            write!(
                sql,
                "strpos(m.title COLLATE pg_catalog.\"C\",${parameter}::text)>0"
            )
            .unwrap();
        }
        Match::Token => {
            params.push(Parameter::Terms(terms.into()));
            write!(
                sql,
                "{terms_source}.title_terms @> (${parameter}::text[] COLLATE pg_catalog.\"C\")"
            )
            .unwrap();
        }
    }
}
fn unknown_sql(leaves: &[String]) -> String {
    if leaves.is_empty() {
        "false".into()
    } else {
        leaves.join(" OR ")
    }
}
