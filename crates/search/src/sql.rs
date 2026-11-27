use std::{fmt::Write, ops::Range};

use rfc_core::BlobDigest;
use tokio_postgres::{Transaction, types::ToSql};

use crate::{
    SearchError,
    query::{Direction, Expr, Family, Field, Match, Predicate, QueryError},
};

const MAX_TERMS: usize = 64;
const TOKENIZE: &str = "SELECT DISTINCT term.lexeme COLLATE pg_catalog.\"C\" AS lexeme
    FROM pg_catalog.ts_debug('pg_catalog.simple'::regconfig, $1::text) AS token
    CROSS JOIN LATERAL pg_catalog.unnest(token.lexemes) AS term(lexeme)
    WHERE octet_length(token.token) < 2048 AND octet_length(term.lexeme) BETWEEN 1 AND 2047
    ORDER BY lexeme LIMIT 65";

pub(crate) const WINDOW: &str = "SELECT m.rfc_id::integer, d.byte_length::bigint,
    coalesce(sr.evidence->>'search'='true',false) AND (coalesce(cap.denied,0) & 8) = 0
    FROM rfc.generation_documents m JOIN rfc.document_versions d ON d.identity=m.document_version
    LEFT JOIN rfc.source_rights sr ON (sr.generation_id,sr.rfc_id) = (m.generation_id,m.rfc_id)
    LEFT JOIN rfc.source_restrictions cap ON cap.rfc_id=m.rfc_id
    WHERE m.generation_id=$1 AND m.rfc_id>$2::integer
    ORDER BY m.rfc_id LIMIT $3";

#[derive(Debug)]
pub(crate) enum Logic {
    Leaf(usize),
    Not(Box<Logic>),
    And(Vec<Logic>),
    Or(Vec<Logic>),
}

impl Logic {
    fn from_expr(expr: Expr, leaves: &mut Vec<Leaf>) -> Self {
        match expr {
            Expr::Predicate(predicate, span) => {
                let slot = leaves
                    .iter()
                    .position(|leaf| leaf.predicate == predicate)
                    .unwrap_or_else(|| {
                        leaves.push(Leaf {
                            predicate,
                            span,
                            terms: Vec::new(),
                        });
                        leaves.len() - 1
                    });
                Self::Leaf(slot)
            }
            Expr::Not(child) => Self::Not(Box::new(Self::from_expr(*child, leaves))),
            Expr::And(children) => Self::And(
                children
                    .into_iter()
                    .map(|child| Self::from_expr(child, leaves))
                    .collect(),
            ),
            Expr::Or(children) => Self::Or(
                children
                    .into_iter()
                    .map(|child| Self::from_expr(child, leaves))
                    .collect(),
            ),
        }
    }

    fn write_sql(&self, sql: &mut String) {
        match self {
            Self::Leaf(slot) => write!(sql, "f.predicates[{}]", slot + 1).unwrap(),
            Self::Not(child) => {
                sql.push_str("(NOT ");
                child.write_sql(sql);
                sql.push(')');
            }
            Self::And(children) | Self::Or(children) => {
                let op = if matches!(self, Self::And(_)) {
                    " AND "
                } else {
                    " OR "
                };
                sql.push('(');
                for (i, child) in children.iter().enumerate() {
                    if i != 0 {
                        sql.push_str(op);
                    }
                    child.write_sql(sql);
                }
                sql.push(')');
            }
        }
    }

    fn value(&self, facts: &[Option<bool>]) -> Option<bool> {
        match self {
            Self::Leaf(slot) => facts[*slot],
            Self::Not(child) => child.value(facts).map(|value| !value),
            Self::And(children) | Self::Or(children) => {
                let deciding = matches!(self, Self::Or(_));
                let mut unknown = false;
                for child in children {
                    match child.value(facts) {
                        Some(value) if value == deciding => return Some(deciding),
                        None => unknown = true,
                        _ => {}
                    }
                }
                (!unknown).then_some(!deciding)
            }
        }
    }

    pub fn witnesses(&self, facts: &[Option<bool>], needed: bool, output: &mut Vec<usize>) {
        if self.value(facts) != Some(needed) {
            return;
        }
        match self {
            Self::Leaf(slot) if needed => output.push(*slot),
            Self::Leaf(_) => {}
            Self::Not(child) => child.witnesses(facts, !needed, output),
            Self::And(children) | Self::Or(children) => {
                for child in children {
                    child.witnesses(facts, needed, output);
                }
            }
        }
    }
}

pub(crate) struct Leaf {
    pub predicate: Predicate,
    span: Range<usize>,
    terms: Vec<String>,
}

pub(crate) struct Plan {
    pub logic: Logic,
    pub leaves: Vec<Leaf>,
    pub exact: Vec<usize>,
    expression: String,
}

impl Plan {
    pub fn new(expr: Expr) -> Self {
        let mut leaves = Vec::new();
        let logic = Logic::from_expr(expr, &mut leaves);
        let exact = leaves
            .iter()
            .enumerate()
            .filter_map(|(slot, leaf)| {
                matches!(
                    leaf.predicate,
                    Predicate::Text(Field::Fulltext, Match::Exact, _)
                )
                .then_some(slot)
            })
            .collect();
        let mut expression = String::new();
        logic.write_sql(&mut expression);
        Self {
            logic,
            leaves,
            exact,
            expression,
        }
    }

    pub fn fingerprint(&self, limit: u16, include_snippets: bool) -> String {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"rfc-search-arguments-v1\0");
        bytes.extend_from_slice(&limit.to_be_bytes());
        bytes.push(u8::from(include_snippets));
        bytes.extend_from_slice(&(self.expression.len() as u32).to_be_bytes());
        bytes.extend_from_slice(self.expression.as_bytes());
        for leaf in &self.leaves {
            match &leaf.predicate {
                Predicate::Text(field, mode, literal) => {
                    bytes.extend_from_slice(&[0, *field as u8, *mode as u8]);
                    bytes.extend_from_slice(&(literal.len() as u32).to_be_bytes());
                    bytes.extend_from_slice(literal.as_bytes());
                }
                Predicate::Relation(family, direction, target) => {
                    bytes.extend_from_slice(&[1, *family as u8, *direction as u8]);
                    bytes.extend_from_slice(&target.get().to_be_bytes());
                }
                Predicate::Obsoleted(value) => bytes.extend_from_slice(&[2, (*value).into()]),
            }
        }
        BlobDigest::hash(&bytes).to_string()
    }

    pub async fn normalize(&mut self, tx: &Transaction<'_>) -> Result<(), SearchError> {
        let mut total = 0;
        let mut normalized = std::collections::BTreeMap::<String, Vec<String>>::new();
        for leaf in &mut self.leaves {
            if let Predicate::Text(_, Match::Token, literal) = &leaf.predicate {
                if let Some(terms) = normalized.get(literal) {
                    leaf.terms.clone_from(terms);
                } else {
                    leaf.terms = tx
                        .query(TOKENIZE, &[literal])
                        .await?
                        .iter()
                        .map(|row| row.get::<_, String>(0))
                        .collect();
                    normalized.insert(literal.clone(), leaf.terms.clone());
                }
                if leaf.terms.is_empty() {
                    return Err(QueryError::new(
                        "invalid_query_syntax",
                        "Token literal has no searchable terms",
                        leaf.span.clone(),
                    )
                    .into());
                }
                total += leaf.terms.len();
                if total > MAX_TERMS {
                    return Err(QueryError::new(
                        "query_too_complex",
                        "Query exceeds 64 normalized token terms",
                        leaf.span.clone(),
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    pub fn compile(&self) -> Evaluation {
        // These scalar lookups stay tied to each bounded candidate. A broad
        // hash or semi-join could scan historical observations or coverage rows.
        let mut sql = String::from("WITH f AS MATERIALIZED (SELECT m.rfc_id::integer,
            CASE WHEN octet_length(m.title)+coalesce(octet_length(m.stream),0)<=262144 THEN m.title END,
            CASE WHEN octet_length(m.stream)<=262144 THEN m.stream END,
            d.txt_digest::bytea,d.byte_length::bigint,d.encoding,d.notice_policy,d.notice_state::text,
            (SELECT o.source_url FROM rfc.source_observations o WHERE o.identity=m.txt_observation LIMIT 1),
            m.document_version::bytea,
            coalesce(p.method, (SELECT o.verification_method FROM rfc.source_observations o
                WHERE o.identity=m.txt_observation LIMIT 1)) AS verification_method,
            p.algorithm AS verification_algorithm, sr.identity::bytea AS policy_identity,
    sr.evidence::text AS policy_evidence, m.metadata_identity::bytea AS policy_metadata,
    coalesce(cap.denied, 0) AS policy_denied,ARRAY[");
        let mut params = Vec::new();
        for (slot, leaf) in self.leaves.iter().enumerate() {
            if slot != 0 {
                sql.push(',');
            }
            let parameter = params.len() + 6;
            match &leaf.predicate {
                Predicate::Text(Field::Title, Match::Exact, literal) => {
                    params.push(Parameter::Literal(literal.clone()));
                    write!(
                        sql,
                        "strpos(m.title COLLATE pg_catalog.\"C\",${parameter}::text)>0"
                    )
                    .unwrap();
                }
                Predicate::Text(Field::Fulltext, Match::Exact, _) => {
                    let exact = self.exact.iter().position(|&index| index == slot).unwrap();
                    write!(
                        sql,
                        "(CASE WHEN (coalesce(cap.denied,0) & 8) = 0 AND sr.evidence->>'search'='true' THEN CASE get_byte(x.facts,{exact}) WHEN 1 THEN true WHEN 0 THEN false END END)"
                    )
                    .unwrap();
                }
                Predicate::Text(Field::Title, Match::Token, _) => {
                    params.push(Parameter::Terms(leaf.terms.clone()));
                    write!(sql, "ARRAY(SELECT term.lexeme
                        FROM pg_catalog.ts_debug('pg_catalog.simple'::regconfig,m.title) token
                        CROSS JOIN LATERAL pg_catalog.unnest(token.lexemes) term(lexeme)
                        WHERE octet_length(token.token)<2048 AND octet_length(term.lexeme) BETWEEN 1 AND 2047)
                        COLLATE pg_catalog.\"C\" @> ${parameter}::text[]").unwrap();
                }
                Predicate::Text(Field::Fulltext, Match::Token, _) => {
                    params.push(Parameter::Terms(leaf.terms.clone()));
                    write!(sql, "(CASE WHEN (coalesce(cap.denied,0) & 8) = 0 AND sr.evidence->>'search'='true' AND d.index_version='rfc-original-bytes-pg19-1'
                        AND d.line_starts IS NOT NULL AND d.encoding='utf8' THEN
                        CASE WHEN NOT EXISTS (SELECT FROM pg_catalog.unnest(${parameter}::text[]) term(lexeme)
                            WHERE NOT EXISTS (SELECT FROM rfc.document_lexemes lexeme
                                WHERE lexeme.document_version=m.document_version
                                AND lexeme.lexeme=term.lexeme COLLATE pg_catalog.\"C\"))
                        THEN true WHEN d.indexing_state='complete' THEN false END END)").unwrap();
                }
                Predicate::Relation(family, direction, target) => {
                    params.push(Parameter::Rfc(i32::try_from(target.get()).unwrap()));
                    write_relation(&mut sql, *family, *direction, parameter);
                }
                Predicate::Obsoleted(value) => {
                    if !value {
                        sql.push_str("NOT ");
                    }
                    sql.push_str("(CASE WHEN (SELECT true FROM rfc.rfc_relations e
                        WHERE e.generation_id=m.generation_id AND e.target_rfc=m.rfc_id AND e.kind='obsoletes'
                        LIMIT 1) THEN true WHEN (SELECT c.state='complete' FROM rfc.relation_coverage c
                        WHERE c.generation_id=m.generation_id AND c.scope='catalog' AND c.rfc_id IS NULL
                        AND c.family='obsoletes' LIMIT 1) THEN false END)");
                }
            }
        }
        sql.push_str(
            "] AS predicates FROM rfc.generation_documents m
            JOIN rfc.document_versions d ON d.identity=m.document_version
            LEFT JOIN rfc.source_rights sr ON (sr.generation_id,sr.rfc_id) = (m.generation_id,m.rfc_id)
    LEFT JOIN rfc.source_restrictions cap ON cap.rfc_id=m.rfc_id
            LEFT JOIN LATERAL (
                SELECT method, algorithm FROM rfc.generation_source_evidence p
                WHERE (p.generation_id,p.rfc_id) = (m.generation_id,m.rfc_id) LIMIT 1
            ) p ON true
            LEFT JOIN ROWS FROM (pg_catalog.unnest($4::integer[]), pg_catalog.unnest($5::bytea[]))
                AS x(rfc_id,facts) ON x.rfc_id=m.rfc_id
            WHERE m.generation_id=$1 AND m.rfc_id>$2::integer AND m.rfc_id<=$3::integer)
            SELECT f.*,",
        );
        sql.push_str(&self.expression);
        sql.push_str(" AS matches FROM f ORDER BY f.rfc_id");
        Evaluation { sql, params }
    }
}

fn write_relation(sql: &mut String, family: Family, direction: Direction, parameter: usize) {
    let (kinds, coverage) = match family {
        Family::References => (
            "'normative_reference','informative_reference','unclassified_reference'",
            "all_references",
        ),
        Family::Normative => ("'normative_reference'", "classified_references"),
        Family::Informative => ("'informative_reference'", "classified_references"),
        Family::Updates => ("'updates'", "updates"),
        Family::Obsoletes => ("'obsoletes'", "obsoletes"),
    };
    let supplied = format!("${parameter}::integer");
    let (source, target) = match direction {
        Direction::Outgoing => ("m.rfc_id", supplied.as_str()),
        Direction::Incoming => (supplied.as_str(), "m.rfc_id"),
    };
    write!(
        sql,
        "(CASE WHEN (SELECT true FROM rfc.rfc_relations e
        WHERE e.generation_id=m.generation_id AND e.source_rfc={source} AND e.target_rfc={target}
        AND e.kind IN ({kinds}) LIMIT 1) THEN true
        WHEN (SELECT c.state='complete' FROM rfc.relation_coverage c
        WHERE c.generation_id=m.generation_id AND c.scope='document' AND c.rfc_id={source}
        AND c.family='{coverage}' LIMIT 1)"
    )
    .unwrap();
    if matches!(family, Family::Normative | Family::Informative) {
        write!(sql, " AND (SELECT true FROM rfc.rfc_relations e
            WHERE e.generation_id=m.generation_id AND e.source_rfc={source} AND e.target_rfc={target}
            AND e.kind='unclassified_reference' LIMIT 1) IS NULL").unwrap();
    }
    sql.push_str(" THEN false END)");
}

enum Parameter {
    Literal(String),
    Terms(Vec<String>),
    Rfc(i32),
}

pub(crate) struct Evaluation {
    pub sql: String,
    params: Vec<Parameter>,
}

impl Evaluation {
    pub fn bind<'a>(&'a self, scope: [&'a (dyn ToSql + Sync); 5]) -> Vec<&'a (dyn ToSql + Sync)> {
        scope
            .into_iter()
            .chain(self.params.iter().map(|parameter| match parameter {
                Parameter::Literal(value) => value as &(dyn ToSql + Sync),
                Parameter::Terms(value) => value,
                Parameter::Rfc(value) => value,
            }))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parse;

    #[test]
    fn compiler_never_interpolates_user_values() {
        let hostile = "in::title:\"%_'; -- (SELECT) É\" AND relation::references:12345";
        let plan = Plan::new(parse(hostile).unwrap());
        let evaluation = plan.compile();
        assert!(!evaluation.sql.contains("12345"));
        assert!(!evaluation.sql.contains("(SELECT)"));
        assert!(
            matches!(&evaluation.params[0], Parameter::Literal(value) if value == "%_'; -- (SELECT) É")
        );
        assert!(matches!(&evaluation.params[1], Parameter::Rfc(12345)));
    }

    #[test]
    fn seeded_queries_keep_values_in_bound_parameters() {
        let mut seed = 20260728_u32;
        for index in 0..512 {
            seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
            let id = (seed % 2_000_000_000 + 1) as i32;
            let literal = format!("fixture-{seed:x} É %_ '");
            let field = ["title", "fulltext"][index % 2];
            let operator = ["AND", "OR"][(index / 2) % 2];
            let negation = ["", "!"][(index / 4) % 2];
            let query = format!(
                "{negation}(in::{field}:\"{literal}\" {operator} relation::references:{id})"
            );
            let plan = Plan::new(parse(&query).unwrap());
            let evaluation = plan.compile();
            assert!(!evaluation.sql.contains(&literal));
            match evaluation.params.as_slice() {
                [Parameter::Literal(value), Parameter::Rfc(number)] if field == "title" => {
                    assert_eq!((value, *number), (&literal, id));
                }
                [Parameter::Rfc(number)] if field == "fulltext" => {
                    assert_eq!(*number, id);
                    assert_eq!(plan.exact, [0]);
                }
                _ => panic!("unexpected parameter layout for {query}"),
            }
        }
    }

    #[test]
    fn fingerprint_normalizes_syntax_but_binds_arguments() {
        let plan = |query| Plan::new(parse(query).unwrap());
        let first = plan("depend::id:01");
        let second = plan("(relation :: references : 1)");
        let hash = first.fingerprint(20, false);
        assert_eq!(hash, second.fingerprint(20, false));
        assert_ne!(hash, second.fingerprint(21, false));
        assert_ne!(hash, second.fingerprint(20, true));
    }

    #[test]
    fn witnesses_follow_true_branches_and_negation() {
        let plan = Plan::new(
            parse("in::fulltext:'a' AND !in::fulltext:'b' OR !!in::fulltext:'c'").unwrap(),
        );
        let mut output = Vec::new();
        plan.logic
            .witnesses(&[Some(true), None, Some(true)], true, &mut output);
        assert_eq!(output, [2]);
        output.clear();
        plan.logic
            .witnesses(&[Some(true), Some(false), Some(false)], true, &mut output);
        assert_eq!(output, [0]);
    }
}
