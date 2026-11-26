use std::ops::Range;

use rfc_core::{BlobDigest, ByteRange, GenerationId, RfcNumber};
use rfc_storage::{
    policy::Policy,
    postgres::{GenerationReadGuard, GenerationSelection},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::Instant;
use tokio_postgres::Row;

use crate::{
    Search, SearchError,
    cursor::{CursorKey, MAX_AGE_SECONDS, validate_context},
    engine::unix_time,
    snippets::{self, MAX_SOURCE_BYTES, Source, SourceVerification},
};

const DEFAULT_LIMIT: i32 = 20;
const MAX_LIMIT: i32 = 100;
const MAX_PAGE_BYTES: usize = 64 * 1024;
const MAX_SECTION_ID: usize = 256;
const MAX_HEADING: usize = 4096;
const DOCUMENT: &str = "SELECT m.rfc_id::integer,m.title,m.stream,d.txt_digest::bytea,
    d.byte_length::bigint,d.encoding,d.notice_policy,d.notice_state::text,
    o.source_url,d.identity::bytea,coalesce(d.section_state::text,'unavailable'),
    coalesce(p.method, o.verification_method) AS verification_method, p.algorithm AS verification_algorithm, d.policy_receipt::bytea AS policy_identity,
    receipt.receipt::text AS policy_evidence,
    (SELECT cap.ceiling FROM rfc.policy_restrictions cap WHERE cap.rfc_id = m.rfc_id
      ORDER BY cap.epoch DESC LIMIT 1) AS policy_ceiling
    FROM rfc.generation_documents m JOIN rfc.document_versions d ON d.identity=m.document_version
    LEFT JOIN rfc.policy_receipts receipt ON receipt.identity = d.policy_receipt
    JOIN rfc.source_observations o ON o.identity=m.txt_observation
    LEFT JOIN rfc.generation_source_evidence p ON (p.generation_id,p.rfc_id) = (m.generation_id,m.rfc_id)
    WHERE m.generation_id=$1 AND m.rfc_id=$2::integer";

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Sort {
    #[default]
    IdAsc,
    IdDesc,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListRequest {
    limit: Option<i32>,
    cursor: Option<String>,
    #[serde(default)]
    sort: Sort,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableRequest {
    id: i32,
    max_depth: Option<i32>,
    limit: Option<i32>,
    cursor: Option<String>,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RawRange {
    start_byte: i64,
    end_byte: i64,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Format {
    Text,
    Base64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetRequest {
    id: i32,
    section_id: Option<String>,
    filter_section_text: Option<String>,
    range: Option<RawRange>,
    max_bytes: Option<usize>,
    cursor: Option<String>,
    format: Option<Format>,
}

#[derive(Serialize)]
enum Selector<'a> {
    Whole,
    Section(&'a str),
    Heading(&'a str),
    Range(RawRange),
}

impl GetRequest {
    fn selector(&self) -> Result<Selector<'_>, SearchError> {
        match (&self.section_id, &self.filter_section_text, self.range) {
            (None, None, None) => Ok(Selector::Whole),
            (Some(id), None, None)
                if (1..=MAX_SECTION_ID).contains(&id.len()) && !id.contains('\0') =>
            {
                Ok(Selector::Section(id))
            }
            (None, Some(heading), None)
                if (1..=MAX_HEADING).contains(&heading.len()) && !heading.contains('\0') =>
            {
                Ok(Selector::Heading(heading))
            }
            (None, None, Some(range)) => Ok(Selector::Range(range)),
            _ => Err(SearchError::InvalidArguments),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageCursor {
    v: u8,
    #[serde(rename = "t")]
    tool: String,
    #[serde(rename = "a")]
    args: String,
    #[serde(rename = "g")]
    generation: i64,
    #[serde(rename = "s")]
    sequence: i64,
    #[serde(default, rename = "e")]
    rights_epoch: i64,
    expiry: u64,
    #[serde(rename = "p")]
    after: i64,
    #[serde(rename = "d", skip_serializing_if = "Option::is_none")]
    document: Option<DocumentCursor>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DocumentCursor {
    id: i32,
    #[serde(rename = "v")]
    version: String,
    start: usize,
    end: usize,
    max: usize,
    format: Format,
}

struct PageContext<'a> {
    key: &'a CursorKey,
    cursor: Option<PageCursor>,
    now: u64,
    tool: &'static str,
}

impl<'a> PageContext<'a> {
    fn new(
        key: &'a CursorKey,
        tool: &'static str,
        token: Option<&str>,
    ) -> Result<Self, SearchError> {
        let now = unix_time()?;
        let cursor = token
            .map(|value| key.open::<PageCursor>(value))
            .transpose()?;
        if let Some(cursor) = &cursor {
            if cursor.v != 1 || cursor.tool != tool || cursor.after < 0 {
                return Err(SearchError::CursorInvalid);
            }
            validate_context(cursor.generation, cursor.sequence, cursor.expiry, now)?;
        }
        Ok(Self {
            key,
            cursor,
            now,
            tool,
        })
    }

    fn check_args(&self, fingerprint: &str) -> Result<(), SearchError> {
        if self
            .cursor
            .as_ref()
            .is_some_and(|value| value.args != fingerprint)
        {
            return Err(SearchError::CursorInvalid);
        }
        Ok(())
    }

    async fn read<T>(
        &self,
        search: &Search,
        deadline: Instant,
        call: impl for<'b, 'c> AsyncFnOnce(
            &'b GenerationReadGuard<'c>,
        ) -> rfc_storage::postgres::Result<T>,
    ) -> Result<T, SearchError> {
        let selection = if let Some(cursor) = &self.cursor {
            GenerationSelection::Pinned(
                GenerationId::new(cursor.generation).ok_or(SearchError::CursorInvalid)?,
            )
        } else {
            GenerationSelection::Active
        };
        search
            .read_generation(selection, deadline, async |guard| {
                if self
                    .cursor
                    .as_ref()
                    .is_some_and(|cursor| cursor.rights_epoch != guard.rights_epoch())
                {
                    return Err(SearchError::CursorExpired.into());
                }
                call(guard).await
            })
            .await
    }

    fn sequence(&self, guard: &GenerationReadGuard<'_>) -> i64 {
        self.cursor
            .as_ref()
            .map_or(guard.publication_sequence(), |cursor| cursor.sequence)
    }

    fn next(
        &self,
        guard: &GenerationReadGuard<'_>,
        args: &str,
        after: i64,
        document: Option<DocumentCursor>,
    ) -> Result<String, SearchError> {
        let expiry = self
            .cursor
            .as_ref()
            .map_or(self.now + MAX_AGE_SECONDS, |cursor| cursor.expiry);
        if expiry <= unix_time()? {
            return Err(SearchError::CursorExpired);
        }
        self.key.seal(&PageCursor {
            v: 1,
            tool: self.tool.into(),
            args: args.into(),
            generation: generation(guard)?,
            sequence: self.sequence(guard),
            rights_epoch: guard.rights_epoch(),
            expiry,
            after,
            document,
        })
    }

    fn result(
        &self,
        guard: &GenerationReadGuard<'_>,
        coverage: Value,
        next: Option<String>,
    ) -> Value {
        json!({
            "schema_version": 1,
            "generation_id": guard.generation().get().to_string(),
            "publication_sequence": self.sequence(guard).to_string(),
            "coverage": coverage,
            "truncated": next.is_some(),
            "next_cursor": next,
        })
    }
}

fn fingerprint(fields: &impl Serialize) -> Result<String, SearchError> {
    let bytes = serde_json::to_vec(fields).map_err(|_| SearchError::Unavailable)?;
    Ok(BlobDigest::hash(&bytes).to_string())
}

fn generation(guard: &GenerationReadGuard<'_>) -> Result<i64, SearchError> {
    i64::try_from(guard.generation().get()).map_err(|_| SearchError::Unavailable)
}

fn limit(value: Option<i32>) -> Result<i64, SearchError> {
    let value = value.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&value) {
        return Err(SearchError::InvalidLimit);
    }
    Ok(i64::from(value))
}

fn rfc(id: i32) -> Result<RfcNumber, SearchError> {
    RfcNumber::new(id).ok_or(SearchError::InvalidArguments)
}

async fn lookup(guard: &GenerationReadGuard<'_>, id: i32) -> Result<(Source, String), SearchError> {
    let row = guard
        .transaction()
        .query_opt(DOCUMENT, &[&generation(guard)?, &id])
        .await?;
    let row = row.ok_or(SearchError::RfcNotFound)?;
    Ok((Source::from_row(&row)?, row.get(10)))
}

impl Search {
    pub async fn list(
        &self,
        request: ListRequest,
        deadline: Instant,
        fits: impl Fn(&Value) -> bool + Send + Sync,
    ) -> Result<Value, SearchError> {
        let limit = limit(request.limit)?;
        let args = fingerprint(&(limit, request.sort))?;
        let context = PageContext::new(&self.key, "list", request.cursor.as_deref())?;
        context.check_args(&args)?;
        let (operator, ordering, initial) = match request.sort {
            Sort::IdAsc => (">", "ASC", 0_i64),
            Sort::IdDesc => ("<", "DESC", i64::from(i32::MAX) + 1),
        };
        let after = context.cursor.as_ref().map_or(initial, |value| value.after);
        context.read(self, deadline, async |guard| {
            let generation = generation(guard)?;
            let tx = guard.transaction();
            let count: Option<i32> = tx.query_opt(
                "SELECT jsonb_array_length(catalog_only) FROM rfc.generation_ingestion WHERE generation_id=$1",
                &[&generation],
            ).await?.map(|row| row.get(0));
            let sql = format!("SELECT m.rfc_id::integer,m.title,m.stream,m.current_status,o.source_url,d.txt_digest::bytea,
                coalesce(p.method, o.verification_method) AS verification_method, p.algorithm AS verification_algorithm
                FROM rfc.generation_documents m JOIN rfc.document_versions d ON d.identity=m.document_version
                JOIN rfc.source_observations o ON o.identity=m.txt_observation
                LEFT JOIN LATERAL (
                    SELECT method, algorithm FROM rfc.generation_source_evidence p
                    WHERE (p.generation_id,p.rfc_id) = (m.generation_id,m.rfc_id) LIMIT 1
                ) p ON true
                WHERE m.generation_id=$1 AND m.rfc_id {operator} $2::bigint
                ORDER BY m.rfc_id {ordering} LIMIT $3");
            let rows = tx.query(&sql, &[&generation, &after, &(limit + 1)]).await?;
            let entries = rows.iter().map(|row| {
                let id: i32 = row.get(0);
                let digest = BlobDigest::from_bytes(row.get::<_, &[u8]>(5).try_into().map_err(|_| SearchError::Unavailable)?);
                let mut item = json!({
                    "id": id, "title": row.get::<_, &str>(1), "stream": row.get::<_, Option<&str>>(2),
                    "status": row.get::<_, Option<&str>>(3), "source_url": row.get::<_, &str>(4),
                    "digest": digest.to_string(),
                });
                let verification = SourceVerification::read(row, "verification_method", "verification_algorithm")?;
                SourceVerification::add(&mut item, verification.as_ref());
                Ok((i64::from(id), item))
            }).collect::<Result<Vec<_>, SearchError>>()?;
            Ok(build_items(guard, &context, ItemPage {
                args: &args, limit, entries, field: "items", coverage: json!({"catalog_only":count}),
                extra: json!({}),
            }, &fits)?)
        }).await
    }

    pub async fn table(
        &self,
        request: TableRequest,
        deadline: Instant,
        fits: impl Fn(&Value) -> bool + Send + Sync,
    ) -> Result<Value, SearchError> {
        rfc(request.id)?;
        if request.max_depth.is_some_and(|depth| depth < 0) {
            return Err(SearchError::InvalidArguments);
        }
        let limit = limit(request.limit)?;
        let args = fingerprint(&(request.id, limit, request.max_depth))?;
        let context = PageContext::new(&self.key, "table", request.cursor.as_deref())?;
        context.check_args(&args)?;
        context.read(self, deadline, async |guard| {
            let (source, state) = lookup(guard, request.id).await?;
            let after = context.cursor.as_ref().map_or(-1_i64, |value| value.after);
            let rows = guard.transaction().query(
                "SELECT ordinal,section_id,parent_section_id,depth,display_number,heading_text,
                    start_byte::bigint,end_byte::bigint,heading_start_byte::bigint,heading_end_byte::bigint,parse_state::text
                 FROM rfc.sections WHERE document_version=$1::bytea AND ordinal>$2::bigint
                    AND ($3::integer IS NULL OR depth<=$3) ORDER BY ordinal LIMIT $4",
                &[&source.version, &after, &request.max_depth, &(limit + 1)],
            ).await?;
            let mut group = None;
            let mut original = None;
            if !rows.is_empty() && source.permits_excerpt() && source.length <= MAX_SOURCE_BYTES
                && let Some(bytes) = self.read_original(guard, source.rfc, source.length, |bytes| bytes).await?
                && let Some(notices) = snippets::notices(guard.transaction(), &source, &bytes, false).await?
            {
                group = Some(source.notice_group(notices, false));
                original = Some(bytes);
            }
            let entries = rows.iter().map(|row| table_entry(row, original.as_deref(), group.as_ref().map(|g| g.id.as_str()))).collect();
            let headings_included = group.is_some();
            let mut extra = json!({
                "id":request.id, "source_url":source.url, "digest":source.digest.to_string(),
                "notice_groups":group.into_iter().collect::<Vec<_>>(),
            });
            SourceVerification::add(&mut extra, source.verification.as_ref());
            Ok(build_items(guard, &context, ItemPage {
                args: &args, limit, entries, field: "sections",
                coverage: json!({"section_state":state, "headings_included":headings_included}),
                extra,
            }, &fits)?)
        }).await
    }

    pub async fn get(
        &self,
        request: GetRequest,
        deadline: Instant,
        fits: impl Fn(&Value) -> bool + Send + Sync,
    ) -> Result<Value, SearchError> {
        rfc(request.id)?;
        let selector = request.selector()?;
        let whole = matches!(selector, Selector::Whole);
        if request
            .max_bytes
            .is_some_and(|limit| !(1..=MAX_PAGE_BYTES).contains(&limit))
        {
            return Err(SearchError::ResourceLimit);
        }
        let context = PageContext::new(&self.key, "get", request.cursor.as_deref())?;
        let args = fingerprint(&(request.id, &selector))?;
        if context.cursor.as_ref().is_some_and(|cursor| {
            cursor
                .document
                .as_ref()
                .is_none_or(|document| document.id != request.id)
        }) {
            return Err(SearchError::CursorInvalid);
        }
        if !whole {
            context.check_args(&args)?;
        }
        context
            .read(self, deadline, async |guard| {
                let (source, section_state) = lookup(guard, request.id).await?;
                let version =
                    BlobDigest::from_bytes(source.version.as_slice().try_into()?).to_string();
                let format = request.format.unwrap_or(if source.encoding == "utf8" {
                    Format::Text
                } else {
                    Format::Base64
                });
                let (scope, position, args, selection_is_whole) =
                    if let Some(cursor) = &context.cursor {
                        let saved = cursor.document.as_ref().ok_or(SearchError::CursorInvalid)?;
                        let position = usize::try_from(cursor.after)?;
                        if saved.version != version
                            || saved.end > source.length
                            || position <= saved.start
                            || position >= saved.end
                            || !(1..=MAX_PAGE_BYTES).contains(&saved.max)
                            || request.format.is_some_and(|format| format != saved.format)
                            || request.max_bytes.is_some_and(|max| max != saved.max)
                        {
                            return Err(SearchError::CursorInvalid.into());
                        }
                        let whole_args = fingerprint(&(request.id, Selector::Whole))?;
                        (
                            saved.clone(),
                            position,
                            cursor.args.as_str(),
                            cursor.args == whole_args,
                        )
                    } else {
                        if !source.permits_excerpt() && !whole {
                            return Err(SearchError::SourcePolicyRestricted.into());
                        }
                        let range = select(guard, &source, selector).await?;
                        (
                            DocumentCursor {
                                id: request.id,
                                version,
                                start: range.start,
                                end: range.end,
                                max: request.max_bytes.unwrap_or(MAX_PAGE_BYTES),
                                format,
                            },
                            range.start,
                            args.as_str(),
                            whole,
                        )
                    };

                let whole_only = selection_is_whole && source.policy == Policy::WholeOnly;
                let reviewed_pages = selection_is_whole
                    && source.policy == Policy::WholePaginated
                    && source.notice_state == "complete";
                if (whole_only && position != 0)
                    || (!whole_only && !reviewed_pages && !source.permits_excerpt())
                {
                    return Err(SearchError::SourcePolicyRestricted.into());
                }
                if scope.format == Format::Text && source.encoding != "utf8" {
                    return Err(SearchError::UnsupportedSourceEncoding.into());
                }
                if source.length > MAX_SOURCE_BYTES || (whole_only && source.length > scope.max) {
                    return Err(SearchError::ResourceLimit.into());
                }
                let bytes = self
                    .read_original(guard, source.rfc, source.length, |bytes| bytes)
                    .await?
                    .ok_or(SearchError::SourceUnavailable)?;
                let notices =
                    match snippets::notices(guard.transaction(), &source, &bytes, reviewed_pages)
                        .await?
                    {
                        Some(notices) if source.notice_state == "complete" => notices,
                        _ if whole_only => Vec::new(),
                        _ => return Err(SearchError::SourcePolicyRestricted.into()),
                    };
                let group = source.notice_group(notices, whole_only);
                let text = if scope.format == Format::Text {
                    let text =
                        std::str::from_utf8(&bytes).map_err(|_| SearchError::SourceUnavailable)?;
                    if !text.is_char_boundary(position) || !text.is_char_boundary(scope.end) {
                        return Err(SearchError::InvalidUtf8Boundary.into());
                    }
                    Some(text)
                } else {
                    None
                };
                let minimum_end = if position == scope.end {
                    position
                } else if let Some(text) = text {
                    position
                        + text[position..scope.end]
                            .chars()
                            .next()
                            .expect("nonempty validated text")
                            .len_utf8()
                } else {
                    position + 1
                };
                let mut end = scope.end.min(position + scope.max);
                if end < minimum_end {
                    return Err(SearchError::ResourceLimit.into());
                }
                loop {
                    while text.is_some_and(|text| !text.is_char_boundary(end)) {
                        end -= 1;
                    }
                    if end == position && position != scope.end {
                        return Err(SearchError::ResourceLimit.into());
                    }
                    let next = (end < scope.end)
                        .then(|| context.next(guard, args, end as i64, Some(scope.clone())))
                        .transpose()?;
                    let mut result =
                        context.result(guard, json!({"section_state":section_state}), next);
                    let mut original = serde_json::to_value(snippets::original(
                        position..end,
                        &bytes[position..end],
                        scope.format == Format::Base64,
                    ))?;
                    original["rfc_id"] = json!(request.id);
                    original["source_url"] = json!(source.url);
                    SourceVerification::add(&mut original, source.verification.as_ref());
                    original["digest"] = json!(source.digest.to_string());
                    original["encoding"] = json!(if scope.format == Format::Base64 {
                        "base64"
                    } else {
                        "utf8"
                    });
                    original["notice_group_id"] = json!(group.id);
                    result["original"] = original;
                    result["selection"] = json!({"start_byte":scope.start,"end_byte":scope.end});
                    result["notice_groups"] = json!([group]);
                    if fits(&result) {
                        return Ok(result);
                    }
                    if whole_only || end == minimum_end {
                        return Err(SearchError::ResourceLimit.into());
                    }
                    end = (position + (end - position) / 2).max(minimum_end);
                }
            })
            .await
    }
}

async fn select(
    guard: &GenerationReadGuard<'_>,
    source: &Source,
    selector: Selector<'_>,
) -> Result<Range<usize>, SearchError> {
    let tx = guard.transaction();
    let bounds = match selector {
        Selector::Whole => return Ok(0..source.length),
        Selector::Range(range) => (range.start_byte, range.end_byte),
        Selector::Section(id) => {
            let row = tx
                .query_opt(
                    "SELECT start_byte::bigint,end_byte::bigint FROM rfc.sections
                WHERE document_version=$1::bytea AND section_id=$2",
                    &[&source.version, &id],
                )
                .await?
                .ok_or(SearchError::SectionNotFound)?;
            (row.get(0), row.get(1))
        }
        Selector::Heading(heading) => {
            let candidates = tx
                .query(
                    "SELECT start_byte::bigint,end_byte::bigint,section_id FROM rfc.sections
                WHERE document_version=$1::bytea AND heading_text COLLATE pg_catalog.\"C\"=$2
                ORDER BY ordinal LIMIT 10",
                    &[&source.version, &heading],
                )
                .await?;
            match candidates.as_slice() {
                [] => return Err(SearchError::SectionNotFound),
                [row] => (row.get(0), row.get(1)),
                rows => {
                    return Err(SearchError::AmbiguousSection(
                        rows.iter().map(|row| row.get(2)).collect(),
                    ));
                }
            }
        }
    };
    let range = ByteRange::new(bounds.0, bounds.1)
        .filter(|range| range.fits(source.length as u64))
        .ok_or(SearchError::InvalidByteRange)?;
    Ok(range.start() as usize..range.end() as usize)
}

fn table_entry(row: &Row, original: Option<&[u8]>, group: Option<&str>) -> (i64, Value) {
    let start: i64 = row.get(8);
    let end: i64 = row.get(9);
    let mut entry = json!({
        "section_id":row.get::<_, &str>(1), "parent_section_id":row.get::<_, Option<&str>>(2),
        "depth":row.get::<_, i32>(3), "number":row.get::<_, Option<&str>>(4),
        "range":{"start_byte":row.get::<_, i64>(6), "end_byte":row.get::<_, i64>(7)},
        "heading_range":{"start_byte":start, "end_byte":end},
        "parse_state":row.get::<_, &str>(10),
    });
    if let (Some(bytes), Some(group), Ok(start), Ok(end)) = (
        original,
        group,
        usize::try_from(start),
        usize::try_from(end),
    ) && let Some(heading) = bytes.get(start..end)
    {
        entry["heading_text"] = json!(row.get::<_, &str>(5));
        entry["original_heading"] = json!(snippets::original(start..end, heading, false));
        entry["notice_group_id"] = json!(group);
    }
    (i64::from(row.get::<_, i32>(0)), entry)
}

struct ItemPage<'a> {
    args: &'a str,
    limit: i64,
    entries: Vec<(i64, Value)>,
    field: &'static str,
    coverage: Value,
    extra: Value,
}

fn build_items(
    guard: &GenerationReadGuard<'_>,
    context: &PageContext<'_>,
    page: ItemPage<'_>,
    fits: &impl Fn(&Value) -> bool,
) -> Result<Value, SearchError> {
    let mut count = page.entries.len().min(page.limit as usize);
    loop {
        let cursor = if count < page.entries.len() {
            let last = count.checked_sub(1).ok_or(SearchError::ResourceLimit)?;
            Some(context.next(guard, page.args, page.entries[last].0, None)?)
        } else {
            None
        };
        let mut result = context.result(guard, page.coverage.clone(), cursor);
        result.as_object_mut().expect("result is an object").extend(
            page.extra
                .as_object()
                .expect("page metadata is an object")
                .clone(),
        );
        result[page.field] = Value::Array(
            page.entries
                .iter()
                .take(count)
                .map(|(_, value)| value.clone())
                .collect(),
        );
        if fits(&result) {
            return Ok(result);
        }
        count = count.checked_sub(1).ok_or(SearchError::ResourceLimit)?;
    }
}
