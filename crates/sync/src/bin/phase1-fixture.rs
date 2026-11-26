//! Offline fixture driver compiled only into the dedicated integration target.

use rfc_core::{
    BlobDigest, ByteRange, Coverage, GenerationId, Relation, RelationKind, RfcNumber, SectionId,
};
use rfc_mcp_docker_sync::{
    Chunk, Document, Evidence, Membership, Notice, Observation, Section, Writer,
};
use rfc_storage::blob::{BlobReader, BlobWriter, Install};
use rfc_storage::postgres::{Reader, Result, RuntimeRole, connect, mirror_root};
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

const TXT: &[(i32, &[u8])] = &[
    (2119, include_bytes!("../../../../fixtures/rfc/rfc2119.txt")),
    (8174, include_bytes!("../../../../fixtures/rfc/rfc8174.txt")),
];
const CATALOG: &[u8] = b"synthetic catalog: 8174 updates 2119\n";
const CAP: usize = 64 * 1024;
const TIME: i64 = 1_795_320_000_000_000;
const FAMILIES: [&str; 4] = [
    "all_references",
    "classified_references",
    "updates",
    "obsoletes",
];

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [command] if command == "populate" => populate().await?,
        [command, id] if command == "verify" => {
            verify(GenerationId::new(id.parse()?).ok_or("invalid ID")?).await?
        }
        [command, rel] if command == "install" => {
            let writer = BlobWriter::open(&mirror_root())?;
            let file = writer.open_candidate(Path::new("/fixture"), rel)?;
            let installed = writer.install(file, CAP as u64)?;
            let status = match installed {
                Install::New(_) => "new",
                Install::Reused(_) => "reused",
            };
            println!("{status} {}", installed.blob().digest());
        }
        _ => return Err("usage: populate | verify GENERATION | install FIXTURE".into()),
    }
    Ok(())
}

async fn populate() -> Result<()> {
    let store = BlobWriter::open(&mirror_root())?;
    let mut writer = Writer::connect().await?;
    let _coordinator = writer.fixture_coordinator().await?;
    let catalog = store.install(CATALOG, CAP as u64)?.blob();
    writer.register_blob(catalog).await?;
    let mut texts = Vec::new();
    for &(number, bytes) in TXT {
        let blob = store.install(bytes, CAP as u64)?.blob();
        writer.register_blob(blob).await?;
        texts.push((number, bytes, blob));
    }
    let mut previous = None;
    let started = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    for _ in 0..2 {
        let candidate = writer
            .begin("fixture-parser-v1", "fixture-index-v1")
            .await?;
        let observation = candidate
            .observe(&Observation {
                kind: "catalog",
                url: "https://www.rfc-editor.org/rfc-index.xml",
                observed_us: TIME,
                fetched_us: Some(TIME),
                etag: None,
                last_modified: None,
                raw_digest: catalog.digest(),
                https_verification: "verified",
            })
            .await?;
        candidate
            .fixture_catalog(observation, &[2119, 8174], started)
            .await?;
        for &(number, bytes, blob) in &texts {
            let rfc = RfcNumber::new(number).unwrap();
            let url = format!("https://www.rfc-editor.org/rfc/rfc{number}.txt");
            let txt = candidate
                .fixture_capture(&Observation {
                    kind: "txt",
                    url: &url,
                    observed_us: TIME,
                    fetched_us: Some(TIME),
                    etag: None,
                    last_modified: None,
                    raw_digest: blob.digest(),
                    https_verification: "verified",
                })
                .await?;
            let doc_range = ByteRange::new(0, blob.byte_length()).unwrap();
            let first_line = bytes
                .iter()
                .position(|byte| *byte == b'\n')
                .ok_or("fixture has no lines")?;
            let line_range = ByteRange::new(0, (first_line + 1) as i64).unwrap();
            let version = candidate
                .fixture_document(
                    Document {
                        rfc,
                        txt: blob,
                        parser_version: "fixture-parser-v1",
                        index_version: "fixture-index-v1",
                        encoding: "utf8",
                        index_state: Coverage::Partial,
                        notice_state: Coverage::Partial,
                        notice_policy: "metadata_only",
                        sections: vec![Section {
                            id: SectionId::new("fixture".into()).unwrap(),
                            parent: None,
                            ordinal: 0,
                            depth: 0,
                            number: None,
                            heading: "Fixture range",
                            body: doc_range,
                            raw_heading: line_range,
                            parse_state: Coverage::Partial,
                        }],
                        chunks: vec![Chunk {
                            range: doc_range,
                            text: std::str::from_utf8(bytes)?,
                        }],
                        notices: vec![Notice {
                            ordinal: 0,
                            category: "other",
                            range: line_range,
                            extraction: Coverage::Partial,
                            review: "unreviewed",
                        }],
                    },
                    std::str::from_utf8(bytes)?,
                    txt,
                    observation,
                )
                .await?;
            candidate
                .member(&Membership {
                    rfc,
                    version,
                    catalog: observation,
                    txt,
                    title: "Fixture",
                    stream: None,
                    publication_date: None,
                    status: None,
                })
                .await?;
            for family in FAMILIES {
                candidate
                    .coverage(Some(rfc), family, Coverage::Partial)
                    .await?;
            }
        }
        let evidence = candidate
            .evidence(&Evidence {
                observation: Some(observation),
                version: None,
                range: None,
                method: "catalog",
                xml_locator: Some("/fixture"),
            })
            .await?;
        let relation = Relation {
            source: RfcNumber::new(8174).unwrap(),
            kind: RelationKind::Updates,
            target: RfcNumber::new(2119).unwrap(),
        };
        candidate.relation(relation, &[evidence, evidence]).await?;
        candidate
            .relation(
                Relation {
                    target: RfcNumber::new(999_999).unwrap(),
                    ..relation
                },
                &[evidence],
            )
            .await?;
        for family in FAMILIES {
            candidate.coverage(None, family, Coverage::Partial).await?;
        }
        let (id, digest) = candidate.ready().await?;
        if let Some(previous) = previous {
            assert_eq!(
                previous, digest,
                "generation ID must not change the manifest"
            );
        }
        previous = Some(digest);
        println!("{} {digest}", id.get());
    }
    Ok(())
}

async fn verify(id: GenerationId) -> Result<()> {
    let mut reader = Reader::connect().await?;
    assert_eq!(reader.active().await?, (None, 0));
    let manifest = reader.verify_manifest(id).await?;
    let store = BlobReader::open(&mirror_root(), true)?;
    let db = connect(RuntimeRole::Serve).await?;
    let generation = i64::try_from(id.get())?;
    let policy = db
        .query_one(
            "SELECT g.manifest_format, p.fixture, ARRAY(
                SELECT m.rfc_id::integer FROM rfc.generation_documents m
                JOIN rfc.document_versions d ON d.identity = m.document_version
                JOIN rfc.policy_receipts r ON r.identity = d.policy_receipt
                WHERE m.generation_id = g.id AND r.decision = 'metadata_only'
                ORDER BY m.rfc_id
             )
             FROM rfc.generations g JOIN rfc.generation_policy p ON p.generation_id = g.id
             JOIN rfc.generation_ingestion i ON i.generation_id = g.id
             WHERE g.id = $1 AND i.scope = 'sample'
               AND i.policy_registry = encode(p.registry_digest, 'hex')",
            &[&generation],
        )
        .await?;
    assert_eq!(policy.try_get::<_, i32>(0)?, 4);
    assert!(policy.try_get::<_, bool>(1)?);
    assert_eq!(
        policy.try_get::<_, Vec<i32>>(2)?,
        TXT.iter().map(|&(rfc, _)| rfc).collect::<Vec<_>>()
    );
    let blobs = db
        .query(
            "SELECT digest::bytea, byte_length::bigint FROM rfc.blobs",
            &[],
        )
        .await?;
    assert_eq!(blobs.len(), 3);
    for row in blobs {
        let digest = BlobDigest::from_bytes(row.try_get::<_, &[u8]>(0)?.try_into()?);
        let length = row.try_get(1)?;
        let contents = store.read_range(
            digest,
            ByteRange::new(0, length).ok_or("invalid blob length")?,
            CAP,
        )?;
        assert_eq!(BlobDigest::hash(&contents), digest);
        assert_eq!(store.open_blob(digest)?.metadata()?.len(), length as u64);
        assert!(TXT.iter().any(|(_, bytes)| *bytes == contents) || contents == CATALOG);
    }
    for &(id, source) in TXT {
        let digest = BlobDigest::hash(source);
        let ranges = db.query(
            "SELECT d.rfc_id::integer, r.start_byte::bigint, r.end_byte::bigint
             FROM rfc.document_versions d JOIN (
                SELECT document_version, start_byte, end_byte FROM rfc.sections
                UNION ALL SELECT document_version, heading_start_byte, heading_end_byte FROM rfc.sections
                UNION ALL SELECT document_version, start_byte, end_byte FROM rfc.text_chunks
                UNION ALL SELECT document_version, start_byte, end_byte FROM rfc.notice_ranges
                UNION ALL SELECT document_version, start_byte, end_byte FROM rfc.relation_evidence WHERE start_byte IS NOT NULL
             ) r ON r.document_version = d.identity WHERE d.rfc_id = $1::integer", &[&id],
        ).await?;
        assert_eq!(ranges.len(), 4);
        for row in ranges {
            let range =
                ByteRange::new(row.try_get(1)?, row.try_get(2)?).ok_or("invalid indexed range")?;
            assert_eq!(
                store.read_range(digest, range, CAP)?,
                range.select(source).ok_or("range outside fixture")?
            );
        }
    }
    println!("verified {} {manifest}", id.get());
    Ok(())
}
