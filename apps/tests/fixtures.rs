use rfc_core::{BlobDigest, ByteRange, Coverage, Relation, RelationKind, RfcNumber};
use rfc_test_support::{HEADINGS, INVALID_UTF8, NOTICES, RELATIONS};
use serde_json::{Value, json};

#[test]
fn original_byte_coordinates_survive_json_escaping() {
    let text = std::str::from_utf8(HEADINGS).unwrap();
    let start = text.find("1.1. ").unwrap();
    let end = start + text[start..].find("2. Redundant title").unwrap();
    let range = ByteRange::new(start as i64, end as i64).unwrap();
    let original = range.select(HEADINGS).unwrap();
    let value = json!({ "content": std::str::from_utf8(original).unwrap() });
    let round_trip: Value = serde_json::from_slice(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert_eq!(round_trip["content"].as_str().unwrap().as_bytes(), original);
    assert_eq!(original, "1.1. café and cafe\u{301}\r\n".as_bytes());
    assert!(HEADINGS.contains(&0x0c));

    let continuation = text.find('é').unwrap() + 1;
    let invalid = ByteRange::new(continuation as i64, end as i64).unwrap();
    assert!(std::str::from_utf8(invalid.select(HEADINGS).unwrap()).is_err());
    assert!(std::str::from_utf8(INVALID_UTF8).is_err());
    assert_ne!(BlobDigest::hash(HEADINGS), BlobDigest::hash(INVALID_UTF8));
}

#[test]
fn fixture_graph_and_notice_expectations_are_explicit() {
    let graph: Value = serde_json::from_str(RELATIONS).unwrap();
    let edge = Relation {
        source: RfcNumber::new(100).unwrap(),
        kind: RelationKind::NormativeReference,
        target: RfcNumber::new(101).unwrap(),
    };
    assert_eq!(
        graph["edges"][0],
        json!([edge.source.get(), "normative_reference", edge.target.get()])
    );
    let coverage = match graph["outgoing_reference_coverage"]["101"].as_str() {
        Some("partial") => Coverage::Partial,
        actual => panic!("unexpected coverage {actual:?}"),
    };
    assert_eq!(coverage, Coverage::Partial);
    assert_eq!(graph["catalog_inbound_obsolescence_coverage"], "partial");

    let notices: Value = serde_json::from_str(NOTICES).unwrap();
    let needle = notices["reviewed"]["notice_start"].as_str().unwrap();
    assert!(std::str::from_utf8(HEADINGS).unwrap().contains(needle));
    assert_eq!(notices["unavailable"]["expected_state"], "unavailable");
}

#[test]
fn reviewed_originals_match_provenance() {
    let provenance: Value =
        serde_json::from_str(include_str!("../../fixtures/rfc/provenance.json")).unwrap();
    for (name, bytes) in [
        (
            "rfc2119.txt",
            include_bytes!("../../fixtures/rfc/rfc2119.txt").as_slice(),
        ),
        (
            "rfc8174.txt",
            include_bytes!("../../fixtures/rfc/rfc8174.txt").as_slice(),
        ),
    ] {
        assert_eq!(provenance[name]["bytes"], bytes.len());
        assert_eq!(
            provenance[name]["blake3"],
            BlobDigest::hash(bytes).to_string()
        );
        assert!(bytes.contains(&0x0c));
    }
}
