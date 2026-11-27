//! Versioned, deterministic manifest bytes for a sealed generation.

use rfc_core::BlobDigest;
use serde::Serialize;

pub const FORMAT_VERSION: i32 = 1;
pub const INGESTION_FORMAT_VERSION: i32 = 2;
pub const AUDIT_FORMAT_VERSION: i32 = 3;
pub const POLICY_FORMAT_VERSION: i32 = 4;
pub const SOURCE_FORMAT_VERSION: i32 = 5;

/// RFC number, TXT digest, document-version identity, catalog metadata identity.
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Member(pub i32, pub String, pub String, pub String);

/// Source RFC, forward kind, target RFC and sorted evidence identities.
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Edge(pub i32, pub String, pub i32, pub Vec<String>);

/// Scope, optional RFC, family and coverage state.
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Coverage(pub String, pub Option<i32>, pub String, pub String);

/// Scope, catalog observation identities, policy version and catalog-only metadata.
#[derive(Serialize)]
pub struct Ingestion(
    pub String,
    pub String,
    pub String,
    pub Option<String>,
    pub serde_json::Value,
);

/// Sealed selection and one normalized proof row per RFC, in numeric order.
#[derive(Serialize)]
pub struct Audit(pub serde_json::Value, pub Vec<serde_json::Value>);

/// Inputs loaded from the locked generation in one database transaction.
pub struct Manifest {
    pub parser_version: String,
    pub index_version: String,
    pub members: Vec<Member>,
    pub edges: Vec<Edge>,
    pub coverage: Vec<Coverage>,
    pub ingestion: Option<Ingestion>,
    pub audit: Option<Audit>,
    pub policy: Option<(String, String, bool)>,
}

impl Manifest {
    pub fn version(&self) -> i32 {
        if self.policy.is_some() {
            POLICY_FORMAT_VERSION
        } else if self.audit.is_some() {
            if self
                .ingestion
                .as_ref()
                .is_some_and(|ingestion| ingestion.3.is_none())
            {
                SOURCE_FORMAT_VERSION
            } else {
                AUDIT_FORMAT_VERSION
            }
        } else if self.ingestion.is_some() {
            INGESTION_FORMAT_VERSION
        } else {
            FORMAT_VERSION
        }
    }

    /// Returns the exact persisted representation and its BLAKE3 digest.
    pub fn encode(mut self) -> (Vec<u8>, BlobDigest) {
        for edge in &mut self.edges {
            edge.3.sort_unstable();
        }
        self.members.sort_unstable();
        self.edges.sort_unstable();
        self.coverage.sort_unstable();
        let format = self.version();
        let inputs = (
            self.parser_version,
            self.index_version,
            self.members,
            self.edges,
            self.coverage,
        );
        let bytes = match (self.ingestion, self.audit, self.policy) {
            (Some(ingestion), Some(audit), Some(policy)) => serde_json::to_vec(&(
                POLICY_FORMAT_VERSION,
                inputs.0,
                inputs.1,
                inputs.2,
                inputs.3,
                inputs.4,
                ingestion,
                audit,
                policy,
            )),
            (Some(ingestion), Some(audit), None) => serde_json::to_vec(&(
                format, inputs.0, inputs.1, inputs.2, inputs.3, inputs.4, ingestion, audit,
            )),
            (Some(ingestion), None, None) => serde_json::to_vec(&(
                INGESTION_FORMAT_VERSION,
                inputs.0,
                inputs.1,
                inputs.2,
                inputs.3,
                inputs.4,
                ingestion,
            )),
            (None, None, None) => serde_json::to_vec(&(
                FORMAT_VERSION,
                inputs.0,
                inputs.1,
                inputs.2,
                inputs.3,
                inputs.4,
            )),
            _ => unreachable!("audit and policy evidence require ingestion metadata"),
        }
        .expect("manifest contains only JSON primitives and arrays");
        let digest = BlobDigest::hash(&bytes);
        (bytes, digest)
    }
}

/// Content identity for a typed, ordered JSON tuple within a stable domain.
/// Callers use scalars and arrays and include the record format in the domain.
pub fn identity(domain: &str, fields: &impl Serialize) -> Result<BlobDigest, serde_json::Error> {
    let bytes = serde_json::to_vec(&(domain, fields))?;
    Ok(BlobDigest::hash(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(reverse: bool) -> Manifest {
        let mut members = vec![
            Member(3, "txt3".into(), "doc3".into(), "meta3".into()),
            Member(1, "txt1".into(), "doc1".into(), "meta1".into()),
        ];
        let mut evidence = vec!["e2".into(), "e1".into()];
        if reverse {
            members.reverse();
            evidence.reverse();
        }
        Manifest {
            parser_version: "parser-é".into(),
            index_version: "index-1".into(),
            members,
            edges: vec![Edge(1, "updates".into(), 3, evidence)],
            coverage: vec![Coverage(
                "catalog".into(),
                None,
                "updates".into(),
                "partial".into(),
            )],
            ingestion: None,
            audit: None,
            policy: None,
        }
    }

    #[test]
    fn manifest_preserves_the_golden_representation() {
        let (bytes, digest) = sample(false).encode();
        assert_eq!(bytes, r#"[1,"parser-é","index-1",[[1,"txt1","doc1","meta1"],[3,"txt3","doc3","meta3"]],[[1,"updates",3,["e1","e2"]]],[["catalog",null,"updates","partial"]]]"#.as_bytes());
        assert_eq!(
            digest.to_string(),
            "6b5a42ee1e42b0746f35e4278e36ac04816a7e27c9f283ece2b2fc58c686119c"
        );
        assert_eq!(sample(true).encode(), (bytes, digest));
        let mut changed = sample(false);
        changed.coverage[0].3 = "complete".into();
        assert_ne!(changed.encode().1, digest);
    }

    #[test]
    fn ingestion_manifest_binds_scope_and_provenance() {
        let snapshot = |scope: &str, reverse: bool| {
            let mut manifest = sample(reverse);
            manifest.ingestion = Some(Ingestion(
                scope.into(),
                "before".into(),
                "after".into(),
                Some("policy-1".into()),
                serde_json::json!([[8, {"title": "PDF-only"}]]),
            ));
            assert_eq!(manifest.version(), INGESTION_FORMAT_VERSION);
            manifest.encode()
        };
        let (bytes, digest) = snapshot("sample", false);
        assert!(bytes.starts_with(b"[2,"));
        assert_eq!(snapshot("sample", true), (bytes, digest));
        assert_ne!(snapshot("full", false).1, digest);
    }
}
