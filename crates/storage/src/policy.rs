//! Durable source policy evidence shared by the writer and readers.

use crate::manifest::identity;
use rfc_core::BlobDigest;
use serde::{Deserialize, Serialize};

pub const CLASSIFIER: &str = "rfc-source-policy-1";
pub const MAX_NOTICES: usize = 128;
pub const MAX_NOTICE_BYTES: usize = 64 * 1024;
pub const RIGHTS_LOCK: i64 = -4;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Policy {
    MetadataOnly,
    WholeOnly,
    WholePaginated,
    ExcerptAllowed,
}

impl Policy {
    pub fn name(self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::WholeOnly => "whole_only",
            Self::WholePaginated => "whole_paginated",
            Self::ExcerptAllowed => "excerpt_allowed",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "metadata_only" => Some(Self::MetadataOnly),
            "whole_only" => Some(Self::WholeOnly),
            "whole_paginated" => Some(Self::WholePaginated),
            "excerpt_allowed" => Some(Self::ExcerptAllowed),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Method {
    Automatic,
    Manual,
    Fallback,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct License {
    pub family: String,
    pub version: String,
    pub section: String,
    pub url: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Span {
    pub category: String,
    pub start_byte: i64,
    pub end_byte: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub rfc: u32,
    pub source_digest: String,
    pub metadata_digest: String,
    pub registry_digest: String,
    pub classifier: String,
    pub rule_id: String,
    pub template: Option<String>,
    pub license: Option<License>,
    pub decision: Policy,
    pub approval_method: Method,
    pub review: Option<String>,
    pub reason: Option<String>,
    pub attribution: Option<String>,
    pub notices: Vec<Span>,
    pub inventory_digest: String,
}

impl Receipt {
    pub fn digest(&self) -> Result<BlobDigest, serde_json::Error> {
        identity("rfc-policy-receipt-v1", self)
    }

    pub fn inventory_digest(notices: &[Span]) -> Result<BlobDigest, serde_json::Error> {
        identity("rfc-policy-notice-inventory-v1", &notices)
    }

    pub fn eligible_for_excerpt(&self) -> bool {
        self.decision == Policy::ExcerptAllowed
            && matches!(self.approval_method, Method::Automatic | Method::Manual)
            && self.reason.is_none()
            && self.license.is_some()
            && self
                .attribution
                .as_ref()
                .is_some_and(|value| !value.is_empty())
            && !self.notices.is_empty()
            && self.notices.len() <= MAX_NOTICES
            && Self::inventory_digest(&self.notices)
                .is_ok_and(|digest| digest.to_string() == self.inventory_digest)
    }
}
