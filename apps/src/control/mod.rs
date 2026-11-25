//! Private, single-host control traffic. The public MCP router does not use this module.

mod auth;
mod feed;
mod wire;

use serde::{Deserialize, Serialize};
use std::{
    io,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

pub use auth::Keys;
pub use feed::{FEED_LIMIT, FeedFormat, publications};
pub use wire::{Client, address, serve};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Link {
    FeedSync,
    SyncServe,
}

impl Link {
    fn roles(self) -> (&'static str, &'static str) {
        match self {
            Self::FeedSync => ("rfc-mcp-docker-feed", "rfc-mcp-docker-sync"),
            Self::SyncServe => ("rfc-mcp-docker-sync", "rfc-mcp-docker-serve"),
        }
    }

    fn key_prefix(self) -> &'static str {
        match self {
            Self::FeedSync => "RFC_FEED_SYNC",
            Self::SyncServe => "RFC_SYNC_SERVE",
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Route {
    Identify,
    FeedPoll,
    Sync,
    Generation,
}

impl Route {
    fn path(self) -> &'static str {
        match self {
            Self::Identify => "/internal/v1/identify",
            Self::FeedPoll => "/internal/v1/feed-poll",
            Self::Sync => "/internal/v1/sync-requested",
            Self::Generation => "/internal/v1/generation-committed",
        }
    }

    fn for_link(path: &str, link: Link) -> Option<Self> {
        let business = match link {
            Link::FeedSync => Self::Sync,
            Link::SyncServe => Self::Generation,
        };
        if link == Link::FeedSync && path == Self::FeedPoll.path() {
            return Some(Self::FeedPoll);
        }
        [Self::Identify, business]
            .into_iter()
            .find(|route| route.path() == path)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    FeedPublication,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncRequested {
    pub event_id: Uuid,
    pub observation_digest: String,
    pub observed_at: u64,
    pub reason: Reason,
}

impl SyncRequested {
    pub fn publication(rfc: u32, observed_at: u64) -> Self {
        let digest = blake3::derive_key("rfc-mcp feed publication event v1", &rfc.to_be_bytes());
        let (event_id, observation_digest) = event_identity(digest);
        Self {
            event_id,
            observation_digest,
            observed_at,
            reason: Reason::FeedPublication,
        }
    }
}

fn event_identity(digest: [u8; 32]) -> (Uuid, String) {
    (
        Uuid::new_v8(digest[..16].try_into().expect("16-byte UUID input")),
        blake3::Hash::from_bytes(digest).to_hex().to_string(),
    )
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationCommitted {
    pub event_id: Uuid,
    pub generation_id: i64,
    pub publication_sequence: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_digest: Option<String>,
}

pub enum Command {
    FeedPoll(FeedPoll),
    Sync(SyncRequested),
    Generation(GenerationCommitted),
}

impl Command {
    fn decode(route: Route, body: &[u8]) -> Result<Self> {
        match route {
            Route::FeedPoll => {
                let poll: FeedPoll = serde_json::from_slice(body)?;
                if let Some(known) = &poll.known {
                    digest(known)?;
                }
                Ok(Self::FeedPoll(poll))
            }
            Route::Identify => Err(invalid("unexpected control operation").into()),
            Route::Sync => {
                let event: SyncRequested = serde_json::from_slice(body)?;
                let digest = digest(&event.observation_digest)?;
                if event_identity(digest).0 != event.event_id || event.observed_at > 253_402_300_799
                {
                    return Err(invalid("invalid feed observation identity or time").into());
                }
                Ok(Self::Sync(event))
            }
            Route::Generation => {
                let event: GenerationCommitted = serde_json::from_slice(body)?;
                if event.generation_id < 0
                    || event.publication_sequence <= 0
                    || event.event_id.is_nil()
                {
                    return Err(invalid("invalid generation event").into());
                }
                if let Some(value) = &event.manifest_digest {
                    digest(value)?;
                }
                Ok(Self::Generation(event))
            }
        }
    }

    fn event_id(&self) -> Option<Uuid> {
        match self {
            Self::FeedPoll(_) => None,
            Self::Sync(event) => Some(event.event_id),
            Self::Generation(event) => Some(event.event_id),
        }
    }
}

fn digest(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid("invalid observation or manifest digest").into());
    }
    Ok(*blake3::Hash::from_hex(value)?.as_bytes())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FeedPoll {
    pub format: FeedFormat,
    pub known: Option<String>,
}

pub fn feed_snapshot(ids: impl IntoIterator<Item = u32>) -> String {
    let mut hash = blake3::Hasher::new_derive_key("rfc-mcp feed snapshot v1");
    for id in ids {
        hash.update(&id.to_be_bytes());
    }
    hash.finalize().to_hex().to_string()
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum FeedReply {
    Pending,
    Unchanged,
    Retry(u64),
    Publications(Vec<u32>),
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Feed(FeedReply),
    Accepted,
    Observed,
    Superseded,
}

#[derive(Clone, Copy)]
pub enum Rejection {
    Conflict,
    Unavailable,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Ack {
    event_id: Uuid,
    outcome: Outcome,
}

pub fn unix_seconds() -> io::Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_secs())
}

pub fn jitter(duration: Duration) -> Duration {
    let mut byte = [0_u8; 1];
    if getrandom::fill(&mut byte).is_err() {
        return duration;
    }
    duration.mul_f64(1.0 + f64::from(byte[0]) / 2550.0)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
