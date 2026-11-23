//! Generation-scoped RFC search.

mod cursor;
mod engine;
pub mod query;
mod reads;
#[cfg(target_os = "linux")]
mod sandbox;
mod snippets;
mod sql;

pub use engine::{
    Coverage, Excerpt, Hit, NoticeGroup, OriginalRange, Search, SearchError, SearchPage,
    SearchRequest,
};
pub use reads::{GetRequest, ListRequest, TableRequest};
#[cfg(target_os = "linux")]
pub use sandbox::{sandbox_probe, sandbox_worker};
