//! Generation-scoped RFC search.

mod cursor;
mod engine;
pub mod query;
mod reads;
mod rg;
#[cfg(target_os = "linux")]
mod sandbox;
mod snippets;
mod sql;
mod worker;

pub use engine::{
    Coverage, Excerpt, Hit, NoticeGroup, OriginalRange, Search, SearchError, SearchPage,
    SearchRequest,
};
pub use reads::{GetRequest, ListRequest, TableRequest};
pub use rg::{Rg, RgRequest};
#[cfg(target_os = "linux")]
pub use sandbox::{sandbox_probe, sandbox_worker};
pub use snippets::SourceVerification;
