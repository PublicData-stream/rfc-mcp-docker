//! Generation-scoped RFC search.

mod cursor;
mod engine;
pub mod query;
mod reads;
mod snippets;
mod sql;

pub use engine::{
    Coverage, Excerpt, Hit, NoticeGroup, OriginalRange, Search, SearchError, SearchPage,
    SearchRequest,
};
pub use reads::{GetRequest, ListRequest, TableRequest};
