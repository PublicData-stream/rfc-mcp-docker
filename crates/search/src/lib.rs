//! Generation-scoped RFC search.

mod cursor;
mod engine;
pub mod query;
mod snippets;
mod sql;

pub use engine::{
    Coverage, Excerpt, Hit, NoticeGroup, OriginalRange, Search, SearchError, SearchPage,
    SearchRequest,
};
