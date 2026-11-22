use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::hmac;
use serde::{Deserialize, Serialize};

use crate::engine::SearchError;

const VERSION: u8 = 1;
const TOOL: &str = "search";
const MAX_CURSOR_BYTES: usize = 512;
const TAG_BYTES: usize = 48;
pub(crate) const MAX_AGE_SECONDS: u64 = 30 * 60;

pub(crate) struct CursorKey(hmac::Key);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Cursor {
    version: u8,
    tool: String,
    sort: String,
    query: String,
    pub generation: i64,
    pub sequence: i64,
    pub after: i32,
    pub expiry: u64,
}

impl Cursor {
    pub fn new(query: String, generation: i64, sequence: i64, after: i32, expiry: u64) -> Self {
        Self {
            version: VERSION,
            tool: TOOL.into(),
            sort: "id_asc".into(),
            query,
            generation,
            sequence,
            after,
            expiry,
        }
    }
}

impl CursorKey {
    pub fn new(key: &[u8; TAG_BYTES]) -> Self {
        Self(hmac::Key::new(hmac::HMAC_SHA384, key))
    }

    pub fn encode(&self, cursor: &Cursor) -> Result<String, SearchError> {
        let mut bytes = serde_json::to_vec(cursor).map_err(|_| SearchError::Unavailable)?;
        let tag = hmac::sign(&self.0, &bytes);
        bytes.extend_from_slice(tag.as_ref());
        let encoded = URL_SAFE_NO_PAD.encode(bytes);
        if encoded.len() > MAX_CURSOR_BYTES {
            return Err(SearchError::Unavailable);
        }
        Ok(encoded)
    }

    pub fn decode(&self, encoded: &str, query: &str, now: u64) -> Result<Cursor, SearchError> {
        if encoded.len() > MAX_CURSOR_BYTES {
            return Err(SearchError::CursorInvalid);
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| SearchError::CursorInvalid)?;
        let (payload, tag) = bytes
            .split_at_checked(bytes.len().saturating_sub(TAG_BYTES))
            .ok_or(SearchError::CursorInvalid)?;
        hmac::verify(&self.0, payload, tag).map_err(|_| SearchError::CursorInvalid)?;
        let cursor: Cursor =
            serde_json::from_slice(payload).map_err(|_| SearchError::CursorInvalid)?;
        if cursor.version != VERSION
            || cursor.tool != TOOL
            || cursor.sort != "id_asc"
            || cursor.query != query
            || cursor.generation < 0
            || cursor.sequence < 1
            || cursor.after < 1
            || cursor.expiry > now.saturating_add(MAX_AGE_SECONDS)
        {
            return Err(SearchError::CursorInvalid);
        }
        if cursor.expiry <= now {
            return Err(SearchError::CursorExpired);
        }
        Ok(cursor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_bind_context_and_expire() {
        let key = CursorKey::new(&[7; 48]);
        let cursor = key
            .encode(&Cursor::new("query".into(), 1, 2, 3, 1900))
            .unwrap();
        assert_eq!(key.decode(&cursor, "query", 100).unwrap().after, 3);
        assert!(matches!(
            key.decode(&cursor, "different", 100),
            Err(SearchError::CursorInvalid)
        ));
        assert!(matches!(
            key.decode(&cursor, "query", 1900),
            Err(SearchError::CursorExpired)
        ));
        assert!(matches!(
            CursorKey::new(&[8; 48]).decode(&cursor, "query", 100),
            Err(SearchError::CursorInvalid)
        ));
        let mut changed = URL_SAFE_NO_PAD.decode(&cursor).unwrap();
        changed[10] ^= 1;
        assert!(matches!(
            key.decode(&URL_SAFE_NO_PAD.encode(changed), "query", 100),
            Err(SearchError::CursorInvalid)
        ));
        assert!(matches!(
            key.decode(&"a".repeat(513), "query", 100),
            Err(SearchError::CursorInvalid)
        ));
    }
}
