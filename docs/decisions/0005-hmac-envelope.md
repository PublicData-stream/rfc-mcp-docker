# 0005 — Internal HMAC envelope

Sign canonical HTTP metadata and the original bounded JSON bytes with full-length HMAC-SHA-384. Use distinct keys for feed-to-sync, sync-to-serve, and cursor signing. Validate the receiving boot epoch, timestamp and nonce, then atomically record the nonce in the receiver's bounded in-memory cache before accepting an effect. Keep durable logical event IDs on the sync side.

MAC acknowledgments use a separate prefix and bind the initiating nonce. Rotation preserves a bounded previous-key window. Phase 06 implements the request and response formats and verifies golden vectors.
