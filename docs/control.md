# Phase 6 control plane

Run one feed process, one sync daemon, and one serve process per Compose deployment. Coordinate the [schema 6 migration](storage.md) and application restart in one maintenance window. Phase 5 and Phase 6 binaries each require their matching schema version. The sync and feed binaries start their daemons with no arguments. The existing storage, fixture, promotion, recovery, and garbage-collection commands remain available. The [Phase 6 Compose overlay](../scripts/phase6/compose.yml) is an offline test configuration. Production Compose hardening belongs to Phase 7.

## Private listeners and credentials

| Service | Settings | File access |
| --- | --- | --- |
| Feed | `RFC_FEED_SYNC_ADDR` points to the sync control address. `RFC_FEED_FORMAT` is `atom` (default) or `rss`. | Feed-to-sync key only |
| Sync | `RFC_SYNC_CONTROL_BIND` is the feed-control interface address. `RFC_SYNC_SERVE_ADDR` points to the serve notification address. | Both pairwise keys, the restricted database writer credential, and the writable mirror |
| Serve | `RFC_SERVE_NOTIFY_BIND` enables the notification listener. Omit it for MCP-only service. | Notification and cursor keys, the SELECT-only database credential, and the read-only mirror |

Addresses use numeric IP and port, for example `172.31.20.2:8783`. Use a stable private interface address from Compose IPAM for each listener. Give ingress its own address and bind `RFC_MCP_BIND` there. Keep the admin listener on loopback. Put feed and sync together on the feed-control network, and sync and serve together on the notification network. Publish only the ingress service. Internal HTTP/1.1 carries HMAC-authenticated messages on a single trusted host. The public proxy forwards MCP traffic.

For each direction, set the corresponding `RFC_FEED_SYNC_KEY_ID` and `RFC_FEED_SYNC_KEY_FILE` or `RFC_SYNC_SERVE_KEY_ID` and `RFC_SYNC_SERVE_KEY_FILE`. Key IDs use 1–32 ASCII letters, digits, underscores, or dashes. Provision 48 raw random bytes per key, for example from `openssl rand -out /path/to/private/key 48` under a private umask. Give each service a read-only mount of its own copy with mode 0400 or 0440 and ownership that grants that service read access. Verify file ownership and permissions inside the container after recreation. Keep the cursor key separate.

### Clock and key rotation

Synchronize the Docker host clock. Request timestamps use UTC Unix seconds with 60 seconds of skew tolerance. Receiver epochs change on restart. The replay cache keeps at most 8,192 authenticated nonces and fails closed under pressure. Entries can expire after 150 seconds once their timestamps are outside the accepted window. If a backward clock correction leaves requests rejected, check host synchronization and restart the affected receiver after correcting the clock. Check time and key IDs before interpreting an authentication rejection as a network failure.

Choose a new key ID and generate a fresh key. Install it as the active key on the receiver first. For that direction, set `_PREVIOUS_KEY_ID`, `_PREVIOUS_KEY_FILE`, and `_PREVIOUS_KEY_EXPIRES` under the same prefix, using a UTC Unix-second deadline up to 24 hours ahead. Restart the receiver, then update and restart the sender to use the new active key. Confirm an authenticated exchange under the new key, and remove the previous-key settings and mount after the overlap. Rotate the two links separately. A receiver signs each successful reply with the key used to authenticate its request. If the receiver restarts, a failed delivery discards the cached epoch. The next retry discovers the new epoch and sends the same event with a fresh nonce.

## Reconciliation and publication

Feed polls the official publication feed every 15 minutes plus jitter. Conditional validators advance only after its hints are acknowledged. The RFC publication identifier determines the observation fingerprint and event ID, so a replay, a switch between feed formats, or a restart can repeat the same hint safely. A feed hint acknowledges durable acceptance, which can refer to an existing job. Inspect `rfc.control_events.job_id`, the event status, and `rfc.sync_jobs.state` to see whether work has completed. A failed job is terminal after two retries. Later scheduled work can still reconcile independently.

Sync performs full reconciliation at startup and daily and checks the official catalog digest hourly. A changed catalog or a new feed event can also queue a full reconciliation. One running job and one pending rerun bound concurrency. PostgreSQL advisory key `-3` owns the daemon and `-4` serializes short queue transactions. The existing mirror and PostgreSQL corpus locks still fence ingestion, maintenance, and promotion. The job retains its candidate and expected publication sequence across a daemon crash. A verified full run commits a new publication with its provenance, even when the corpus content is unchanged. Under a promotion error, the previous active generation remains readable. A failed job can leave its ready candidate for inspection or disposal with `--phase3-discard`.

A publication and its outbox entry share a transaction. Pending entries hold the attempt count and next due time. A lost or unauthenticated reply leaves the entry pending. Check the configured notification address, receiver epoch, key IDs and expiry, and `rfc.notification_outbox.next_attempt_at` when investigating backlog. Old notifications are acknowledged as superseded after the receiver checks PostgreSQL. Serve resolves the active pointer from PostgreSQL even when notifications are down. A reader that pinned an earlier generation continues under the existing retirement rules.

For operator rollback, stop the sync daemon, obtain the current publication sequence, and use the [guarded rollback command](publication.md) while serve continues running. Restart sync after the incident is resolved. Its normal schedule resumes automatic promotion and outbox delivery. If sync loses its scheduler database connection, it exits so Compose can restart it and reacquire ownership. Configure the deployment restart policy accordingly.

## Transport payloads

Issue a POST to `/internal/v1/identify` with body `{}` and epoch `bootstrap`. The signed response carries `epoch`. Use that epoch on business routes. `sync-requested` takes `event_id` (UUID), `observation_digest` (64 lowercase hex characters), `observed_at` (Unix seconds), and `reason` (`feed_publication`). `generation-committed` takes `event_id`, `generation_id`, and `publication_sequence` as well as an optional `manifest_digest`. Business replies contain `event_id` and `outcome` (`accepted`, `observed`, or `superseded`).

Both reply types bind the raw JSON and HTTP status to the caller and initiating nonce. The response headers are `X-RFC-Request-Nonce`, `X-RFC-Receiver`, and `X-RFC-Response-Signature`. The response MAC uses a separate domain prefix from the request MAC. Phase 6 processes one bounded request per connection.

The request prefix starts with `RFMCP-HMAC-SHA384-V1`. In order, the remaining lines carry the key ID, caller, audience, epoch, Unix timestamp, nonce, `POST`, literal request path, `application/json`, and decimal body length. End the prefix with a blank line and append the original JSON bytes. Send the key ID, caller, audience, epoch, timestamp, nonce, and tag in their corresponding `X-RFC-*` headers.

The response prefix starts with `RFMCP-HMAC-SHA384-RESPONSE-V1`. The remaining lines carry the key ID, receiver, caller, request nonce, decimal HTTP status, `application/json`, and decimal body length, followed by a blank line and the original JSON bytes. Both prefixes use LF line endings. Encode the 32-byte nonce, receiver epoch, and full 48-byte tag as unpadded base64url.
