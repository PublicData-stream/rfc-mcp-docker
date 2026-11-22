# 0004 — Publication and reader locking

Install and synchronize immutable files before committing the active-generation pointer and outbox event in PostgreSQL. Use a publisher/GC coordination lock and transaction-scoped per-generation locks. Readers acquire shared locks in READ COMMITTED READ ONLY and recheck generation status in a new statement before accessing blobs. A retire operation takes the corresponding exclusive lock and rechecks eligibility.

Lock lifetime covers the reader's final blob access, including a ripgrep worker. Readers retain SELECT-only application access. The Phase 0 probe verifies that the shared lock works under that role and conflicts with an exclusive lock.
