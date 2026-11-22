# 0004 — Publication and reader locking

Install and synchronize immutable files before committing the active-generation pointer and outbox event in PostgreSQL. Sync acquires a stable mirror lock before taking the session advisory lock. Readers acquire shared generation locks in PostgreSQL and on the mirror, then recheck generation status in a fresh statement inside `READ COMMITTED READ ONLY`. Retirement takes the corresponding exclusive locks and rechecks eligibility. The mirror lock protects ongoing filesystem access if a PostgreSQL session disconnects.

Lock lifetime covers the reader's final blob access, including a supervised worker. Readers retain SELECT-only application access. See the [operator commands and recovery contract](../publication.md).
