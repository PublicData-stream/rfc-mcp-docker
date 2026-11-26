# 0011 — Audited source policy and cross-generation restrictions

Classify redistribution rights in Sync with a versioned registry and
evidence mapped to the original bytes. Seal the receipt with the document
version and the registry with the generation manifest. A maintainer-approved
registry pin permits automated decisions to enter the normal audited
publication path.

Serve uses receipt-backed excerpt decisions and reads mandatory restrictions
from PostgreSQL. A committed rights restriction caps operations across every
generation and invalidates older cursors. Serve checks the rights epoch again
at buffered response authorization. This keeps a retained generation, a
second replica, or rollback from reviving a permission.

The [source-policy guide](../source-policy.md) records rule applicability, the
review boundary, and operator commands.
