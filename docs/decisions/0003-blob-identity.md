# 0003 — BLAKE3 blob identity

Use unkeyed BLAKE3 over the original source bytes and retain all 32 digest bytes. Render the digest as 64 lowercase hex characters and store blobs under the first two hex characters and full digest. A verified existing blob may be reused. Inconsistent bytes at an existing digest path require quarantine and investigation.

Record upstream authenticity separately. BLAKE3 is the content identity. Official HTTPS verification establishes the provenance of bytes collected over rsync.
