# RFC redistribution and search

RFC MCP gives readers access to authentic Internet standards and the rest of
the RFC Series. Sync keeps verified official TXT intact, including copyright
notices, legal legends, authorship, line breaks, and form feeds. Serve checks
the BLAKE3 digest before using the bytes and reports the publication status
from the RFC Index. The service returns exact original bytes with attribution.

## Rights by operation

The policy evaluates internal copying and search, complete reproduction,
sequential whole-file delivery, and unmodified quotation independently.
RFC 8721 §§4.1–4.4 gives the dissemination and quotation objectives. The
actual terms come from the applicable outbound grant. The RFC Editor and IPMC
publish affirmative guidance supporting complete reproduction. A separate
grant supports quotation, including quotation of an identified Code Component.
[BCP 78](https://www.rfc-editor.org/rfc/rfc5378.html) ·
[RFC 8721 §4](https://www.rfc-editor.org/rfc/rfc8721.html#section-4) ·
[RFC Editor guidance](https://www.rfc-editor.org/series/rfc-use/) ·
[IPMC guidance](https://www.ietf-ipm.org/faq/)

| Evidence | Operations covered |
| --- | --- |
| RFC Series guidance and applicable historic distribution notices | Complete unmodified reproduction. Sequential delivery uses the same fixed original, with the required context and attribution on each page where the terms permit it. |
| Historical grants and notices | Apply the actual terms in the original, including the explanatory and implementation grant under RFC 2026 §10.4 and Code Component terms under RFCs 3667, 3978, and 4748. |
| IETF Stream TLP 1.0–5.0 | Apply §3.c(i) for complete reproduction and §3.c(iii) for attributed unmodified quotation. Apply the relevant §4 grant to identified Code Components when its conditions are met. |
| IAB, IRTF, and Independent Streams | Apply the applicable outbound terms, including TLP §§8 and 3.a(i) where in effect, and any document-specific grant. |
| Editorial Stream | Apply the general whole-copy guidance and any affirmative document-specific quotation grant. Check for final IPMC terms and their effective dates before applying TLP 6.0. |

When a publication month crosses a TLP effective date, use permissions common
to the plausible editions. Identify the applicable licenses in diagnostics.
[RFC 2026 §10](https://www.rfc-editor.org/rfc/rfc2026.html#section-10) ·
[RFC 3667](https://www.rfc-editor.org/rfc/rfc3667.html) ·
[RFC 3978](https://www.rfc-editor.org/rfc/rfc3978.html) ·
[RFC 4748](https://www.rfc-editor.org/rfc/rfc4748.html) ·
[TLP archive](https://trustee.ietf.org/documents/trust-legal-provisions/) ·
[Corrected TLP 5.0](https://trustee.ietf.org/documents/trust-legal-provisions/tlp-5/) ·
[RFC 5743](https://www.rfc-editor.org/rfc/rfc5743.html) ·
[RFC 5744](https://www.rfc-editor.org/rfc/rfc5744.html) ·
[RFC 5745](https://datatracker.ietf.org/doc/html/rfc5745) ·
[RFC 9920 §6.1](https://www.rfc-editor.org/rfc/rfc9920.html#section-6.1) ·
[IPMC provisions](https://www.ietf-ipm.org/tlp/)

Interpret no-modification and no-derivatives legends according to their scope.
A standard Pre-5378 legend identifies potential limits on inherited rights.
The applicable general quotation grant supports unmodified quotations unless
specific evidence identifies affected material. Check an independent
historical, third-party, or code license for those ranges. Descriptions of
copyright and BSD terms in the body carry no denial by themselves.

Serve can search a verified original internally and return the occurrence's
coordinates and official URL when it lacks a basis to quote the match. It
returns exact quote bytes when the grant and required notice material support
them. Response notice groups carry attribution and applicable conditions once
per RFC per page. [MCP API](mcp.md)

## Diagnose rights

Run the Sync CLI privately. Use the active generation by default or pass
`--generation ID` to inspect a retained generation.

~~~sh
rfc-mcp-docker-sync policy explain --rfc 8174
rfc-mcp-docker-sync policy audit --status unresolved --format json
~~~

These commands report the license evidence, original ranges, unresolved
operations, and emergency denials. Sync applies the policy on verified
material during ordinary reconciliation. Publication continues to depend on
acquisition proof, integrity, and the publication sequence.

## Restrict an operation or recover the ledger

Inspect the current epoch with `policy explain` or `status --json`. Pass
`--deny full_reproduction`, `--deny pagination`, `--deny excerpt`, or
`--deny internal_search`. A full-reproduction denial covers both delivery
forms. An excerpt denial allows metadata-only matches where internal search
is authorized. Internal-search denial leaves body predicates unknown and
excludes the original from regex scanning. The existing `--ceiling` values
retain their effects on public bytes. Restrictions accumulate.

~~~sh
rfc-mcp-docker-sync policy restrict --rfc "$RFC" --deny excerpt \
  --reason rights_revoked --review-ref "$REFERENCE" --expect-epoch "$EPOCH"
~~~

The commit takes effect across Serve replicas and readable generations.
It expires earlier cursors and invalidates buffered responses that have
yet to pass the final rights fence. Keep ingress closed after restoring
an older backup until the latest verified restriction ledger has been
replayed in epoch order and the final epoch and effective rights have
been checked. Back up `rfc.policy_state` and `rfc.policy_restrictions`
with the paired database and mirror inventory. [Publication and recovery](publication.md) ·
[Deployment](deployment.md)
