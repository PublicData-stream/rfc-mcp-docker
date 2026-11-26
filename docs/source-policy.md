# RFC source policy

Sync evaluates redistribution rights against the verified official TXT and
the RFC Index. It seals the decision with the document version. Serve checks
that decision and the current rights restriction before releasing original
text. Metadata and structural ranges remain available when original text is
restricted.

## Rule review and applicability

The first automatic candidate rule covers the reviewed IETF Stream TLP 5.0
boilerplate. The IETF Trust archive dates TLP 5.0 to 2015-03-25. Corrected TLP
5.0 §3.c(iii) covers unmodified portions with attribution and requires the
original legends, legal notices, and indications of authorship for substantial
reproduction. RFC MCP includes the verified inventory on every excerpt.
The 2021 correction explains the historical Simplified BSD wording. The
classifier recognizes that wording while preserving the exact original
paragraph. Separate code license claims and third-party terms need their own
review.

The rule requires matching RFC number, publication period and stream, a
standard copyright and license context, and corroborated authorship from the
byline, author block and RFC Index. For a month that crosses an effective
date, Sync withholds automatic permission when the available evidence cannot
resolve applicability. Unknown templates, pre-5378 material with unresolved
rights, and conflicting or additional legal terms go to review. The classifier
implements the reviewed layouts and supported restriction patterns. Rule
review must evaluate missed legal notices as well as false matches before
approving the bundle.

- TLP 1.0–4.0 have distinct tracked effective periods. Their historical
  boilerplate, grant scope and exceptions require independent review.
- IAB, IRTF and Independent submissions have separate outgoing rights. The
  IETF Stream §3.c(iii) grant does not apply to them under TLP 5.0 §8.
- Editorial Stream and legacy documents require supporting evidence for
  their own grants. The checked-in RFC 2119 review permits the complete
  exact-digest original. RFC 8174 retains its exact-digest whole-document
  approval when automatic qualification is unavailable.
- A reviewed exception identifies its RFC, source digest, grant and any
  verified notice spans. A restriction takes priority across all generations.

[IETF Trust archive](https://trustee.ietf.org/documents/trust-legal-provisions/) ·
[Corrected TLP 5.0](https://trustee.ietf.org/documents/trust-legal-provisions/tlp-5/) ·
[RFC 5377](https://www.rfc-editor.org/rfc/rfc5377.html) ·
[RFC 5378](https://www.rfc-editor.org/rfc/rfc5378.html) ·
[RFC 5743](https://www.rfc-editor.org/rfc/rfc5743.html) ·
[RFC 5744](https://www.rfc-editor.org/rfc/rfc5744.html) ·
[RFC 5745](https://www.rfc-editor.org/rfc/rfc5745.html) ·
[RFC 2026](https://www.rfc-editor.org/rfc/rfc2026.html) ·
[RFC 9920](https://www.rfc-editor.org/rfc/rfc9920.html)

## Audit and activate a policy bundle

Run the Sync CLI in an isolated deployment restored from an independently
verified corpus backup. Audit validates the selected generation manifest and
rechecks the original bytes from its mirror. The report records coverage,
current and proposed decisions, and reason- and template-specific review
queues. The two approval denominators are the audited TXT corpus and the
documents in a supported applicable rule family. The source generation stays
unchanged.

~~~sh
rfc-mcp-docker-sync policy audit --format json > /tmp/policy-audit.json
rfc-mcp-docker-sync policy audit --status unresolved
rfc-mcp-docker-sync policy explain --rfc 8174
~~~

Review representative positives and negatives, inspect unresolved evidence,
and record maintainer approval of each applicable permissive rule and the
complete audit report. Before disclosing text under the new policy, validate
the candidate over the normal MCP transport and record the rollout decision.
Install the matching Sync build and pin the reviewed registry in the target
database using the report. The review reference identifies the maintainer's
approval of the exact report and bundle. Keep reports and their attestation
within the private operator boundary.

~~~sh
rfc-mcp-docker-sync policy approve --registry "$REGISTRY_DIGEST" \
  --audit-report /tmp/policy-audit.json --review-ref "$REVIEW_REFERENCE"
rfc-mcp-docker-sync --phase2-reconcile --full
rfc-mcp-docker-sync --phase3-publish "$GENERATION" --expect-sequence "$SEQUENCE"
~~~

The pin governs forward promotion. The daemon uses the same audited
candidate and promotion checks during its normal full runs. Requalification
is required when the bundle changes. Serve can read the current qualified
generation while a replacement is being built.

## Restrict an RFC

Use `policy explain` or `status --json` to read the current rights epoch.
Choose a ceiling of `metadata_only`, `whole_only` or `whole_paginated`.
The ceiling intersects the existing permission, so a restriction cannot
grant a new operation. Use `rights_revoked`, `legal_obligation_changed` or
`unresolved_rights` as the reason.

~~~sh
rfc-mcp-docker-sync policy restrict --rfc "$RFC" --ceiling metadata_only \
  --reason rights_revoked --review-ref "$REVIEW_REFERENCE" --expect-epoch "$EPOCH"
~~~

A committed restriction affects every Serve replica and every generation.
It expires earlier cursors and prevents a buffered response with an older
rights epoch from passing the final database check. Authorization linearizes
at that check while Serve holds the shared rights fence. Data already handed
to HTTP or received by a client cannot be recalled. Serve responses carry
`Cache-Control: no-store`.

Treat the append-only restriction ledger as recovery state. While operator
writes are paused, save an independent PostgreSQL data dump containing
`rfc.policy_restrictions`, `rfc.policy_state`, and `rfc.policy_approvals`.
Record the rights epoch and the dump digest with the backup inventory.
After restoring an older corpus backup, keep ingress closed while an
administrator verifies the ledger in a disposable schema 12 database.
Replay the verified restriction suffix in epoch order, preserving any
existing stricter restrictions. Verify the final epoch and reviewed pin
and check a restricted MCP read before reopening ingress. An earlier
Serve image must support the current ledger to reopen public ingress.
[Publication and recovery](publication.md) · [Deployment](deployment.md)
