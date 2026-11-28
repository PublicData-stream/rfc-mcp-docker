# RFC search DSL

Search returns RFC metadata in ascending numeric RFC order. Every page identifies the corpus generation and publication sequence, including continuations after a publication or rollback. Phase 4 exposes the Rust search interface and the private Compose fixture. Phase 5 wires the interface into the public MCP tool.

## Text predicates

Use `in::fulltext` for the official TXT body and `in::title` for the upstream catalog title.

| Example | Meaning |
|---|---|
| `in::fulltext:"TLS"` | Find the contiguous, case-sensitive bytes `TLS`, including inside `DTLS` |
| `in::title:"TLS"` | Find the same case-sensitive substring in the catalog title |
| `in::fulltext:'handshake TLS'` | Find both PostgreSQL `simple` lexemes anywhere in the same RFC |
| `in::title:'handshake TLS'` | Find both lexemes in the catalog title |
| `in::fulltext:"%_"` | Find a literal percent sign followed by an underscore |

Double quotes preserve punctuation, whitespace, line endings, and the original Unicode sequence. Single quotes use the pinned PostgreSQL dictionary for tokenization and case normalization. All terms must occur, in any order, including across chunks. An input with zero searchable terms reports a syntax error.

Escape the active quote delimiter, backslash, newline, carriage return, or tab with a backslash. For example, `in::fulltext:"line 1\r\nline 2"` matches across an original CRLF sequence. In `in::title:'a "quoted" word'`, the double quotes are part of the token input.

## Relationships and composition

Every predicate evaluates direct edges. The RFC on the left is the result candidate and `N` is a positive RFC number up to 2,147,483,647.

| Forward predicate | Inverse predicate |
|---|---|
| `relation::references:N` | `relation::referenced_by:N` |
| `relation::normative_reference:N` | `relation::normative_referenced_by:N` |
| `relation::informative_reference:N` | `relation::informative_referenced_by:N` |
| `relation::updates:N` | `relation::updated_by:N` |
| `relation::obsoletes:N` | `relation::obsoleted_by:N` |

`depend::id:N` is an alias of `relation::references:N`. The any-class predicate also includes unclassified references. `is::obsoleted:true` checks for a known incoming obsolescence edge. `is::obsoleted:false` requires complete catalog-wide knowledge of incoming obsolescence.

Use uppercase `AND`, `OR`, and `NOT`. Negation binds first, then `AND`, then `OR`. The `!` operator can precede a predicate or group. `NOT` requires parentheses.

```text
relation::references:8446 AND !is::obsoleted:true
(relation::updated_by:8174 OR relation::obsoleted_by:9110) AND in::title:'TLS'
relation::updates:8446 AND NOT(relation::obsoletes:8446)
```

The following inputs report errors with zero-based, half-open query-byte spans.

| Input | Reason |
|---|---|
| `int::fulltext:"TLS"` | Unknown predicate namespace |
| `in::title:'TLS` | Unclosed quote |
| `in::title:"a" in::title:"b"` | Missing boolean operator |
| `NOT is::obsoleted:true` | Missing parentheses |
| `relation::references:2147483648` | RFC number exceeds the supported range |
| `in::title:'%_'` | No searchable lexemes |

## Coverage, pages, and excerpts

Positive evidence can establish a match even when coverage is incomplete. An absent edge establishes false only when the relevant source and relationship family are complete. Negating UNKNOWN still yields UNKNOWN. Only final TRUE rows appear. An original blob that fails verification leaves an exact predicate UNKNOWN. The page reports how many evaluated rows had unknown evidence, and the catalog-only count when available.

PostgreSQL evaluates title, relationship, and full-text token predicates as sets over the pinned generation. Full-text token matches use the lexeme index. All normalized terms must be present. An indexed positive can be known on a partial index. A negative requires a compatible, complete index and internal search authorization. SQL combines these facts with three-valued logic. Coverage counts final UNKNOWN separately from documents with any unknown text or relationship predicate. Title-only searches can stop evaluating predicates at the first hit beyond the page and count coverage from the member range.

The optional `limit` defaults to 20 and accepts 1–100. PostgreSQL-native pages contain the next matching RFCs after the cursor. These pages count all evaluated RFCs through the last returned hit, including nonmatches. The final page also counts the remaining tail. `catalog_only` describes the pinned generation on every page. When a hit does not fit, the cursor leaves that hit for the next page. A successful native search with no remaining matches finishes with no cursor.

Any exact full-text predicate selects the hybrid executor. It can stop at 512 evaluated RFCs, 32 MiB of original reads, or 512 MiB of comparisons, calculated as original bytes times distinct exact full-text literals. A hybrid page can report `scan_truncated: true` and contain zero hits. Follow any `next_cursor` with the original search arguments. Expired cursors and unavailable generations require a fresh search. Signed schema 13 search cursors work across new and previous Serve images when the cursor key is retained.

The engine accepts up to 8 KiB of query text, 4 KiB per decoded literal, 128 syntax nodes, 16 levels of nesting, and 64 normalized token terms. The SQL statement timeout is 5 seconds and the search deadline is 15 seconds. A deadline returns `deadline_exceeded`. Exceeding the PostgreSQL temporary-file limit returns `resource_limit_exceeded`. Failed native requests return a typed error without a continuation.

Set `include_snippets` to request original-byte excerpts from positive full-text evidence. Snippets are materialized after selecting hits, using the same original-read and comparison budgets. PostgreSQL-native searches without snippets read no original blobs. Each excerpt carries original byte coordinates and refers to the document's complete notice group. The engine returns at most eight excerpts per document and groups excerpts for at most ten documents. Token excerpts require an exact round trip through PostgreSQL's token highlighting on up to 256 KiB of original UTF-8. Where policy, byte verification, notices, or the 256 KiB serialized search payload budget prevent excerpts, the confirmed hit remains available as metadata.

## Source rights and original matches

Catalog metadata and structural information remain available where text
redistribution is restricted. Full-text predicates use internally authorized
originals and can report metadata-only matches. Excerpts require a grant
covering the actual returned bytes and their required notices. Ripgrep scans
the verified whole original when internal search is authorized. A hit with
unresolved excerpt rights reports coordinates and the official URL.
[Source policy](source-policy.md)
