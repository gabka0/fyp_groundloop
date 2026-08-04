# M5.5 independent WiCE source audit

Date: 2026-08-03

Status: independently reproduced; no discrepancy against the coordinator
preflight

## Source identity and license boundary

The adapter consumed the detached official `ryokamoi/wice` checkout at commit
`ddeb6c183665e2a20c5f03c5aa07f03888b9870f`. The checked-in manifest is
`configs/m5/wice_source_manifest_v1.json`; its canonical content hash is
`c4a186f42252c280b5cafdc26b182e88c5aec14072866af15dd86d4b4c4722f2`.

At runtime, frozen revision metadata is enforced and exact consumed bytes are
verified against manifest SHA-256 before parsing. This verification uses the
declared revision plus byte/hash inventory; the loader does not inspect or
claim to inspect Git HEAD.

WiCE annotations are recorded as ODC-BY. Wikipedia and Common Crawl terms
continue to apply to underlying text. No downloaded JSONL, source text,
generated event corpus, or text-bearing result is committed here.

## Independent byte inventory

The lane recomputed commit identity, SHA-256, byte counts, and physical JSONL
row counts directly from `/tmp/groundloop-wice-source` before running the
adapter:

| Split | Kind | Rows | Bytes | SHA-256 |
|---|---:|---:|---:|---|
| train | claim | 1,260 | 12,039,943 | `3ef74c7203e1d9b369cb2c145e764d8ab4743f008f9765fa47f9bdd6ffa2d2e1` |
| dev | claim | 349 | 3,490,479 | `67531ca79bde4c81d3752fb69d9cb0d3d6763de5c3028f548b83ef053a6f8042` |
| test | claim | 358 | 3,624,529 | `4c91b9e9590cfcd8f8a0f7288b5ff315af8ade946d7bf26f50fbd671bb86dbe8` |
| train | subclaim | 3,470 | 33,175,462 | `e8ba1ed589e22c5f9d41c8d858dd10bdc17665d224f8ce28be9d9024239cf513` |
| dev | subclaim | 949 | 9,646,735 | `4747a5a60c85c0c12cbf1cc7a2ab4bc9279fd4fd01a83d36630b7151330b6f57` |
| test | subclaim | 958 | 9,431,288 | `3202b7bff5979dfbca745f48edd8da5ad166da0886fd15042cc1ab9d2010ee19` |

Supplementary hashes also matched:

- `LICENSE.md`: `f96619e6c5d30955769f82eb72a20f816361c2682c099e6e689e5e82a0352757`
- `README.md`: `8359490c1b93bded804ab7760c0f8aa064a22049101d02ed69c8acb51965ea24`

The adapter refuses to parse any JSON row until all eight manifest files have
passed their byte-count, row-count, and SHA-256 checks.

## Independently derived cohort

The lane then derived records from raw rows using the frozen 29-code-point
normalizer, atomic evidence sets, a 1,200-character rendered-input cap,
content-hash deduplication, and independent bounded matching:

| Split | Parent rows | Subclaim rows | Same-split byte-equal mappings | Source-supported parents | Representable primary parents | Hall complete | Hall failing | Primary overlength annotations |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| train | 1,260 | 3,470 | 3,470 | 460 | 460 | 362 | 98 | 0 |
| dev | 349 | 949 | 949 | 115 | 114 | 98 | 16 | 4 |
| test | 358 | 958 | 958 | 111 | 110 | 89 | 21 | 2 |
| **Total** | **1,967** | **5,377** | **5,377** | **686** | **684** | **549** | **135** | **6** |

There were no mapping mismatches in the official source. One dev parent and
one test parent were excluded because an otherwise positive requirement had no
valid unit after overlength rejection. Across every source row, including
rows outside the primary cohort, the adapter exposed 13 overlength annotation
rejects. The frozen primary-candidate scope accounts for 6 of those (0/4/2).
Those scopes are separate in the report.

The retained primary cohort contains 684 claims, 1,796 requirements, 3,573
least-ordinal controlled projections, and 135 Hall-failing claims. Every Hall
failure remains source-supported and is measured as a possible
distinct-representative policy false invalidation. It is never called negative
gold and is never filtered from the SDR applicability cohort.

## Discrepancy verdict

No source hash, source count, mapping count, eligibility count, or Hall count
disagreed with `COORDINATOR_SOURCE_PREFLIGHT.md`. This is an independent
reproduction, not a hard-coded comparison table: no derived cohort count is
stored in the source manifest or used to produce adapter output.
