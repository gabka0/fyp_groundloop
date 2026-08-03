# M5.5 WiCE source preflight

Status: coordinator input, not an M5.5 result

Date: 2026-08-03

## Frozen source

The M5 design names the official `ryokamoi/wice` repository at commit
`ddeb6c183665e2a20c5f03c5aa07f03888b9870f`. The commit exists in the
official repository and contains the six claim/subclaim JSONL files used by
the controlled adapter.

The source repository describes WiCE as a fine-grained textual-entailment
dataset derived from Wikipedia claims and their cited evidence. Its license
file releases the annotations under ODC-BY while retaining the applicable
Wikipedia and Common Crawl terms for underlying text. M5 therefore MUST NOT
commit downloaded JSONL files or relabel the underlying text as an
unrestricted GroundLoop artifact.

Primary sources:

- <https://github.com/ryokamoi/wice/commit/ddeb6c183665e2a20c5f03c5aa07f03888b9870f>
- <https://github.com/ryokamoi/wice/blob/ddeb6c183665e2a20c5f03c5aa07f03888b9870f/README.md>
- <https://github.com/ryokamoi/wice/blob/ddeb6c183665e2a20c5f03c5aa07f03888b9870f/LICENSE.md>
- <https://aclanthology.org/2023.emnlp-main.470/>

## Byte-level source inventory

Checksums below were computed from a detached checkout of the official commit.
They are frozen adapter inputs.

| Split | Kind | Rows | Bytes | SHA-256 |
|---|---:|---:|---:|---|
| train | claim | 1,260 | 12,039,943 | `3ef74c7203e1d9b369cb2c145e764d8ab4743f008f9765fa47f9bdd6ffa2d2e1` |
| dev | claim | 349 | 3,490,479 | `67531ca79bde4c81d3752fb69d9cb0d3d6763de5c3028f548b83ef053a6f8042` |
| test | claim | 358 | 3,624,529 | `4c91b9e9590cfcd8f8a0f7288b5ff315af8ade946d7bf26f50fbd671bb86dbe8` |
| train | subclaim | 3,470 | 33,175,462 | `e8ba1ed589e22c5f9d41c8d858dd10bdc17665d224f8ce28be9d9024239cf513` |
| dev | subclaim | 949 | 9,646,735 | `4747a5a60c85c0c12cbf1cc7a2ab4bc9279fd4fd01a83d36630b7151330b6f57` |
| test | subclaim | 958 | 9,431,288 | `3202b7bff5979dfbca745f48edd8da5ad166da0886fd15042cc1ab9d2010ee19` |

Additional provenance hashes:

- `LICENSE.md`: `f96619e6c5d30955769f82eb72a20f816361c2682c099e6e689e5e82a0352757`
- `README.md`: `8359490c1b93bded804ab7760c0f8aa064a22049101d02ed69c8acb51965ea24`

## Independent preflight expectations

These are coordinator-side exploratory counts, deliberately produced before
the M5.5 adapter exists. They are test expectations to reproduce or explain,
not accepted evaluation results. The preflight applied the frozen primary
cohort predicate, exact 29-code-point normalizer, atomic sentence-set rule,
1,200-character rendered-unit cap, identical-content coalescing, and exact
Hall condition over distinct content hashes.

| Split | Parent rows | Subclaim rows | Source-supported parents | Representable primary parents | Hall complete | Hall failing |
|---|---:|---:|---:|---:|---:|---:|
| train | 1,260 | 3,470 | 460 | 460 | 362 | 98 |
| dev | 349 | 949 | 115 | 114 | 98 | 16 |
| test | 358 | 958 | 111 | 110 | 89 | 21 |
| **Total** | **1,967** | **5,377** | **686** | **684** | **549** | **135** |

Preflight structural checks also found:

- every subclaim ID mapped to a same-split parent by removing one final
  decimal `-<ordinal>` suffix;
- every mapped subclaim evidence array was byte-equal to its parent array;
- train had no overlength units; dev had four and test had two;
- one dev parent and one test parent lost all valid units for at least one
  positive requirement and were therefore excluded from the representable
  primary cohort; and
- Hall-failing source-supported parents were retained, not filtered or
  relabelled as negative source truth.

The independent M5.5 implementation MUST derive its own records from the raw
JSONL and compare its output to these values. A mismatch is a failed preflight
until the discrepancy is traced to a documented interpretation or a defect in
this exploratory calculation. It MUST NOT hard-code these counts as output.

## Reproduction boundary

Downloaded source belongs outside the Git repository, for example:

```text
/tmp/groundloop-wice-source
```

The adapter accepts explicit source paths plus the frozen manifest, verifies
all hashes before parsing, and writes only small deterministic manifests and
reports to caller-selected artifact directories. Raw source rows, generated
event corpora, and text-bearing reports remain untracked.
