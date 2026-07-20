# M4.13 data and provenance handoff

Status: implemented and reproduced against the frozen real artifacts on
2026-07-20

## Verdict

Lane A is complete. The preparer fails closed on the official VitaminC source,
all three extracted split hashes, the full M3 replay artifacts, the consumed
M4.12 diagnostic and the corrected M4.10 transfer identities. It reproduced
the pre-registered train, development and terminal-reserve identities exactly.

This is a data/provenance result, not evidence that the M4.13 objective improves
the verifier. No model output is read during train/development or reserve
selection. Source labels are used only for the two frozen strata.

## Owned paths

This lane adds only:

- `configs/m4/verifier/change_aware_v1.json`
- `training/m4_13_verifier/prepare.py`
- `training/m4_13_verifier/__init__.py`
- `tests/m4/change_aware_verifier/data/**`
- this handoff

Generated text, manifests and sealed rows remain outside Git.

## External output contract

The real run writes:

```text
<output-root>/
  source/source_manifest.json
  prepared/train_vitaminc.jsonl
  prepared/development_vitaminc.jsonl
  prepared/train_vitaminc_manifest.json
  prepared/development_vitaminc_manifest.json
  prepared/train_m3_replay.jsonl
  prepared/development_m3.jsonl
  prepared/test_m3.jsonl
  prepared/dataset_manifest.json
  sealed/terminal_reserve.jsonl
  sealed/terminal_reserve_manifest.json
  sealed/terminal_reserve_identity.json
```

`train_vitaminc.jsonl` and `development_vitaminc.jsonl` use schema
`groundloop-m4-13-vitaminc-row-v1`. Every row contains:

```text
schema_version, split, stratum, selection_key, unique_id, case_id,
page, normalized_page_sha256, wiki_revision_id, revision_type,
source_label, label, claim, evidence, claim_sha256, evidence_sha256
```

The stored `label` is one of `support`, `refute`, `neutral`. `source_label`
retains the VitaminC literal. The base-logit order remains contradiction,
entailment, neutral; the stored-probability order remains support, refute,
neutral.

The two train/development identity manifests are canonical JSON arrays. Each
entry has only:

```text
stratum, selection_key, unique_id, case_id, page, wiki_revision_id,
revision_type, label, claim_sha256, evidence_sha256
```

Their file SHA-256 is therefore also the pre-registered selection-manifest
identity. The complete M3 files are byte-identical copies of the frozen M3
prepared splits.

`prepared/dataset_manifest.json` is the training-facing contract. Its
`training_surface.artifacts` exposes only VitaminC train/development and M3
train/development files. It exposes the terminal reserve only by manifest hash,
identity hash, counts and normalized-page exclusion digest. It contains no
terminal row ID, page, text or label. The original M3 public-test identity is
also listed only in the sealed-terminal reference. `prepared/test_m3.jsonl`
exists because the frozen artifact tree requires it, but a training or
development command must not accept that path.

The terminal evaluator owns all files under `sealed/`. The terminal identity
has role `evaluator-only pre-frozen page-disjoint terminal diagnostic` and
binds the raw-row file, the canonical identity manifest, the exclusion digests,
zero exact M3 overlap, and all corrected M4.10 inputs. The reserve was frozen
before optimizer work; its separate representativeness note records that this
is not an untouched benchmark because M4.12 used a sibling label-stratified
sample from the same official test split.

## Real source audit

The run re-derived these dimensions rather than trusting the config:

| Official split | Rows | Cases | Raw pages | Normalized pages |
|---|---:|---:|---:|---:|
| train | 370,653 | 112,426 | 22,198 | 22,195 |
| development | 63,054 | 18,836 | 2,975 | 2,975 |
| test | 55,197 | 16,487 | 3,068 | 3,068 |

Normalized page intersections were train-development 18, train-test 22 and
development-test 2. One page occurs in all three. Taking the union, rather
than adding the pairwise counts, produces exactly 40 quarantined identities.
The quarantine digest is
`0d2dd3fbe64fb6759cdbac9c4bdae68529fffdaf0370d703310a28166e7b54da`.

After quarantine, eligible source-case pools were:

| Split | SUPPORT-REFUTE | SUPPORT-NEUTRAL |
|---|---:|---:|
| train | 35,346 | 7,416 |
| development | 5,945 | 1,416 |

The deterministic samples are:

| Split | Cases by stratum | Rows | Pages | S/R/N | Manifest SHA-256 |
|---|---:|---:|---:|---:|---|
| train | 512 + 512 | 4,096 | 1,024 | 2,048 / 1,024 / 1,024 | `d51ca3366328fd623148d3d9f0c01844aca73212831fef563d4839cf43238c2d` |
| development | 128 + 128 | 1,024 | 256 | 512 / 256 / 256 | `527e727273af7b7721509c625b721a489da88503fc994b7bca05215eb93580df` |

The preparer independently reconstructed M4.12's 128 consumed pages and
reproduced manifest
`214885784d13912ce603cc30eaed5904fbb588e493405767d66bbb3a490787d6`.
It then checked the canonical semantic result
`017fd20fb810652725846e214c884114c5afdc06cb009206e4bdaf899f9ecef7`
and all 512 prediction rows against source IDs and claim/evidence hashes. The
predictions file is
`0cd315438ff94d54923b8fdefc16ac1c20d9f9dc385ae2963251c31e04c4bc0e`.
Neither artifact participates in selection.

After excluding official train/development and consumed M4.12 pages, the test
pool contained 3,310 SUPPORT-REFUTE and 626 SUPPORT-NEUTRAL cases. Seed
20260721 selected 64 plus 64 cases, 512 rows and 128 distinct normalized pages.
The terminal identity is exactly:

```text
3dcfcba0b809e3bcfcf2f9c036c4946f484d706faa61eec8c3fa489fbcb11dc5
```

The full official VitaminC test and the selected reserve both have zero exact
normalized claim, evidence and pair overlap with all M3 prepared splits.

## Corrected M4.10 terminal bindings

The config no longer leaves M4.10 pending. Terminal validation requires all of:

| Identity | SHA-256 |
|---|---|
| v2 config | `6818e8611ff0790a289a61bf9712cf29927617c9d0b5c1c6c312169f4030c483` |
| source manifest | `c5bf4c45e43b6df308b5bc86abb5da007464c2ae006103a8727e26a42883542d` |
| model identity | `0bab4b0cdd4f3e1ff84c372be93c500e6b96f9a05775339f106c1b3537113647` |
| raw oracle/model artifacts | `83ed945ffe51690d96f9074353a869f54c179455237a24748513897a07fedaf8` |
| structural result | `92d7586c78f40ae447d97f703a51caf4dbdefc76ee911cde298dc841e1a5144d` |
| study manifest | `c2f009ea381d5ddff6295375d88efeece56c9fb4dbf7d3b0e1222df25af60438` |
| report | `71a52641b90ad908c6122f1750ac7724def08aee02d013393d24a04c2b44fd3e` |
| empirical bundle | `02229805627c4a2412be3257244e613bf4fd8c79e6a737b69b7b73777a6c66c6` |

`validate_terminal_prerequisites` fails if status is not `resolved`, any value
is null/malformed, or the corrected config schema is not v2.

## Exact real command

```bash
PYTHONPATH=src:. .venv/bin/python training/m4_13_verifier/prepare.py \
  --official-repository /tmp/vitaminc-official-20260720 \
  --vitaminc-archive /tmp/vitaminc-official-152c963.zip \
  --vitaminc-root /tmp/vitaminc-data-152c963/vitaminc \
  --m3-artifact-root models/m3/verifier-run-20260718 \
  --m4-12-config configs/m4/public_ai/vitaminc_revision_gate_v1.json \
  --m4-12-artifact-root \
    /tmp/groundloop-m4-12-vitaminc-real-final-20260720 \
  --output-root /tmp/groundloop-m4-13-data-real-e \
  --config configs/m4/verifier/change_aware_v1.json
```

The generated deterministic identities were:

| Artifact | SHA-256 |
|---|---|
| M4.13 config | `d50c2af5b5461f74e31fc759c3ca5e0aa074556cf0163789a4fe9615c8b7a11d` |
| source manifest | `3295b8022b31c3ff3c8e2329445b373c914ad466968b1638208b0c2dbfaeb0cc` |
| dataset manifest | `1a5ed4b7933c79cba5a09487a24c7dcd576e3518e1a27808d5f4afcbc3007e60` |
| train raw JSONL | `01459cc7b4cbe611c0967dec22b4ab53ef93f336dc71706475e8858d07c7a178` |
| development raw JSONL | `1a306620c363d52c6abfcdf5f6d272e7767cfdc8cbd364768b27901870a3c882` |
| terminal raw JSONL | `b7d3c61f041d7000626c4f1ce248c2c01a3dcde52de7f94701247e5efb124796` |
| terminal identity | `294c0199a99156e2a81546d2d274605a19ea98741c08c0246b0af133b15b6341` |

The first timed successful run completed in 11.22 seconds with peak RSS
569,320 KiB. That is preparation telemetry, not a model-training result.

## Validation

```text
pytest -o addopts='' -q tests/m4/change_aware_verifier/data
  11 passed, 1 skipped

ruff check training/m4_13_verifier tests/m4/change_aware_verifier/data
  All checks passed

mypy --strict --python-version 3.12 training/m4_13_verifier/prepare.py
  Success: no issues found in 1 source file

python -m compileall -q training/m4_13_verifier \
  tests/m4/change_aware_verifier/data
  passed
```

The ordinary run skips the explicit real-source preparation gate. With the
documented local paths supplied through its environment variables, the same
test suite reports `12 passed` in 10.67 seconds.

## Remaining limits

- VitaminC source labels are dataset annotations, not objective truth.
- The reserve is page-disjoint from training/development and M4.12 but comes
  from the same official test split that supplied the consumed diagnostic.
- The M3 public test has no REFUTE rows, so it cannot detect REFUTE forgetting.
- The preparer verifies corrected M4.10 identities; it does not adjudicate the
  Git claims or establish transfer quality.
- The official VitaminC split files and generated raw text are licensed
  external artifacts and must not be committed with model weights.
