# M5-D26 Recovery Wave R6 Lane A Contracts/Digests Handoff

Status: scoped pure D25/D26 candidate ready for independent same-byte audit;
M5-D25, M5-D26, M5.0-25, and M5.0-26 implementation remain `PENDING`

Date: 2026-09-13

## 1. Authority and reconstructed checkpoint

This lane read the complete authority order in
`D26_MIGRATION_017_ACTIVATION.md` and started from exact integrated activation
commit `14598ae51562006eaf67850b19e8212f38997903`, tree
`36af2be4c58572c5adde68279d1a3aacedd0beff`, whose sole parent is
`b9bbf9b9124bf4297f3d7394069088906352dd25`. The activation commit changes
only `docs/workstreams/m5_runtime_implementation/D26_MIGRATION_017_ACTIVATION.md`.
Local `main` and `origin/main` both named that activation commit at lane entry.

The retained Lane A evidence stayed read-only and clean at exact commit
`6591d4779cd59bec4bcb9aa15c1b44901e0049f7`, tree
`35709442636bbbe33830867616e642d34dade185`. Its four commits were replayed in
the required order without conflict:

```text
held 079d3898741978e146eaec06aedc9ade881217d8
  -> replay 697c3e0b46e59d0c58791edf446967e894c8cc02
held 40317ee5d8c77f1a939822112f268a9fa393841f
  -> replay 71fbd766d2b55292d2d8ff6a85348775374cc0dc
held 6a95c22ae95a8d54f6e502a1fcc9245b68f40685
  -> replay 469dc50a8ab84a330a1ef0efdc45d5a3cb3c99d4
held 6591d4779cd59bec4bcb9aa15c1b44901e0049f7
  -> replay 143c144daf92a4ad2c3c5072928924d32a52dd9a
```

The exact post-replay checkpoint is commit
`143c144daf92a4ad2c3c5072928924d32a52dd9a`, tree
`5f5ac4a20ac78bfb053d509e409c6e40f9b0988b`. Before any D26 edit, its delta
from the activation was exactly the five held paths and every imported path
reproduced the frozen ledger:

```text
src/groundloop/m5/runtime/contracts.py =
  f441789a18f9381ffc9951e17788d7df7a728dc63de18cd4cc540fe2c8395d8c
src/groundloop/m5/runtime/digests.py =
  34f55999b77a9e96c831b4f664321bc8f558232d3bdf61237bf4ac4e038571f9
tests/m5/runtime/test_contracts.py =
  d0d244f55bc870f29d31afe9e29c6746b91712a8735e542633ee3ade6c599542
tests/m5/runtime/test_digests.py =
  b35785893462fcabeba10eee5486c24bd0ed03cfda64741ec3173f877bd799ea
docs/workstreams/m5_runtime_implementation/D25_CONTRACTS_DIGESTS_HANDOFF.md =
  3b77deaaa968b39f1115d17ee0eacd8bdc7aa39725487d885f8b105c2578e66f
```

## 2. Implemented D26 pure surface

Technical implementation commit
`255753871f5c47547efbac8b3578966482b8a6f5`, tree
`2060d42f0826e145faa17324d606203e303ac6e3`, has sole parent the exact
post-replay checkpoint. It changes only the two editable source/test paths.

The implementation adds one small pure helper whose name is an implementation
detail, not a newly frozen public contract. It computes exactly:

```text
stable_m5_digest(
  "m5-changed-state-absence-artifact-v1",
  *ENUM(kind), *TEXT(object_id))
```

It accepts exactly `requirement_state`, `group_state`, and
`group_certificate`, rejects the other three existing reference kinds and
unknown kinds, applies the existing nonempty identifier rule without changing
the bytes of a valid identifier, and uses only the existing typed M5 digest
primitives. It does not add a DTO, reference kind, tombstone, nullable hash,
JSON/`repr`/reflection encoding, normalization, delimiter join, SQL, or
database behavior.

The new tests cover:

- fixed Python golden vectors for all three legal kinds using the same
  non-ASCII object ID and an independent framing implementation;
- Unicode composed/decomposed inequality and preservation of surrounding
  nonempty whitespace without trimming;
- empty and whitespace-only identifier rejection;
- all three excluded existing kinds even though the forbidden domain digest
  itself can be independently computed, plus an unknown seventh-kind
  negative;
- domain, legal-kind, object-byte, output-byte, and prefix/framing mutations;
- unchanged outer `m5-changed-state-reference-v2` and
  `m5-changed-state-set-v2` fixed vectors;
- the exact six-value enum and fixed present-reference vectors for all four
  state kinds plus both direct certificate-digest kinds; and
- carried D25 empty logical-output/37-counter vectors and frozen M4-v1
  publication/claim-registry vectors.

The existing outer/set helpers, four present-state helpers, direct
certificate behavior, D25 logical-output/point/work recipes, M5 contracts,
and M4-v1 implementation were not edited.

## 3. Exact technical byte ledger

On technical commit `255753871f5c47547efbac8b3578966482b8a6f5`:

```text
src/groundloop/m5/runtime/contracts.py =
  f441789a18f9381ffc9951e17788d7df7a728dc63de18cd4cc540fe2c8395d8c
src/groundloop/m5/runtime/digests.py =
  ac6088de78808958ab9bf8016f4aa97acc7c0e61e93283542b1de9c79d54756f
tests/m5/runtime/test_contracts.py =
  d0d244f55bc870f29d31afe9e29c6746b91712a8735e542633ee3ade6c599542
tests/m5/runtime/test_digests.py =
  a99f9498448ebe29919bf3afb519b027298a38d38e41d55f7398059828e36327
docs/workstreams/m5_runtime_implementation/D25_CONTRACTS_DIGESTS_HANDOFF.md =
  3b77deaaa968b39f1115d17ee0eacd8bdc7aa39725487d885f8b105c2578e66f
```

The first, third, and fifth values remain byte-identical to the held ledger.
The final handoff-containing commit/tree and this handoff's own SHA-256 cannot
be embedded in these same bytes without self-reference; the coordinator and
independent reviewer must record them externally together with the five
technical hashes above.

Frozen authority and prerequisite bytes remained:

```text
PERSISTED_MATCHING_AMENDMENT.md =
  bac12ab5e74632c04f1bd70d0ef0d00522ba9d268eb8b73d11845bbf3b873aae
CHANGED_STATE_ABSENCE_AMENDMENT.md =
  85372d4c2f9108810bd75c3e5611de541d0f31c8a096421f30e68fad84676721
migrations/015_m5_runtime.sql =
  85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c
migrations/016_m5_runtime_recovery.sql =
  a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7
accepted 016 bundle =
  28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565
accepted empty oracle =
  e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
accepted 016 prerequisite =
  b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd
```

## 4. Executed verification

All successful final test commands used the repository virtual environment,
`PYTHONPATH=src`, `PYTHONDONTWRITEBYTECODE=1`, empty pytest addopts, and no
pytest cache provider.

```text
pytest tests/m5/runtime/test_contracts.py tests/m5/runtime/test_digests.py
  -> 140 passed in 0.43s

pytest tests/m5/runtime
  -> 420 passed in 3.43s

pytest
  tests/m5/runtime/test_contracts.py::
    test_d24_public_m4_dto_and_api_signature_snapshot_is_unchanged
  tests/m5/reference/test_legacy_regression.py
  tests/m4/test_m4_contracts.py
  -> 14 passed in 0.19s

ruff check contracts.py digests.py test_contracts.py test_digests.py
  -> All checks passed

ruff format --check contracts.py digests.py test_contracts.py test_digests.py
  -> 4 files already formatted

mypy --strict --explicit-package-bases contracts.py digests.py
  -> Success: no issues found in 2 source files

compileall -q src/groundloop/m5/runtime tests/m5/runtime
  -> exit 0

git diff --check
  -> exit 0
```

The post-replay D26 delta was exactly
`src/groundloop/m5/runtime/digests.py` and
`tests/m5/runtime/test_digests.py` before this handoff was added. The final
post-replay delta must be exactly those two paths plus this handoff; the full
activation-to-final delta must be the six-path Lane A manifest. There were no
skips, xfails, deselections, pooled partial results, database calls, network
calls, providers, deployment actions, or runtime-mode changes.

## 5. Complete failure chronology

No assertion, Ruff, format, mypy, compile, digest, path, or hash check failed.
The following environment/orchestration mistakes were retained rather than
silently omitted:

1. The first independent migration-016 bundle calculation used the system
   interpreter without `PYTHONPATH=src` and stopped with
   `ModuleNotFoundError: No module named 'groundloop'`. It changed no file or
   Git state. Re-running with `PYTHONPATH=src` reproduced the complete accepted
   tuple.
2. One wrapper invocation used invalid JavaScript quoting while preparing the
   empty-oracle print and failed before its shell command started. It changed
   no file or Git state; the corrected invocation then passed.
3. The first worktree-creation invocation selected the not-yet-created
   worktree as its process working directory and failed before the shell
   started with `No such file or directory`. Re-running from the existing main
   worktree created the required branch/worktree and replayed all commits
   conflict-free.
4. The first complete-runtime pytest attempt used the system interpreter and
   stopped during collection because `psycopg` was unavailable, reporting two
   collection errors. No test ran and no source change followed. Re-running
   through `/home/kassym/Desktop/groundloop/.venv/bin/python` collected and
   passed all 420 tests.

The imported pre-edit focused D25 checkpoint also passed its carried 134-test
contract/digest suite before the D26 edit.

## 6. Protected user state

The main worktree's unrelated user-owned dirt remained outside this branch and
matched the activation ledger after implementation:

```text
pyproject.toml =
  2af4b19962dc8a7d22e377be17f342530a06ee6395bbbf2a092eab36599c8fc2
groundloop_fyp_professor_feedback.pdf =
  45c20ca46e9ad5bcd86b22c0d8882d1d611497f57ca8c45d3dea149260c110cd
groundloop_fyp_professor_feedback_v2.pdf =
  59a13cd8d4bbb017e712c0f39e70f2eba136557e945f64f1b1fc3891742a79f0
render_groundloop_fyp_professor_deck.py =
  c12929c349a5c0be9793159143b09da40ea2a0b27df37b92d61d9ed6483d8c2a
PERSISTED_MATCHING_AMENDMENT_DRAFT.md =
  167d1e7df5a720041fe0ff51879d08357f0dfbe3a7781ffaa08c0d958a47aa94
```

## 7. Boundary and next gate

This lane supplies pure D25 contracts plus D26 absence-digest bytes and tests
only. It does not implement structural reference emission, lifecycle or
predecessor validation, migration 017, PostgreSQL cross-language vectors,
store/runtime composition, reconnect without regeneration/model calls,
physical matching, sealing, activation, production history, evaluation, a
provider, deployment, or AI-quality improvement.

The candidate requires one independent same-byte final audit covering the
complete carried D25 checkpoint and D26 delta, with `GO`, `P0=0`, and `P1=0`,
before integration. This handoff does not self-audit and does not mark D25,
D26, M5.0-25, M5.0-26, M5.4, deployment, or any empirical claim `PASS`.
Runtime remains `v1_only` outside isolated fixtures.
