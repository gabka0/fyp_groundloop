# M5.1 Pure Reference Handoff

Status: accepted

Date: 2026-08-03

Branch/base: `main` at M5.0 commit `d78c82b`

Owned implementation:

- `src/groundloop/m5/digests.py`
- `src/groundloop/m5/domain.py`
- `src/groundloop/m5/repository.py`
- `src/groundloop/m5/reference.py`
- `src/groundloop/m5/events.py`
- `src/groundloop/m5/__init__.py`
- additive global observation-ID reservation in
  `src/groundloop/repository.py`
- `tests/m5/reference/`

Accepted behavior:

1. M5 records are immutable, typed, hash-bound and limited to one through
   eight densely ordered requirements.
2. Requirement observations share the global observation-ID namespace with
   direct claim observations and are prevalidated before any mutation.
3. Structural and observation events are copy-apply-commit atomic, exact-
   replay idempotent and payload-conflict detecting.
4. The independent oracle derives active witness edges, maximum matching,
   completeness, direct/group claim composition, answer state and certificate
   validity without importing the future Hall-mask kernel.
5. Direct M1 digest vectors and the never-activated behavior remain unchanged.

Validation evidence:

```text
focused reference suite: 73 passed
finite oracle domain: 74,958 graphs through r<=4,H<=4
full repository pytest: passed to 100%, exit 0
Ruff --no-cache: passed
mypy --strict --no-incremental: passed
independent adversarial audit: GO, high confidence
```

Open mandatory boundaries:

- Historical combined claim-certificate bindings are not available from the
  current-only direct M1 oracle. M5.2/M5.3 must persist and validate them at
  exact snapshot points.
- `ObservationCurrencyInterval` is the pure total-order reference model.
  Migration 014 must implement the frozen epoch-local working currency rows,
  close-once intervals and prior-epoch fallback.
- The legacy alias bridge exists only for M5.1 reference composition. M5.4
  activation replaces it with the typed dispatcher and atomic mixed runtime.
- No optimized IVM, SQL equivalence, neural-runtime, latency or model-quality
  result is claimed by this handoff.

Forbidden user-owned files were not modified or staged by M5.1:
`pyproject.toml` and `docs/presentations/`.

Integration order: commit M5.1 shared contracts first; rebase the matching
lane onto that commit; integrate a re-audited M5.2 kernel; merge migration 014
and the independent SQL oracle only after their live upgrade gates pass.
