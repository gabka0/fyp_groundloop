# M4.1 Deterministic Application Lane Status

Status: complete for lane-owned Wave A scope

Commit: `7f31e171bb511a9b2ecb10dfb30c6b785c866cda`

## Implemented

- Typed, persistence-neutral application ports and deterministic event plan.
- Insert/delete/replacement validation and exact-withdrawal ordering.
- One impact-discovery root and open global discovery scope per inserted
  chunk.
- Mandatory frontier roots returned by exact withdrawal; unsatisfied fallback
  blocks sealing.
- External admission followed by atomic complete-child-set declaration.
- Event-wide admitted-pair deduplication before verifier job creation.
- External verification with immutable result identity validation.
- One atomic observation/job-completion port operation: both active and
  inactive artifacts are auditable, only active results enter the working
  effective-currency overlay, and no result changes published currency before
  seal.
- Strict readiness check, all three equality gates and a single publication
  request.
- Expected external-worker failures preserve the previous publication.

## Test evidence

The 14 lane tests cover:

- supporting, neutral and refuting insertion;
- exact deletion with zero impact-discovery retrieval;
- support-to-neutral and support-to-refute replacement;
- empty discovery;
- exact and conflicting replay;
- failure before expansion, after expansion and at atomic
  observation/completion;
- late inactive artifact archival without effective or published mutation;
- unsatisfied fallback blocking;
- event-wide duplicate-pair suppression.

Validation at the implementation commit plus the documentation-only change:

```text
pytest application + runtime + shared M4 contracts: 109 passed
full repository pytest: 331 passed, 24 skipped
ruff: passed
mypy --strict application.py: passed
compileall lane paths: passed
git diff --check: passed
```

The 24 full-suite skips are environment-gated PostgreSQL/model tests; this lane
does not claim to have exercised them.
