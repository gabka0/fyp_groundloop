# GroundLoop M4.1 Acceptance Matrix

Status: **M4.1 deterministic integration accepted; retained as a regression
contract**

Acceptance date: 2026-07-20; M4 closure update: 2026-07-21

This matrix is executable scope, not an aspirational checklist. Every required
row now has deterministic/live PostgreSQL evidence. Later M4.7--M4.11 work
strengthens the physical and real-model paths but does not redefine the M4.1
semantics. M4.1 acceptance is not M4 scientific completion.

## 1. Frozen deterministic history

Initial published state `B0` contains one registered answer, one required
claim, one active supporting chunk and one current SUPPORT observation. The
claim is `SUPPORTED`; the answer is `VALID`.

The history then runs:

1. insert a NEUTRAL chunk;
2. delete the original supporting version;
3. replace an active neutral version with a REFUTE chunk;
4. replay the exact replacement;
5. attempt a conflicting replay with the same event ID.

The same event plans run first with deterministic fake admission/verifier
ports and later with pinned real-model ports on a bounded corpus.

## 2. Structural and coordination surface

| Scenario | Required persisted result | Falsifying condition |
|---|---|---|
| Open insertion | One new epoch, immutable update, one discovery root per inserted chunk, one open global scope | Missing/duplicate root, second active epoch, partial structural rows |
| Exact event replay | Existing epoch returned; revision and rows unchanged | New epoch/job/scope or revision increment |
| Conflicting event replay | Conflict; no row changes | Existing event silently reused or overwritten |
| Start attempt | One immutable next ordinal and RUNNING job | Duplicate ordinal, execution identity drift |
| Expand discovery | Parent result, exact child closure, every child/dependency and scope close in one revision | Parent completed with missing child, append after closure, more than one revision increment |
| Complete verifier | One terminal job and at most one immutable current observation transition | Duplicate observation or partial state write |
| Retryable failure | Same logical job, new attempt ordinal, unchanged execution identity | New logical job or changed payload |
| Terminal failure | Epoch cannot seal; last published pointer remains unchanged | Failed working state becomes public |
| Late inactive completion | Artifact archived as inactive; no active observation/state/publication effect | Failed epoch resurrects or newer state is overwritten |
| Seal | No open jobs/scopes; exactly one SEALED transition | Seal while fallback/discovery/job remains open |

After every transition, the SQL projection must equal the pure runtime model,
including job states, attempts, child closure, scope closure, revision,
evaluation state and active/sealed epoch pointers.

Acceptance evidence: the runtime-store, application and PostgreSQL pipeline
tests cover structural open, child expansion, verifier completion, retry,
failure, seal and replay. M4.11 additionally runs retryable runtime failure and
the signed evaluation transition in one transaction, proves that the second
attempt does not double-count PENDING work, and checks exact/conflicting replay
with full-state readers forbidden during the measured kernel.

## 3. Grounding surface

| Event point | Required claim state | Required answer state |
|---|---|---|
| Initial `B0` | SUPPORTED | VALID |
| Neutral insertion sealed | SUPPORTED | VALID |
| Supporting deletion after required fallback closes empty | UNSUPPORTED | UNSUPPORTED |
| Neutral-to-refute replacement sealed | REFUTED | CONTRADICTED |

For every observation completion and seal:

```text
incremental complete ClaimState/AnswerState
  == Python full recomputation
  == SQL full recomputation
```

Equality includes counts, best scores, observation identifiers and answer
aggregates, not only status enums. The three paths consume the identical
stored-observation snapshot.

Acceptance evidence: deterministic integration histories cover the table
above. M4.8 repeats the INSERT/DELETE/REPLACE shape with pinned BGE and
calibrated-verifier ports, then runs Python and independent SQL audits after
every measured event with zero claim/answer mismatches.

## 4. Evaluation and publication surface

| Point | Provisional visibility | Strict visibility |
|---|---|---|
| After structural insert/replace | all claims in open discovery scope PENDING | previous sealed snapshot |
| After discovery closes with open verifier child | targeted claim and owning answer PENDING | previous sealed snapshot |
| After all jobs/scopes close | COMPLETE, seal eligible | previous sealed snapshot until seal transaction |
| After seal | COMPLETE at new epoch | new append-only published snapshot |
| After failure | FAILED working epoch | previous sealed snapshot forever |

Sealing must close prior current published-state validity intervals, append
the new complete claim/answer rows, emit one net public status delta per
changed object, advance the sealed pointer and mark the epoch SEALED in one
transaction.

Acceptance evidence: sparse working/publication integration tests preserve
nonoverlapping validity intervals and the previous publication on failure.
M4.11 checks required/optional PENDING propagation, failed-epoch preservation,
late `COMPLETED_INACTIVE` archival and sealed reconnect replay.

## 5. Failure injection

Inject an exception after every logical SQL step in:

- structural open;
- expandable completion;
- verifier observation completion;
- grounding-state installation;
- final publication/seal.

For each injected failure, reconnect or begin a new transaction and compare
the entire affected table projection with the pre-transaction snapshot. No
test may establish rollback merely by observing one row.

Acceptance evidence: `tests/m4/crash_matrix/` reconnects after every exposed
production injection point and compares complete logical base-table
projections. M4.11 independently injects failure after verifier-state
installation and verifies rollback of all 61 transactional GroundLoop tables;
the separately committed execution-accounting increment is asserted
explicitly rather than hidden.

## 6. Stage disposition at M4 closure

The implementation prerequisites for M5 have now passed:

1. **M4.1a — passed:** PostgreSQL runtime parity, full-projection rollback and
   reconnect tests.
2. **M4.1b — passed:** persistence-neutral deterministic application and
   composed PostgreSQL ports.
3. **M4.1c — passed:** live deterministic insert/delete/replace publication
   plus CLI.
4. **M4.2i — passed:** persisted exact withdrawal and mandatory exact fresh
   frontier fallback integrated into the application.
5. **M4.3i — passed:** frozen claim registry, PostgreSQL lexical/vector
   admission and persisted hit/pair provenance.
6. **M4.4i — passed:** executable exhaustive audit/refresh persistence,
   content-validated replay and deliberate-miss detection.
7. **M4.5/M4.8 — passed as an implementation gate:** pinned real-model
   insert/delete/replace with exact reconnect replay and post-kernel oracles.
8. **M4.6/M4.9 controlled — passed as evaluation infrastructure:** all seven
   ablations, same-event checks, misses/timeouts and deterministic outputs.
9. **M4.10 — executed, bounded negative pilot:** naturally versioned real-
   history execution with pinned models, persisted audit identities and actual
   telemetry. Non-exhaustive policies recovered `1/4` model-relative positive
   pairs and `0/1` answer-status effects; three histories without independent
   adjudication cannot support a population claim.
10. **M4.11 — passed as physical regression evidence:** adversarial histories
    at two unrelated-state scales with full paths forbidden in the successful
    measured kernel and audited afterwards.
11. **M4.12 — executed AI diagnostic:** the frozen M3 verifier was weak on
    fine-grained public revision pairs, motivating one bounded adaptation.
12. **M4.13 — executed `NO_GO`:** the change-aware experiment improved the
    revision diagnostic but failed preregistered retention gate G8. No adapted
    checkpoint is promoted; frozen M3 remains the default.

The optional learned impact retriever M4.N may follow the real-history study or
remain future work; it does not block M4 CORE. Any newly discovered correctness
dependency becomes another explicit M4.x gate rather than being deferred
silently to M5. M4 is now closed and M5 is unblocked but not started. The
M4.10 pilot closes the executable path only; a larger independently
adjudicated natural-history evaluation remains pre-dissertation/M6 debt and is
required before strong selective-maintenance or end-to-end utility claims.
