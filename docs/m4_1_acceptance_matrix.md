# GroundLoop M4.1 Acceptance Matrix

Status: coordinator integration gate

Date: 2026-07-19

This matrix is executable scope, not an aspirational checklist. M4.1 is not
accepted until every required row has a deterministic test and every
PostgreSQL row runs against a unique live schema.

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

## 6. M4 completion stages before M5

M5 evidence groups cannot begin until these stages pass:

1. **M4.1a:** PostgreSQL runtime parity and rollback.
2. **M4.1b:** persistence-neutral deterministic application.
3. **M4.1c:** live deterministic insert/delete/replace publication plus CLI.
4. **M4.2i:** persisted exact withdrawal and frontier fallback integrated into
   the application, not only unit-tested.
5. **M4.3i:** production claim registry, lexical and vector admission
   integrated with persisted hits/pairs.
6. **M4.4i:** executable exhaustive audit/refresh runner integrated with event
   records and deliberate-miss detection.
7. **M4.5:** pinned real-model dynamic smoke with exact replay.
8. **M4.6:** controlled/real-history evaluation, ablations, uncertainty and
   one-command reproduction.

The optional learned impact retriever M4.N may follow M4.6 or remain future
work; it does not block M4 CORE. Any newly discovered correctness dependency
becomes another explicit M4.x gate rather than being deferred silently to M5.
