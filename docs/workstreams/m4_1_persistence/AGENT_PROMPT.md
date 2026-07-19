# M4.1 PostgreSQL Runtime Lane Prompt

Branch: `workstream/m4_1-persistence`

Exclusive ownership:

- `src/groundloop/m4/persistence.py`
- `tests/m4/persistence/**`
- `docs/workstreams/m4_1_persistence/**`

Implement a typed PostgreSQL mirror of the frozen pure M4 epoch/runtime model
over migrations 003 and 004. Candidate policy registration must be
content-validated. Epoch opening, attempts, retryable failure, atomic
expandable completion, verifier completion, epoch failure, strict sealing,
exact replay, conflicts and revision CAS must have the same projection as the
pure runtime model.

Every live test uses a unique temporary schema. Inject exceptions after child
insertion and after publication-head advancement to prove whole-transaction
rollback. Sealing must compose with a coordinator-supplied publication action
in the same transaction; this lane may not publish grounding state itself or
write pending observations into M2 currency.

Forbidden work includes admission, models, observations, claim/answer
derivation, migrations, contracts, CLI and application orchestration.
