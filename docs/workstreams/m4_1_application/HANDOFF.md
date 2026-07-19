# M4.1 Deterministic Application Lane Handoff

## Verdict

The lane supplies the application ordering contract needed by coordinator
integration. It is not a database adapter or an end-to-end pipeline.

## Interfaces the coordinator must adapt

`M4Application` expects seven injected boundaries:

1. `StructuralMutationPort` performs exact withdrawal and atomically opens the
   event with roots and discovery scopes.
2. `RuntimeTransitionPort` performs leases, expandable completion, failure,
   readiness and persisted child lookup.
3. `AdmissionPort` returns persisted, replay-safe admission results.
4. `VerificationPort` returns immutable verifier artifacts outside storage
   transactions.
5. `ObservationApplicationPort.complete_verifier_atomically` archives the
   observation and terminalizes its job in one transaction. Active results
   also update the per-epoch working overlay; inactive results never do.
6. `EqualityGatePort` checks grounding, coordination and evaluation surfaces.
7. `PublicationPort` alone promotes the working overlay and seals the epoch.

The persistence adapter must not implement item 5 as two commits. An artifact
may be staged before completion, but it must not become effective while the
logical verifier job remains nonterminal.

## Preserved invariants

- Withdrawal performs only reverse-edge enumeration. Application code invokes
  no vector, lexical or ANN retrieval for deactivation.
- An impact root exists for every inserted chunk and its scope covers the
  immutable claim-registry snapshot.
- Expandable completion declares the exact sorted child set once, including
  an explicit empty closure.
- One `(claim, chunk)` pair produces at most one verifier child event-wide.
- Admission/verifier work occurs outside the structural transaction.
- Published state is unchanged by every injected failure prior to seal.
- Three equality gates execute only after the runtime reports no open job,
  open discovery scope or blocked fallback.
- Late inactive verifier output is stored for audit, never made effective and
  never resurrects a failed epoch.

## Integration limitations

- Port implementations are deliberately absent. Coordinator integration must
  bind them to the M4.1 PostgreSQL runtime and working-overlay schema.
- This lane does not provide crash recovery for an already-running external
  lease; the persistence/runtime adapter decides retry and lease-expiry policy.
- It performs no real vector/lexical retrieval or verifier inference.
- It does not run Python/SQL equality itself; it orders injected exact gates.
- It emits a seal request but does not define public delta rows or CLI output.

## Revalidation commands

```bash
.venv/bin/pytest -q -o addopts='' \
  tests/m4/application tests/m4/runtime tests/m4/test_m4_contracts.py
.venv/bin/ruff check src/groundloop/m4/application.py tests/m4/application
.venv/bin/mypy --strict src/groundloop/m4/application.py
.venv/bin/python -m compileall -q \
  src/groundloop/m4/application.py tests/m4/application
git diff --check main...HEAD
```
