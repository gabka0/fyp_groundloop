# Contract request: per-step structural-open failure injection

Severity: P1 acceptance-evidence gap

## Minimal failing behavior

Construct `PostgresM4ApplicationPorts` with a recording `failure_injector`,
call `open_event`, and inspect the recorded point names. The pipeline emits no
structural-open point. `PostgresM4RuntimeStore.open_epoch` emits only
`open_structural_written` after the whole callback and `open_rows_written`
after job/scope declaration.

The M4.1 acceptance matrix requires failure injection after every logical SQL
step in structural open. The current hooks prove rollback only at two coarse
boundaries; they cannot deliberately crash between claim-registry insertion,
version/chunk writes, withdrawal overlay, working-state installation and
Surface-C initialization.

Executable witness:

```bash
pytest -q \
  tests/m4/crash_matrix/test_atomicity_matrix.py::\
test_pipeline_structural_open_exposes_required_per_step_hooks
```

It is a strict xfail until the contract exists.

## Proposed non-semantic interface

Inside the existing `structural_action` transaction, call the already injected
failure callback after these groups:

```text
structural_registry_written
structural_versions_written
structural_withdrawal_written
structural_working_states_written
structural_evaluation_written
```

No new public DTO is required. The callback is diagnostic only and must never
commit, catch the injected exception, or change event identity.

## Compatibility

This is backward compatible for callers without a failure injector. Existing
injectors receive additional names and must ignore names they do not target,
which is already the current callback convention.

## Acceptance

1. Remove the strict xfail marker from the executable witness.
2. Add all five names to the structural crash parameterization.
3. For every point and execution mode, snapshot all schema base tables before
   the open transaction, crash, reconnect, and require exact equality.
