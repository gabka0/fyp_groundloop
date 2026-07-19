# M4.5 Model Application-Port Handoff

Status: implementation complete; coordinator merge pending

Code commit: `5c0fea8`

## Delivered

- Canonical persistence-neutral `PairVerificationInputResolver` protocol.
- `M4VerificationApplicationPort`, implementing the M4 application verifier
  boundary over the pinned M3/M4 calibrated adapter.
- Full logical-job, pair, resolver, execution-spec, model artifact, prompt,
  calibration, and immutable-input drift rejection.
- Domain-separated result payload hashing over every persisted verification
  artifact field.
- Read-only content-addressed access to the full exact pair artifact for the
  coordinator-owned persistence boundary.
- Replay behavior that re-resolves canonical inputs while avoiding repeated
  neural inference.
- `M4AdmissionEmbeddingService` with sorted role artifacts and vectors ready
  for admission indexes.
- Download-free fake tests plus a passing opt-in pinned local-artifact port
  smoke.

## Validation

```bash
.venv/bin/pytest -o addopts='' -q tests/m4/models tests/m4/application
# 36 passed, 2 skipped

GROUNDLOOP_RUN_M4_REAL_MODEL_SMOKE=1 \
  .venv/bin/pytest -o addopts='' -q \
  tests/m4/models/test_application_ports.py::test_opt_in_pinned_local_artifact_application_port_smoke
# 1 passed

.venv/bin/ruff check src/groundloop/m4/models tests/m4/models
# All checks passed

.venv/bin/mypy --strict src/groundloop/m4/models
# Success: no issues found in 6 source files

.venv/bin/python -m compileall -q \
  src/groundloop/m4/models tests/m4/models
# exit 0
```

## Integration assumptions

1. The coordinator supplies a resolver backed by canonical immutable claim and
   chunk registries; this lane does not issue SQL.
2. The application's verifier execution-spec hash must be set from
   `bundle.verifier.spec.execution_spec_hash`, not reconstructed manually.
3. The returned application result and underlying pair artifact must be
   persisted with job completion and observation activation in the existing
   all-or-nothing completion boundary.
4. Durable cross-process replay must validate stored content. Process-local
   counters and caches are observability/optimization mechanisms, not the
   durable authority.
5. The coordinator may re-export the new classes from
   `groundloop.m4.models.__init__`; that shared existing file was deliberately
   not edited in this lane.

## Limits

- No database resolver implementation is included.
- No epoch, admission, observation, or publication orchestration is included.
- No model was downloaded or trained.
- The real smoke validates wiring and artifact identity, not neural quality.
