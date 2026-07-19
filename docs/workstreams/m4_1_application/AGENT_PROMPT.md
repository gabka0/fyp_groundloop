# M4.1 Deterministic Application Lane Prompt

Branch: `workstream/m4_1-application`

Worktree: `/home/kassym/Desktop/groundloop-worktrees/m4_1-application`

Exclusive ownership:

- `src/groundloop/m4/application.py`
- `tests/m4/application/**`
- `docs/workstreams/m4_1_application/**`

Implement the persistence-neutral deterministic M4 application coordinator.
Use injected ports for exact structural withdrawal/mutation, runtime
transitions, admission, verification, atomic observation/job completion,
three-surface equality gates and publication. Do not issue SQL or load models.

Required histories are support, neutral and refute insertions; deletion;
support-to-neutral and support-to-refute replacement; empty discovery; exact
and conflicting replay; failures before and after expansion; failure at the
observation/completion boundary; fallback blocking; and late inactive result
archival.

The observation boundary is non-negotiable: verifier output is an immutable
audit artifact. Active output may become effective only in the per-epoch
working overlay, and that effective update must be atomic with verifier-job
terminalization. Inactive output remains auditable but inert. Published M2
observation currency changes only through atomic sealing.

Forbidden paths include shared contracts, runtime/admission/oracle modules,
persistence, migrations, CLI, pipeline and foreign documentation. Record a
lane-local contract request instead of changing a shared contract.
