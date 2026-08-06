# GroundLoop M5 Implementation Status

Status date: 2026-08-06

Milestone status: **M5.0 through M5.3 complete; M5.4 partially complete.**
M5.4-01 passes. M5.4-02 through M5.4-09 and every M5.5--M5.6
implementation/evaluation closure remain pending.

## 1. Honest current verdict

M5 is no longer an informal "AND over requirement counts" extension. The
frozen contract defines exact bounded bipartite-matching semantics, a
stateful Hall-mask delta operator, independent Python and SQL oracles,
versioned certificate artifacts, typed runtime identities, a PostgreSQL
upgrade/activation boundary, and a retrospective controlled WiCE evaluation.

Accepted bounded evidence now covers the pure reference, optimized matching and
incremental overlay, migration 014/PostgreSQL integrity, the independent SQL
oracle, a 100,000-event in-memory differential, and a snapshot-per-prefix
three-oracle history, durable failure/replay coordination, byte-total v2
runtime contracts, and public activation/bootstrap. There is still no accepted
M5 evidence for dynamic requirement execution, typed document composition,
sparse publication, sealed reconnect replay, maintained-runtime model quality,
production latency, call savings, utility, novelty, or publishing potential. Those claims remain
blocked by the executable gates in `docs/m5_acceptance_matrix.md`.

## 2. M5.0 evidence

The pre-M5 implementation baseline was revalidated before the freeze:

```text
PostgreSQL 16.14 / pgvector 0.8.5 validator: passed
full pytest: 680 passed, 7 intentional opt-in skips
Ruff, strict mypy, compileall, git diff check: passed
```

Three independent `gpt-5.6-sol` ultra audits examined theory, pure-reference
and runtime semantics, and PostgreSQL/evaluation integrity. The first audit
round returned NO-GO and forced corrections including:

- Hall matching instead of global witness-union cardinality;
- every requirement/hash edge crossing as a material update;
- historical revision currency and close-once certificate bindings;
- an explicit policy-range-probe term in M5-T2;
- failure-safe staged group structure and activation serialization;
- a bounded SQL assignment cross-check with an explicit cap result;
- an exact cross-language 29-code-point normalizer;
- byte-total forward/reverse scope shapes and controlled projections;
- REQUIREMENT-only WiCE projections with no direct-claim bypass; and
- source/ablation metrics that share the same independent direct-support
  disjunct.

After correction, all three auditors returned GO with high confidence against
the same semantic-content hashes:

```text
m5_design_freeze.md          db88cad47f33710dfb3cc07a501710d9e14c312a8966e06f373bd70461af852d
m5_implementation_plan.md    319fc28a59df65bc3009f34bcca966bbffbe3659e4411335c9df2218ba427677
m5_acceptance_matrix.md       7861cf4146f190843560e1cdf912a9f862372fc891a4851d72b56251bfac6a71
m5_multiagent_execution_plan faffae5f839623efd9e387f63e951881cf1f46c5b66f37c3db642cc416a9e20a
```

Those hashes identify the final audited semantic candidate. Subsequent edits
have changed status and executable-evidence reporting without changing the
frozen semantic contract. Final implementation artifact hashes and the
decision-row cross-stage mapping will be recorded at M5.6.

## 3. Frozen M5.0 result

The authoritative documents are:

- `docs/m5_design_freeze.md` -- decisions M5-D1 through M5-D22 and theorems
  M5-T1/M5-T2;
- `docs/m5_implementation_plan.md` -- stages M5.1 through M5.6;
- `docs/m5_multiagent_execution_plan.md` -- path-exclusive ownership and
  integration order; and
- `docs/m5_acceptance_matrix.md` -- falsifiers and executable evidence gates.

The strongest proposed exact statement is conditional: after immutable group,
requirement, chunk, observation-currency and policy inputs are fixed, the M5
incremental state must equal independent Python and SQL recomputation at every
serialized committed semantic revision. Neural retrieval and verification
remain empirical.

The proposed logical affected-group bound is parameterized by the number of
policy probes, changed observations, coalesced hash-mask transitions,
certificate repairs/rebuilds, touched states and output bytes, with
`r <= 8`. It is not a claim of general dynamic-matching novelty or superiority
over DBSP, F-IVM, CROWN, or another system.

## 4. M5.1--M5.3 result and partial M5.4

M5.1 implements the pure semantic foundation:

1. typed digest and normalization primitives;
2. immutable family/group/requirement records and lifecycle history;
3. typed requirement observations and historical currency;
4. independent unmatched-branch Python recomputation;
5. register/replace/retire/observe events with exact replay and rollback; and
6. frozen M4 v1 regression vectors.

An independent adversarial audit returned GO with high confidence after 73
focused tests, an exact 74,958-graph oracle enumeration, a successful full
repository suite, Ruff, and strict mypy. It specifically confirmed global
event/observation identifier collision safety, lazy epoch synchronization,
failure-atomic currency rejection, typed prevalidation, immutable records, and
alternative valid certificate acceptance.

Those were M5.1-only boundaries. The later integrated work closes them only at
the following bounded stages:

- M5.2 passes all nine incremental-algorithm gates. Exact bounded matching,
  Hall-mask and multiplicity suites, sparse/unrelated-key assertions,
  certificate repair/rebuild/history, failure-atomic injection, replay, and
  every-term complexity guards pass. The frozen seed-`20260802` run records
  100,000 committed events, 7,731 exact replays, 20,726 generator rejections,
  1,000 audits of each maintained index/history class, and zero mismatches.
  Its ignored artifact was revalidated against the integrated runner, config,
  and manifest rather than regenerated.
- M5.3-01 through M5.3-05 and M5.3-08 through M5.3-09 pass after sequential
  integration of migration 014, the PostgreSQL repository and SQL oracle. The
  owned live PostgreSQL suite passed 55 tests; 247 relevant live M4/pre-M5 tests
  passed with one explicit real-model skip.
- M5.3-06 passes at integrated executable commit `e266696`. One deterministic
  16-checkpoint history covers all nine overlay event variants and exact
  replay, with incremental, Python, and SQL state/certificate checks agreeing.
  The complete live M5 suite passed 339 tests with only the explicit 100k gate
  skipped, and the ordinary full suite passed 816 tests with 211 classified
  skips. See
  `docs/workstreams/m5_integration/THREE_ORACLE_RESULT_2026-08-05.md`.

M5.3-06 loads fresh rollback-isolated PostgreSQL rows for each logical prefix
and materializes derived state with `publish=False`. It is not a durable
same-schema mutation history. M5.3-07 now separately passes through the
production persistence slice: 27 adversarial live tests cover every typed
group open/failure injection point, REGISTER/REPLACE/RETIRE staged failure,
exact cancellation/PENDING accounting, strict/published immutability,
read-only reconnect replay, conflict rejection, and concurrent exact open.
The composed migration/failure/bundle-race gate passed 67/67 tests.

M5.4-01 passes through the integrated byte-total contract, digest, direction,
shape, nullability, F64, ordering, and pure-frontier suite. M5-D22 closes the
previously undefined inner state-artifact identity without changing M4-v1 or
M5 semantic state. Public activation is implemented and passes 8/8 live tests,
including all six bootstrap reference kinds, cross-language SQL/Python hashes,
no synthetic epoch, read-only replay, conflicts, and six failure-atomic
injection points. Activation alone does not close M5.4-05.

## 5. Remaining closure boundary

M5 completes only after M5.1--M5.6 pass. Closure still requires M5.4 dynamic
jobs, typed direct composition, sparse publication, reconnect and exactly-once
runtime gates; a real
maintained-runtime M5.5 controlled WiCE execution; and M5.6 reproduction,
artifact, documentation, and final acceptance audits.

M5 is not complete. Even after technical closure, without a fresh blinded,
independently adjudicated cohort the strongest permitted semantic conclusion
will remain **implementation complete with controlled/retrospective semantic
evidence only**. A representative utility claim remains M6 debt.
