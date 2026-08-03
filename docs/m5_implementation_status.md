# GroundLoop M5 Implementation Status

Status date: 2026-08-03

Milestone status: **M5.0 and M5.1 complete; M5.2 integration active.**
No M5.2--M5.6 implementation or evaluation gate is reported complete yet.

## 1. Honest current verdict

M5 is no longer an informal "AND over requirement counts" extension. The
frozen contract defines exact bounded bipartite-matching semantics, a
stateful Hall-mask delta operator, independent Python and SQL oracles,
versioned certificate artifacts, typed runtime identities, a PostgreSQL
upgrade/activation boundary, and a retrospective controlled WiCE evaluation.

M5.1 now supplies executable pure-reference evidence. There is still no
accepted M5 evidence for the optimized matching operator, PostgreSQL integrity,
dynamic requirement jobs, model quality, latency, call savings, or publishing
potential. Those claims remain blocked by the executable gates in
`docs/m5_acceptance_matrix.md`.

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
only changed candidate/freeze status labels and the M5.0 contract cells from
`PENDING` to `PASS / PENDING`; final implementation hashes will be recorded at
M5.6.

## 3. Frozen M5.0 result

The authoritative documents are:

- `docs/m5_design_freeze.md` -- decisions M5-D1 through M5-D20 and theorems
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

## 4. M5.1 result and active next stage

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

Two boundaries remain deliberately open. M5.1 cannot validate historical
combined claim-certificate bindings because the preserved direct M1 oracle has
only current state and M5.1 does not persist claim bindings. That is mandatory
M5.2/M5.3 work. Its in-memory currency interval is a total-order reference
abstraction; migration 014 must implement exact epoch-local working rows and
fallback semantics. Neither full M5-D9/D12 persistence nor SQL equivalence is
claimed here.

M5.2 integration is active. The first matching-kernel audit found its core
mathematics credible but rejected integration until cross-epoch certificate
carry-forward, bounded measured indexes, uncharged sorting, shared-type
adapters, and exhaustive certificate evidence are fixed. M5.3 schema/oracle
implementation may proceed in a disjoint worktree against the published M5.1
contracts. M5.4 remains blocked on the audited runtime addendum and M5.2/M5.3.

## 5. Remaining closure boundary

M5 completes only after M5.1--M5.6 pass. In particular, closure still requires
the 100,000-event differential gate, exhaustive bounded matching tests, live
013-to-014 PostgreSQL upgrade and three-oracle histories, dynamic requirement
jobs and replay, the pinned controlled WiCE study, and final full validation.

Without a fresh blinded independently adjudicated cohort, the strongest M5
semantic conclusion must remain: **implementation complete with
controlled/retrospective semantic evidence only**. A representative utility
claim remains M6 debt.
