# M5-D33 / H33 Migration-021 Exact-Byte Hash Freeze

Date: 2026-10-12

Status: docs-only candidate. These literal pins become authority ONLY after two
independent complete identical-byte GO/P0=0/P1=0 reviews, sole-path commit,
two fresh postcommit identity checks and atomic branch/main push. This document
does not accept S33, its installer, runtime composition or deployment.

## 1. Authority and exact sole ownership

Authority is AGENTS.md, frozen M5-D24--D33, runtime-addendum revision14 and the
acceptance matrix. The original implementation activation and accepted order
correction authorize this separately owned literal-pin tranche:

```text
worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d33-schema-021-hash-freeze
branch = workstream/m5-d33-schema-021-hash-freeze
required_parent = 68eb4bcf46c21ee369e034481fe6b50ca6d22cc4
required_parent_tree = d32cb713b88aa3055993a44b8e778da17b0c91f5
required_origin_main = 68eb4bcf46c21ee369e034481fe6b50ca6d22cc4
sole_owned_path = docs/workstreams/m5_runtime_implementation/D33_SCHEMA_021_HASH_FREEZE.md
original_activation_sha256 = 320170693177b6e939b98ab42382d96b0dd1e55271d123d9aa97ee505ddf591b
order_correction_sha256 = 0ddfe719262e88c48ea6ae7bad288bbb2d6ac75a33a969a8f62f585ca7d32199
frozen_D33_sha256 = 055ef0d6f971a23be31cc23d2b9f03b4b7cd17fa9f9bc7c505d1f565d6726b32
sql_evidence_worktree = /home/kassym/Desktop/groundloop-worktrees/m5-d33-schema-021
sql_evidence_head = 68eb4bcf46c21ee369e034481fe6b50ca6d22cc4
```

The referenced authorities are respectively
D33_DOCUMENT_COMPLETION_IMPLEMENTATION_ACTIVATION.md,
D33_READINESS_PROMOTION_ORDER_ACTIVATION.md and
../m5_runtime_contract/DOCUMENT_UPDATE_COMPLETION_AMENDMENT.md.
Root owns ONLY this document while H33 is active. Both independent auditors own
zero edit paths and perform no DB/provider actions. All SQL/source/tests and
other docs remain read-only; no S33 WIP is copied, staged or accepted here.

## 2. Exact migration and five-field identity

```text
migration_label = migrations/021_m5_document_update_completion.sql
migration_sha256 = 198646f9a4b2f965d9ca89df684eb4f4287f3753aeff1d255c848bcb0352681c
migration_line_count = 4670
migration_byte_count = 274945
bundle_id = m5-document-update-completion-schema-bundle-v1
bundle_sha256 = 893baf9d50cbb84c27b356cbc329f97bc714ca8c280778ece920b5a4dc728eb5
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = b7706feb7d54fcf9fdb4f9f38350a32967e0b460229b6f264493fb428be8ddfc
```

Use only the unchanged additive bundle framing:

```text
stable_m5_digest(
  "m5-document-update-completion-schema-bundle-v1",
  TEXT("migrations/021_m5_document_update_completion.sql"),
  HASH(migration_sha256), HASH(prerequisite_sha256)
)
```

The exact prerequisite020 five-field row remains:

```text
bundle_id = m5-semantic-readiness-schema-bundle-v1
bundle_sha256 = b7706feb7d54fcf9fdb4f9f38350a32967e0b460229b6f264493fb428be8ddfc
migration_sha256 = df0a3c0c9b228a4a22903479896326d27fbd6f98f5878e34d73182ba007bf837
oracle_sha256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
prerequisite_sha256 = e12d4abd95a9b2ef49010a43d6b708824462a80dfed2548107e175920dabe481
```

Describing arbitrary requested bytes does not authorize them. A first install
requires these reviewed literal identities, all transitive000--019 exact source/
ledger authority and the exact pre021 catalog. Changed SQL, including comments,
requires a NEW separately audited H33 correction before production acceptance;
neither a mutable checkout digest nor a caller parameter overrides this pin.

## 3. Exact pre/post catalog and permitted delta

The only statements, in order, use marker
`-- groundloop:m5-document-completion-statement:`:

1. predecessor_function: CREATE OR REPLACE
   groundloop_m5_validate_semantic_readiness_predecessor();
2. work_validator: CREATE OR REPLACE
   groundloop_m5_validate_work_contribution();
3. result_children: CREATE OR REPLACE
   groundloop_m5_validate_event_result_children().

Only these three existing prosrc bodies change. Signatures, return types,
languages, owner/ACL, invoker/security/path flags, trigger identities/attachments,
CHECKs, deferrability and the entire remaining catalog stay exact. No fourth
function, new table/type/column/index/view/sequence/trigger/CHECK/privilege or
backfill is authorized. All migration000--020 bytes and ledger fields remain
unchanged. The old strict020 checker MUST still reject021's changed bodies.

Exact SHA256 of UTF8 prosrc bytes, without trimming or rewriting:

```text
groundloop_m5_validate_semantic_readiness_predecessor()
  pre = 15cedb39754ba55eaf9077cdb91b8bac9a414a4a0e6a15167c63b94f480722d4
  post = 52b16a02d69fbaa85e7f2b5cd359afaa5ddb1a55f7e897bebe05048ae22263fb
groundloop_m5_validate_work_contribution()
  pre = 6a34bb7532ace4bac94ebc88b2150788682f5d55793f913cecef77825cc0c3ae
  post = 2883d1db47dda61b8ef6c2ae88567319ece8c936355deac20e67fe4e22000f08
groundloop_m5_validate_event_result_children()
  pre = 2781f0561a310c98080e30cac2b3a7f691a39d5c561018b9eb4ffb93faa3fe73
  post = 067b2b5a3d75a413903e2d0e417eaafdb3da0e69362a66d116253e15465349ea

pre_structural_catalog = 27a02a8910e1a4b671b879cea1bdc0a5a35d9f693a870183355088990e207f62
post_structural_catalog = 27a02a8910e1a4b671b879cea1bdc0a5a35d9f693a870183355088990e207f62
function_count = 172
pre_function_sources = 95de1ba328b2f3de1cf99f3810d7f992e2bf9d9d01b4b08c85940efec2e65cfa
post_function_sources = 8e8cfe6cbcf544d0e843bfa8189cb229573fa2b0cc597f84ff0a6d39139fb059
```

The structural identity uses the unchanged020 schema-normalized catalog
fingerprint and preserved owner/ACL/signature/path/trigger checks. Function-
source identity is a schema-admin catalog receipt, NOT a new runtime/artifact
digest recipe: initialize SHA256 with UTF8
`m5-document-completion-function-sources-v1` followed by NUL. Select every
permanent installation-schema pg_proc row whose proname LIKE 'groundloop_%',
ordered by proname COLLATE "C". There are172 unique names/no overloads. For each
row append name then prosrc, each framed by its UTF8 byte length as unsigned
big-endian8 followed by exact UTF8 bytes. Hash the resulting stream. This covers
all172 bodies, not only the167 parsed AS-dollar bodies. No JSON/repr hashing is
introduced. Exact post021 source and structural catalogs are required on EVERY
accepted installer rerun/require check; three body hashes alone are insufficient.

The opt-in installer retains one idle read-write READ COMMITTED outer
transaction, captured permanent schema, ledger-first lookup, exact prerequisite
five fields, fixed lock order and ledger reread. Before the first replacement
require exact020/pre021 catalog; replace only these three bodies; insert021
ledger LAST atomically. Replay requires original caller-byte identity and exact
post021 catalog. Conflict, partial/tampered catalog, wrong schema/prerequisite,
lookup/temp shadows and different-byte first installs fail closed. Rollback cuts
and both installer race orders remain mandatory S33 gates. No public fallback,
default migration glob, runtime-mode or application API change is authorized.

## 4. Review and diagnostic evidence ceiling

Both independent fresh complete SQL-only reviews returned GO/P0=0/P1=0/P2=0
on precisely the198646 migration above. Earlier467922 HOLDs identified missing
canonical withdrawn-source validation and an unkeyed closed-binding-history
search. The2d98 draft fixed both but received A's inserted-source canonical-shape
HOLD despite B's SQL-only GO; it was NOT accepted. All three source copies now
match the accepted canonical rules, including optional provenance/input length
semantics. Every validated affected group shape drives indexed binding lookups;
the independent final-loss/binding/reference bijection remains intact.

S33's owned isolated PostgreSQL16.14 raw catalog probe on the pinned bytes
confirmed the complete catalog/ledger preservation and exact identities above.
Final-byte targeted diagnostics include coherent REPLACE row-plus-manifest
source corruptions and fully coherent omission of all11 affected families
against accepted preparation's actual source-derived projection; both pass.
The canonical REPLACE fixture intentionally reaches required direct-root
rejection, NOT fake job-free readiness. The weaker child-to-shape falsifier is
retained separately. SQL-boundary positives use genuine D25 working values and
all guards/journals/constraints; controlled SQL publication is not a production
helper/J33/C1 success. Existing-helper double-journal rejection remains explicitly
diagnostic until B33's separately activated conversion.

All failed diagnostics/regex/guard-layer/outer-xmin fixture corrections and
overlapping runs are retained in S33's handoff and external receipts. No failing
gate is erased, skipped, xfailed or counted as acceptance. A complete45-case
raw suite was started before S33 paused for H33; its result and ALL remaining
S33 installer/source/interval/as-of-K/restoration/compatibility/pure/static/
package and final four-path acceptance gates remain PENDING at this freeze.
H33 does not depend on declaring that suite or S33 complete and waives none
of those gates. The prior authorized raw test process may finish read-only
with respect to frozen source; H33 adds no DB/provider operation.

## 5. Custody and permitted S33 advance

Before H33 creation root verified every protected primary/AI/R32/R-T HEAD,
empty index and raw-v2-z status, all nine held C1 content pins from the current
D33 freeze handoff, all four held B33 pins and S33's NEW four-path pause pin.
No held worktree advanced. S33 remains HEAD68eb4bc, empty index, exactly three
owned untracked files and an unchanged migration module. Its new pause pins:

```text
s33_raw_v2_z_sha256 = 4529238c7947f216c73d766a41818e660367ea4d158af887ce3d7bc668dd415b
migrations/021_m5_document_update_completion.sql = 198646f9a4b2f965d9ca89df684eb4f4287f3753aeff1d255c848bcb0352681c
tests/m5/postgres_runtime/test_migration_021.py = 307837635f7ec8f58656f21c8f0dd1e851bda7a2cf94f293cf973a0c8d550b60
docs/workstreams/m5_runtime_implementation/D33_SCHEMA_021_HANDOFF.md = df7a4fb4462aec3ed1aa676224cf61cd022b2f6a687ff9b639e6a40fbb4e3d4e
src/groundloop/postgres/migrations.py = 6394a3700ba4b2dc9f925d841ddd070ff091da470438b6f986c752004c4f7b81
```

These replace only S33's earlier mutable diagnostic checkpoint, not any protected
or held B33/C1 pin. Full custody checks and manifests are retained externally at
/tmp/groundloop-d33-schema-resume-audit.Jromux/; durable protected/held authority
remains the accepted correction Section6 and D33_CONTRACT_FREEZE_HANDOFF.md
Section4, not a temporary file alone.

ONLY after H33's two complete audits, exact sole-parent/path commit, both fresh
postcommit GOs and atomic push/live-ref verification may root resume S33.
Before its advance: reverify ALL protected/held custody and these complete S33
pins, ancestry, and the ENTIRE upstream changed-path set disjoint from ALL four
S33 owned paths. Stop any nonzero prerequisite before advance. Use --ff-only
and prove every WIP byte/index/status survives. No reset/clean/stash/rebase/
cherry-pick/WIP copy/transplant. Accepted H33 becomes S33's final sole parent;
no different source parent or hidden semantic correction is permitted.

## 6. Claim ceiling and remaining sequence

H33 accepts literal SQL/bundle/catalog authority ONLY. S33's scoped schema and
installer acceptance is next; then M33 metadata, R33 readiness and J33 seal
remain separate exact-parent/path/audit gates. B33 diagnostics resume only
after accepted J33. Final B33/P33/C1 remains HELD for separate audited C2/order
authority and EVERY original applicable live carry gate; none is N/A here.

Task17/C1/Task2, D24--D33 implementation closure, M5.4--M5.6, failure/C2,
public workflow/deployment, AI quality, independently adjudicated histories and
end-to-end speed/cost/utility remain PENDING. Runtime remains v1_only outside
isolated fixtures. No provider/model call, production write or performance/
completion claim follows from this pin.
