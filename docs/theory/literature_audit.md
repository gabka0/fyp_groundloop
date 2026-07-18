# Primary-Source Literature Audit: Exact-Flip Policy Maintenance

Audit date: 2026-07-18. Scope: database IVM foundations, dynamic conjunctive
queries, ordered range reporting, and materialized-view changes caused by
query/predicate redefinition. This audit supports a conservative positioning;
it is not a claim of systematic-review completeness.

## Search protocol

Searches used combinations of:

- `incremental view maintenance query parameter update threshold predicate`;
- `output sensitive view maintenance parameter changes`;
- `materialized view adaptation selection predicate redefinition`;
- `dynamic conjunctive query q-hierarchical updates`;
- `dynamic one dimensional range reporting`;
- the exact titles `DBSP`, `F-IVM`, `CROWN`, and `Maintaining Views
  Incrementally`;
- recency-constrained variants for 2025 and 2026.

The review followed primary paper pages, author/institution PDFs, DOI records,
and conference proceedings. Search-result snippets, documentation, and surveys
were used for routing but not as sole evidence for the classification.

## Findings by required line of work

### Classical counting IVM

[Gupta, Mumick, and Subrahmanian (SIGMOD 1993)](https://doi.org/10.1145/170035.170066)
explicitly maintain counts of alternative derivations under inserts, deletes,
and updates, including set and duplicate semantics. GroundLoop's signed
content refcounts and zero crossings are a direct specialization, with the
additional domain choice that derivations collapse by normalized content hash.

### DBSP

[DBSP (PVLDB 2023)](https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf)
provides an algebraic incrementalization framework for rich query languages
and derives view-change streams from database-change streams. It strongly
supports the signed-delta framing but does not, in the inspected theorem and
SQL sections, establish this score-partition result for a changed query
threshold.

### F-IVM

[F-IVM (SIGMOD 2018 author manuscript)](https://www.cs.ox.ac.uk/dan.olteanu/papers/no-sigmod18.pdf)
maintains a hierarchy of simpler keyed views and factorizes keys, payloads, and
updates. It is relevant to the hierarchy and auxiliary-state design, not a
novelty basis for a one-dimensional policy predicate.

### CROWN

[CROWN (PVLDB 2023)](https://cs.uwaterloo.ca/~xiaohu/papers/vldb23.pdf)
studies full and delta enumeration under tuple insertions/deletions and avoids
join blowups using semijoin/projection views. GroundLoop's direct-witness query
is far simpler than CROWN's general join workloads. CROWN does not make the
policy-threshold specialization new.

### Dynamic conjunctive-query maintenance

[Berkholz, Keppeler, and Schweikardt](https://arxiv.org/abs/1702.06370)
give constant update and constant-delay/counting results for q-hierarchical
conjunctive queries after linear preprocessing, with conditional lower bounds
outside the tractable classes. GroundLoop's direct-witness hierarchy lies on
the easy side. Their OMv/OV lower bounds are not the source of this document's
`Omega(f+p)` claim, which is only an explicit-output/write lower bound.

The [PODS 2024 Gems survey](https://sigmod.org/wp-content/uploads/2024/09/GemsPODS24-Olteanu.pdf)
also shows why factorized output can avoid materializing a linear number of
join-result changes. GroundLoop, however, explicitly stores changed labels and
claim/answer boundaries, so it cannot use factorization to avoid writing those
declared outputs.

### View-definition and predicate changes

[Gupta, Mumick, and Ross (SIGMOD 1995)](https://research.ibm.com/publications/adapting-materialized-views-after-redefinitions)
ask how to adapt a materialized view when its definition changes, cover SQL
SELECT-FROM-WHERE-GROUP-BY redefinitions, identify useful extra maintained
information, and handle simultaneous changes. This is the closest conceptual
precedent for a decision-policy threshold change: the data scores remain fixed
while the selection definition changes.

### Ordered selection/range reporting

[Mortensen, Pagh, and Patrascu](https://arxiv.org/abs/cs/0502032) treat dynamic
one-dimensional interval reporting as a fundamental ordered-search problem.
The GroundLoop index uses a simpler comparison-based AVL tree, giving
worst-case `O(log E)` point updates/endpoint search and linear reporting in the
returned interval. The data-structure ingredient is standard rather than a new
database operator.

## Threshold-specific result

The searches did not find a primary source stating GroundLoop's exact
`P_support`/`P_refute` partition under this REFUTE-conservative neural score
tie rule. Absence from this bounded audit is not evidence of novelty. The
mechanism follows immediately from fixed dominance regions plus classical
one-dimensional range reporting and view adaptation.

Classification (exactly one): **known mechanism specialized to GroundLoop**.

The justified claim is:

> For frozen tie rule v1, partitioning active current observations by their
> threshold-independent winning score makes threshold-only policy maintenance
> output-sensitive in the exact number of label flips, with worst-case linear
> degeneration when the flip output is linear.

Not justified: `first`, novel IVM algorithm, faster than DBSP/F-IVM/CROWN, or
faster than prior predicate-maintenance systems. No apples-to-apples
implementation comparison was performed.

Confidence: **moderate** for literature classification; **high** that the
listed works make a stronger novelty label inappropriate.
