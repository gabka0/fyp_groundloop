# Extraction provenance contract request

Decision: **accepted with amendment** in coordinator commits `ce514c9` and
`4e6165c`.

The accepted shared shape is narrower than the proposal below:

- `CitedAnswer` carries generation input hash, raw-output hash, and bounded
  `repair_count`;
- `ClaimExtractionResult` carries claims, extraction input hash,
  raw-output hash, and bounded `repair_count`;
- model/prompt task identities remain in the typed artifact registries and
  publication bundle/database execution rows;
- ordered context is the immutable ordered citation tuple included in the
  normalized complete input hash.

No additional shared provenance DTO was added. The lane retains an equivalent
runtime provenance view for inspectable component execution and handoff.

## Failing case

The frozen M3 generation-lane prompt requires every claim-extraction execution
to record the normalized complete input hash, raw-output hash, and bounded
repair count. `AtomicClaim` contains only the semantic claim fields, and
`PipelineRunManifest` has no extraction-execution record. Consequently the
coordinator pipeline cannot persist or serialize those required fields through
the shared contract without depending on lane-private runtime state.

Generation has a partial equivalent because `CitedAnswer` carries `input_hash`
and `raw_output_hash`, but neither generation nor extraction has a shared
repair-count field.

## Proposed minimal change

Add an immutable shared execution-provenance record with:

- task (`generation` or `claim_extraction`);
- model artifact ID and exact model/tokenizer revisions;
- prompt artifact ID, prompt hash, and decoding-config hash;
- ordered context chunk-version IDs;
- normalized complete input hash;
- raw-output hash;
- repair count (`0` or `1`).

Add an ordered tuple of these records to `PipelineRunManifest`. Keep
`CitedAnswer` and `AtomicClaim` unchanged. The generation lane will expose the
same shape as a lane-local `StructuredOutputProvenance` until the coordinator
accepts or replaces this proposal.

## Compatibility impact

This is an additive manifest field but would require updating all manifest
constructors and persistence serialization. A default empty tuple could ease
construction, but PUBLISHED M3 manifests should require exactly one generation
and one extraction record.

## Required test

Publish a deterministic repaired generation/extraction run and assert that the
round-tripped manifest preserves both input/output hashes, ordered contexts,
artifact identities, and repair counts. Also assert that a repair count above
one and missing provenance on a PUBLISHED manifest are rejected.
