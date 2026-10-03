# Task 5C Hosted-Verifier Activation

## Boundary

Task 5C executes the request bytes frozen by Task 5B. It may add only the
network runner, strict parsers, resumable ledgers, result evaluation, tests,
and this evidence. It does not alter M4/M5 contracts or promote a new default
verifier into the runtime.

The API key is read only from `OPENAI_API_KEY`. It must never enter a request
artifact, result, log, test, document, commit, or command-line argument.

## Frozen model and prompt

- Provider: OpenAI
- Model: `gpt-6-luna`
- Endpoint: `/v1/responses`
- Reasoning effort: `none`
- Output: strict `support|refute|neutral` JSON schema
- Prompt hash:
  `f38392b2ec95b3b8a65ec380bc3196def6096d6e61da11b945e59f6f9a94be6f`
- Request manifest hash:
  `ad962ba1ec4336fd24f1d255f511cc93f5baa4a157d85d3c0f16e31a41d49bc2`
- Cumulative Task 5C spend cap: USD 4.00

Gold labels, stratum, page identity, and expected affected identities are not
included in held-out requests. Gold is joined only after provider responses
are frozen.

## Development smoke

The only allowed development smoke executed 32 balanced own-page old/new
requests. All 32 responses were complete and schema-valid.

- Accuracy: 26/32 (81.25%)
- Old-side accuracy: 12/16
- New-side accuracy: 14/16
- Input/output tokens: 7,122 / 587
- Actual standard-endpoint cost: USD 0.0010057
- Sum of per-request elapsed time: 131,330 ms
- Result SHA-256:
  `30c860e066df5bce80ac630c479421ba7a7b6c0e5f0b9883945fb2865f2b3fdb`

The errors expose a real label/input limitation. Some source evidence omits
the subject named in the claim, while semantically similar changed-value
relations are `neutral` in the `support_neutral` stratum and `refute` in the
`support_refute` stratum. Supplying the stratum would leak the target
construction, so it remains excluded. Accuracy is therefore reported as a
diagnostic, not used as a retroactively invented held-out gate.

No second development smoke is allowed: the frozen 32-request stage is
exhausted. The v1 prompt and schema remain unchanged.

## Held-out execution and success gate

The Batch program runs sequentially to stay below queued-token limits and to
prevent duplicate submissions:

1. selected new path: 1,024 requests, one Batch call;
2. baseline old observations: 32,768 requests, two Batch calls; and
3. exhaustive new observations: 32,768 requests, two Batch calls.

The exhaustive old/new observations define the model-relative effect oracle.
For an unselected claim, replacement removes the old observation and leaves
the claim unsupported. Each source case forms one controlled answer with its
two claims required. This is a controlled projection, not a natural deployed
answer population.

The existing Task 5A thresholds are reused rather than chosen after seeing
held-out results. A budget qualifies only if it simultaneously has:

- at least 80% verifier-pair work reduction; and
- at least 95% pair, claim, claim-status, and answer-status effect recall.

The final verdict is `PASS` if at least one frozen budget qualifies; otherwise
it is `NO_GO`. Annotated own-page label accuracy, actual input/output tokens,
actual API cost, Batch calls, provider processing time, submission-to-complete
time, selector time, and local projection time are always reported. OpenAI
does not expose pure accelerator inference time, so provider Batch lifecycle
time must not be renamed as pure model compute time.

## Stop conditions

Stop without advancing the next job if any request is missing, duplicated,
unknown, non-200, incomplete, schema-invalid, or if the projected cumulative
cost exceeds USD 4.00. The ledger advances only its first incomplete job, so a
retry cannot silently resubmit completed work.
