# M3 Verifier Lane Status

Status: implementation and D-5 real experiment complete in the lane; final
integration rebase and shared-gate validation pending.

## Delivered

- Deterministic fake and lazy pinned MiniLM2 adapter.
- Exact base-logit to stored-score mapping and finite temperature softmax.
- Auditable logits, output/input hashes, artifact identities, and inactive late
  completion semantics.
- Location-independent fine-tuned checkpoint identity plus relocation test.
- Leakage-safe SciFact/WiCE preparation with fixed mappings, official splits,
  claim grouping, normalized deduplication, checksums, and seed.
- One bounded CPU fine-tuning run, development-only temperature scaling,
  untouched public-test evaluation, and independent software/API transfer
  evaluation.
- Per-class metrics, present-class macro-F1, confusion matrices, multiclass
  Brier, 10-bin ECE/reliability rows, and seeded bootstrap confidence intervals.
- Explicit prepare/train/calibrate/evaluate/real-smoke commands that fail when
  prerequisites are absent. Ordinary tests do not download models or data.

## Real experiment outcome

The fine-tuned calibrated model improves public-test present-class macro-F1
from 0.4001 to 0.5298 and public-test ECE from 0.2597 to 0.0709 relative to the
zero-shot base. Transfer macro-F1 moves from 0.6083 to 0.6646, but transfer ECE
is 0.2834 versus 0.2698 uncalibrated. All public-test examples truncate and
public-test REFUTE is not estimable. See `MODEL_CARD.md` and
`CALIBRATION_REPORT.md`; no stronger claim is warranted.

## External artifacts

All raw data, cached base weights, fine-tuned weights, and per-example logits
are under `/tmp/groundloop-m3-verifier-20260718` and are not Git deliverables.
The external location may be ephemeral; hashes in the model card and handoff
are the durable identity record.

