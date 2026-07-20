# Nonblocking schema hardening request

Owner requested: M4 coordinator / next migration owner.

The current `groundloop_impact_evaluation_run` table can faithfully store the
bounded event-audit record in its JSONB manifest, and the new runner validates
that link against a sealed `groundloop_epoch` plus `groundloop_m4_update` at
write and replay time. However, the database schema itself cannot enforce the
event relationship or result immutability.

Before running large M4.6 histories, add a coordinator-owned migration with:

1. `epoch_id bigint NOT NULL REFERENCES groundloop_m4_update(epoch_id)`;
2. `event_id text NOT NULL REFERENCES groundloop_epoch(event_id)` or a single
   epoch foreign key plus a trigger checking the event projection;
3. `input_hash char(64) NOT NULL` and `result_hash char(64) NOT NULL`;
4. `audit_manifest_id text NOT NULL` and `refresh_manifest_id text NOT NULL`;
5. integer event metrics in typed columns or a child table, including expected
   pairs, verifier-positive pairs, omitted verifier-positive pairs,
   status-effect denominator/numerator, answer-effect denominator/numerator,
   verifier calls and token counts;
6. an immutable-row trigger after `status = 'completed'`; and
7. a uniqueness constraint over the intended logical identity
   `(epoch_id, treatment_manifest_id, split_id, config_hash)`.

Do not silently retrofit these columns in this lane. Migration ordering and
backfill semantics are coordinator-owned, and existing controlled-evaluation
records may need an explicit compatibility path.
