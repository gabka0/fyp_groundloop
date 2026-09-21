CREATE INDEX groundloop_m5_admitted_pair_by_chunk_edge
    ON groundloop_m5_requirement_admitted_pair (
        chunk_version_id COLLATE "C"
    );

CREATE INDEX groundloop_m4_job_by_epoch
    ON groundloop_semantic_job (epoch_id);
