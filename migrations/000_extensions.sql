-- GroundLoop's PostgreSQL deployment includes pgvector. M2 does not yet store
-- embeddings, but installing the extension here makes the declared database
-- runtime explicit and lets later migrations add vector columns deterministically.

CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;
