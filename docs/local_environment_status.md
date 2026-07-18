# Local Environment Status

Checked: 2026-07-18

Host observations:

- `python3` is available: Python 3.12.3.
- `python` is not currently available as a command; repository instructions use
  `python3`.
- The source and tests compile successfully with `python3 -m compileall`.
- `pyproject.toml` parses successfully with Python's `tomllib`.
- The dependency-free GroundLoop domain import smoke test passes.
- `pytest` 9.1.1, `ruff` 0.15.22, and `mypy` 2.3.0 are installed in the user
  site-packages (installed 2026-07-17 via
  `pip3 install --user --break-system-packages pytest ruff mypy`) and run as
  `python3 -m pytest` / `python3 -m ruff` / `python3 -m mypy`.
- The validated versions are inside the development ranges declared by
  `pyproject.toml` (`pytest<10`, `mypy<3`, `ruff<1`).
- The repository has a working `.venv`; the project and normal `dev`
  dependencies are installed editable. The system `python3-venv` prerequisite
  is now installed.
- Docker Engine 29.6.2 and Docker Compose v5.3.1 are installed. The account is
  in the `docker` group; existing shells may require `sg docker` until the next
  logout/login. The Compose PostgreSQL service is running and healthy.
- `psql` is not installed on the host. PostgreSQL 16.14 runs in Compose with
  pgvector 0.8.5 available and installed. The repository
  `.venv` now contains Psycopg 3.3.4, SQLAlchemy 2.0.51, pgvector client 0.5.0,
  FastAPI 0.139.2, NumPy 2.5.1, pytest 9.1.1, Ruff 0.15.22, and mypy 2.3.0.
- `pglast` 7.17 was installed on 2026-07-18 solely for static
  PostgreSQL parsing. It parsed all 29 migration statements and all 6 oracle
  statements. This proves parser acceptance, not execution or constraint/view
  behavior on a live server.
- The M3 hardware audit found an AMD Ryzen 7 5700U with 8 physical cores / 16
  logical CPUs, 14 GiB RAM, 4 GiB swap, and approximately 92 GiB free disk.
  No NVIDIA device, CUDA driver, or `nvidia-smi` is available. M3 is therefore
  CPU-first and uses compact pinned models; sustained work is reserved for the
  verifier lane and real-model executions are serialized.
- Before the M3 contract baseline, the ML group was absent. The coordinator
  installed and verified the CPU stack on 2026-07-18: PyTorch 2.13.0+cpu,
  Transformers 4.57.6, Sentence Transformers 5.6.0, scikit-learn 1.9.0,
  Datasets 4.8.5, Accelerate 1.14.0, Hugging Face Hub 0.36.2, and Safetensors
  0.8.0. `torch.cuda.is_available()` is false and `pip check` reports no
  broken requirements.
- The Docker service is healthy, but shells created before the account joined
  the `docker` group need `sg docker -c '<command>'` until the next login.

The privileged host prerequisites were installed using Docker's official
Ubuntu repository procedure:

```bash
scripts/install_system_prerequisites_ubuntu.sh
```

The database gate can be rerun with:

```bash
docker compose up -d db
set -a
source .env
set +a
make validate-postgres
```

The initial M3 models and datasets are now frozen in
`docs/m3_model_dataset_audit.md`. The optional `ml` dependency group may be
installed by the coordinator; model weights, datasets, and trained checkpoints
must remain outside Git.
