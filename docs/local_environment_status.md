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
- The repository now has a working `.venv`, created with user-space
  `virtualenv` because the operating-system `python3-venv` package is still
  missing. The project and normal `dev` dependencies are installed editable.
- Docker is not currently installed, so the PostgreSQL Compose service has not
  been started or validated on this host.
- `psql` and a local PostgreSQL server are not installed. The repository
  `.venv` now contains Psycopg 3.3.4, SQLAlchemy 2.0.51, pgvector client 0.5.0,
  FastAPI 0.139.2, NumPy 2.5.1, pytest 9.1.1, Ruff 0.15.22, and mypy 2.3.0.
- `pglast` 7.17 was installed in the user site on 2026-07-18 solely for static
  PostgreSQL parsing. It parsed all 29 migration statements and all 6 oracle
  statements. This proves parser acceptance, not execution or constraint/view
  behavior on a live server.

To install the missing privileged host prerequisites using Docker's official
Ubuntu repository procedure:

```bash
scripts/install_system_prerequisites_ubuntu.sh
```

Install Docker Engine and the Compose plugin using the instructions for the
host distribution; package names and repository setup vary. Depending on the
host's Docker policy, add the user to the Docker group or run Docker through
the configured service mechanism. Then execute:

```bash
docker compose up -d db
set -a
source .env
set +a
make validate-postgres
```

The optional `ml` dependency group is intentionally not installed. Select and
record the initial embedding, claim-extraction, and verification models before
installing it.
