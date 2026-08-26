# CDM Migration Tool

Internal OpenText Content Server to OpenText Extended ECM Cloud (GX39 SaaS)
migration controller.

The tool scans a selected source folder/workspace from read-only PostgreSQL,
streams document versions from Azure Blob Storage, recreates supported content
under a selected GX39 destination and records resumable state plus verification
evidence in local SQLite.

## Start

Linux/macOS:

```bash
./start.sh
```

Windows PowerShell:

```powershell
.\start.ps1
```

Open `http://127.0.0.1:8110`. The application is localhost-only and does not
require an API key. On first start, dependencies are installed and
`config.example.json` is copied to local `config.json`.

## Read before operating or changing the tool

- [`AGENTS.md`](AGENTS.md) — mandatory engineering and AI-agent rules.
- [`ARCHITECTURE.md`](ARCHITECTURE.md) — implemented architecture and invariants.
- [`DEPLOYMENT_AND_QUALIFICATION.md`](DEPLOYMENT_AND_QUALIFICATION.md) — corporate
  installation, GX39 qualification and operator runbook.
- [`MIGRATION_PLAN.md`](MIGRATION_PLAN.md) — production scope and acceptance plan.

## Local verification

```bash
python3 -m unittest discover -s tests -v
.venv/bin/ruff check .
.venv/bin/mypy app.py engine tests
```

Local Dry Run and unit tests do not certify corporate PostgreSQL/Azure access,
GX39 multipart behavior or production throughput.

## Build a transfer package

```bash
python3 package_release.py
```

Transfer the generated ZIP together with its `.sha256` file. The release builder
excludes local configuration, secrets, migration state, logs, caches, virtual
environments and previous release archives.
