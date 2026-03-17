# lte_pm_platform

LTE PM Platform is a local developer stack for ingesting LTE PM archives into PostgreSQL and reviewing results through a small API and UI.

## Architecture

The system ingests PM archives, stores raw records in PostgreSQL, enriches them with entity and topology data, and exposes results through CLI, API, and UI.

```mermaid
flowchart LR
    A[Local or FTP archive] --> B[Archive discovery]
    B --> C[CSV reader]
    C --> D[PM parser]
    D --> E[Raw records]
    E --> F[(pm_ltefdd_sample)]
    F --> G[sync-entities]
    G --> H[(ref_lte_entity_identity)]
    H --> I[sync-topology]
    I --> J[(ref_lte_entity_topology_enrichment)]
    F --> K[KPI queries]
    K --> L[CLI / API / UI]
```

## Setup

```bash
docker compose up -d postgres

python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]

cp .env.example .env

python -m lte_pm_platform.cli init-db
```

## Run

Start the API:

```bash
./.venv/bin/python -m uvicorn lte_pm_platform.api.app:app --host 127.0.0.1 --port 8000
```

Start the UI:

```bash
cd ui
npm install
npm run dev -- --host 127.0.0.1
```

Open:

- API: `http://127.0.0.1:8000/api/v1/health`
- UI: `http://127.0.0.1:5173`

## Quick verification

```bash
curl -sS http://127.0.0.1:8000/api/v1/health
curl -sS http://127.0.0.1:8000/api/v1/ready
```

Then open the UI and confirm the main pages load correctly.

## KPI usage

Use the `KPI Results` page or the API to review data by family, aggregation level, and dataset.

Example:

```bash
curl -sS 'http://127.0.0.1:8000/api/v1/kpi-results/entity-time?family=prb&dataset_family=PM/sdr/ltefdd&limit=20'
```

## Topology workflow

Use the `Topology` page to:

1. Upload a workbook
2. Preview the snapshot
3. Run reconciliation
4. Review issues
5. Apply the snapshot
6. Run `sync-topology`

CLI example:

```bash
python -m lte_pm_platform.cli load-topology-regions --csv data/reference/regions.csv
python -m lte_pm_platform.cli sync-topology
```

## Notes

- FTP sources can be configured with `FTP_REMOTE_DIRECTORY` or `FTP_REMOTE_DIRECTORIES`.
- See [docs/reference.md](/home/mhmd/local/repo/myprojects/lte_pm_platform/docs/reference.md) for more details.
