# lte_pm_platform

LTE PM Platform is a local developer stack for ingesting ZTE LTE PM archives into PostgreSQL, syncing entity and topology reference data, and checking KPI results through an API and a small UI.

## Verify the system

### Start the database

```bash
docker compose up -d postgres
```

### Install and initialize

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
cp .env.example .env
python -m lte_pm_platform.cli init-db
```

### Start the API

```bash
./.venv/bin/python -m uvicorn lte_pm_platform.api.app:app --host 127.0.0.1 --port 8000
```

### Start the UI

```bash
cd ui
npm install
npm run dev -- --host 127.0.0.1
```

### Open the app

- API health: `http://127.0.0.1:8000/api/v1/health`
- UI: `http://127.0.0.1:5173`

### Basic local checks

```bash
curl -sS http://127.0.0.1:8000/api/v1/health
curl -sS http://127.0.0.1:8000/api/v1/ready
```

Useful UI pages:

- `#/overview`
- `#/ingestion`
- `#/kpi-results`
- `#/validation`
- `#/topology`

## Check KPI results

Use the `KPI Results` page or call the API directly.

Main filters:

- `family`
  - `prb`
  - `bler`
  - `rrc`
- `grain`
  - `entity-time`
  - `site-time`
  - `region-time`
- `dataset_family`
  - usually `PM/sdr/ltefdd` or `PM/itbbu/ltefdd`

Current KPI families in the repo:

- PRB
  - `dl_prb_utilization`
  - `ul_prb_utilization`
- BLER
  - `dl_bler`
  - `ul_bler`
- RRC
  - `rrc_connected_users_max`
  - `rrc_connected_users_mean`
  - `rrc_connected_users_online`

Date usage:

- the UI uses date inputs
- `from` is sent as start of day
- `to` is sent as end of day
- if a supported query omits dates, the backend may use the latest available `collect_time`

Simple examples:

```bash
curl -sS 'http://127.0.0.1:8000/api/v1/kpi-results/entity-time?family=prb&dataset_family=PM/sdr/ltefdd&limit=20'
```

```bash
curl -sS 'http://127.0.0.1:8000/api/v1/kpi-results/site-time?family=bler&dataset_family=PM/itbbu/ltefdd&limit=20'
```

Validation is available from the `Validation` page.

## Topology workflow

Use the `Topology` page for workbook-based topology updates.

Typical flow:

1. Upload workbook
2. Preview snapshot
3. Run reconciliation
4. Review warnings and blocking issues
5. Apply snapshot
6. Run `sync-topology`

If you want to load topology from CSV files instead:

```bash
python -m lte_pm_platform.cli load-topology-regions --csv data/reference/regions.csv
python -m lte_pm_platform.cli load-topology-sites --csv data/reference/sites.csv
python -m lte_pm_platform.cli load-topology-reporting --csv data/reference/reporting.csv
python -m lte_pm_platform.cli load-topology-entity-map --csv data/reference/entity_site_map.csv
python -m lte_pm_platform.cli sync-topology
```

## Notes

- FTP source config supports either `FTP_REMOTE_DIRECTORY` or `FTP_REMOTE_DIRECTORIES`.
- The Ingestion page shows persistent FTP run history from the backend.
- Detailed topology coverage is loaded on demand from the Topology page.
- `docs/reference.md` contains deeper CLI and reference-data details.
