# Reference Notes

This file keeps deeper operational and reference material out of the front-page README.

## Verified KPI Reference Files

Current reference seeds in `data/reference/` include:

- `counter_dictionary.csv`
- `proposed_verified_prb_kpi_definitions.csv`
- `proposed_verified_bler_kpi_definitions.csv`
- `kpi_definitions_rrc_slice.csv`
- vendor-indicator curation and starter-review files

Current verified KPI families:

- PRB
- BLER
- direct-mapped RRC

## Topology Reference Loading

Expected local topology CSV inputs:

- `data/reference/regions.csv`
- `data/reference/sites.csv`
- `data/reference/reporting.csv`
- `data/reference/entity_site_map.csv`

Those filenames are the expected loader inputs, but the curated CSVs are not currently bundled in this repo.

Topology load workflow:

```bash
python -m lte_pm_platform.cli load-topology-regions --csv data/reference/regions.csv
python -m lte_pm_platform.cli load-topology-sites --csv data/reference/sites.csv
python -m lte_pm_platform.cli load-topology-reporting --csv data/reference/reporting.csv
python -m lte_pm_platform.cli load-topology-entity-map --csv data/reference/entity_site_map.csv
python -m lte_pm_platform.cli sync-topology
```

Verification:

```bash
python -m lte_pm_platform.cli list-unmapped-entities --limit 20
python -m lte_pm_platform.cli summarize-site-coverage --limit 20
python -m lte_pm_platform.cli summarize-region-coverage --limit 20
```

If topology reference rows are missing, `sync-topology` will populate `ref_lte_entity_topology_enrichment` with `UNMAPPED` rows and site/region KPI outputs will not be meaningful.

## API Baseline

Implemented API domains:

- health / readiness
- ingestion reads
- topology reads
- KPI results
- KPI validation
- manual operator actions

Important current guardrails:

- KPI Results `entity-time` requires `dataset_family`
- if `entity-time` dates are omitted, the backend defaults to the latest `collect_time` for that `dataset_family`
- KPI Results UI uses date inputs and normalizes them to day bounds
- KPI Results UI uses offset-based paging with `Rows`, `Previous`, and `Next`

## Operator UI Baseline

Pages currently implemented:

- Overview
- Ingestion
- KPI Results
- Validation
- Topology

The UI is intentionally table- and form-based. It is not a charts-first analytics layer.

## Useful Commands

Initialize DB:

```bash
python -m lte_pm_platform.cli init-db
```

Load sample data:

```bash
python -m lte_pm_platform.cli load-sample \
  --zip data/input/local_selection/UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_0000.tar.gz
python -m lte_pm_platform.cli sync-entities
```

Load KPI references:

```bash
python -m lte_pm_platform.cli load-counter-dictionary --csv data/reference/counter_dictionary.csv
python -m lte_pm_platform.cli load-kpi-definitions --csv data/reference/proposed_verified_prb_kpi_definitions.csv
python -m lte_pm_platform.cli load-kpi-definitions --csv data/reference/proposed_verified_bler_kpi_definitions.csv
python -m lte_pm_platform.cli load-kpi-definitions --csv data/reference/kpi_definitions_rrc_slice.csv
```

Run API:

```bash
./.venv/bin/python -m uvicorn lte_pm_platform.api.app:app --host 0.0.0.0 --port 8000
```

Run UI:

```bash
cd ui
npm install
npm run dev
```

## SQL / View Scope

Important view groups:

- raw and entity-aware views
- topology projection and coverage views
- semantic KPI base-input and coverage views
- verified KPI output views
- verified KPI validation views

For exact SQL object names, inspect `sql/init/` and `sql/queries/`.
