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

Local topology CSV inputs now present in `data/reference/`:

- `data/reference/regions.csv`
- `data/reference/sites.csv`
- `data/reference/reporting.csv`
- `data/reference/entity_site_map.csv`

These were generated conservatively from:

- `LTE Project Parameter-20260301.xlsx`
- sheet: `4G LTE`

The current local seed set is good enough for local development and operator validation. It should not yet be treated as the final authoritative production mapping source.

## Topology Workbook Snapshot Workflow

The current topology authority baseline is workbook-driven.

Implemented workflow:

- upload workbook through the API/UI
- extract release date from the workbook filename when present
- parse the `4G LTE` sheet into normalized snapshot rows
- store the upload as a candidate topology snapshot
- reconcile the candidate snapshot against:
  - PM entity identity
  - the current active snapshot
- inspect detailed issues before activation
- apply a reconciled snapshot into the live topology reference tables
- run `sync-topology` to refresh `ref_lte_entity_topology_enrichment`

Current hard blocks:

- duplicate entity mapped to multiple sites
- conflicting site to region mapping
- parser errors on critical workbook keys

Current soft warnings:

- PM entities missing from workbook
- workbook entities missing from PM
- workbook sites with no PM activity
- normal drift versus the active snapshot

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

Current local verification status:

- topology reference tables load successfully
- `sync-topology` materially maps `ref_lte_entity_topology_enrichment`
- remaining unmapped rows are a small minority
- topology-aware site/region operator paths are now meaningful locally

## FTP Source Configuration

Supported config:

- `FTP_REMOTE_DIRECTORY`
  - backward-compatible single-directory mode
- `FTP_REMOTE_DIRECTORIES`
  - explicit multi-directory mode
  - comma-separated list of remote directories

Example:

```dotenv
FTP_REMOTE_DIRECTORIES=/pm_archive/PM/sdr/ltefdd,/pm_archive/PM/itbbu/ltefdd,/pm_archive/PM/itbbu/itbbuplat
```

Behavior:

- each configured remote directory is scanned explicitly
- broad recursive FTP scanning is not used
- new `ftp_remote_file` rows preserve:
  - `remote_directory`
  - full `remote_path`

Current local-dev caveat:

- older registry rows created before full remote-path identity may still use filename-only `remote_path` values
- the new multi-directory path works correctly for newly discovered rows
- for a fully clean local registry state, rebuild or clean the old registry rows once

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
- PRB and BLER `site-time` / `region-time` also require `dataset_family`
- if PRB or BLER `site-time` / `region-time` dates are omitted, the backend defaults to the latest `collect_time` for that `dataset_family`
- `site-time` / `region-time` KPI validation now also requires `dataset_family`
- the Overview page no longer auto-loads validation queries; validation is on-demand from the Validation page
- the Overview page also avoids auto-loading detailed site/region coverage aggregates; it uses a lightweight topology summary instead
- detailed site/region coverage remains available from the Topology page as an explicit operator action
- KPI Results UI uses date inputs and normalizes them to day bounds
- KPI Results UI uses offset-based paging with `Rows`, `Previous`, and `Next`
- operator-facing PRB and BLER `site-time` / `region-time` API routes use direct fast paths over narrowed raw facts plus topology enrichment instead of the heavier nested verified SQL views
- topology snapshot Apply is intended only after reconciliation and is blocked when blocking issues are present
- FTP scanning remains explicit and directory-configured; it does not switch to broad recursive crawling
- FTP cycles now use persistent `ftp_cycle_run` and `ftp_cycle_run_event` records
- long-running FTP cycles are executed by an in-process background worker; HTTP requests enqueue runs and return immediately
- refreshing the UI does not cancel an active FTP cycle
- on API startup, stale `running` FTP cycle rows from an interrupted prior process are marked `failed`
- stale-run recovery preserves existing `summary_json` and sets:
  - `finished_at`
  - `error_message = "Run interrupted by process restart before completion"`
- stale runs are not auto-requeued yet
- startup recovery is schema-aware; if `ftp_cycle_run` tables have not been applied yet, the API skips recovery instead of crashing
- Rust/Go remain deferred until profiling proves a real parsing or concurrency hotspot

Current topology API additions:

- `POST /api/v1/topology/workbook-preview`
- `GET /api/v1/topology/snapshots`
- `GET /api/v1/topology/snapshots/{snapshot_id}`
- `GET /api/v1/topology/active-snapshot`
- `POST /api/v1/topology/snapshots/{snapshot_id}/reconcile`
- `GET /api/v1/topology/reconciliations/{reconciliation_id}/details`
- `GET /api/v1/topology/snapshots/{snapshot_id}/drift`
- `POST /api/v1/topology/snapshots/{snapshot_id}/apply`
- `POST /api/v1/topology/sync`

Current operations API additions:

- `POST /api/v1/operations/ftp-run-cycle`
- `GET /api/v1/operations/ftp-runs`
- `GET /api/v1/operations/ftp-runs/{run_id}`
- `GET /api/v1/operations/ftp-runs/{run_id}/events`

## Operator UI Baseline

Pages currently implemented:

- Overview
- Ingestion
- KPI Results
- Validation
- Topology

The UI is intentionally table- and form-based. It is not a charts-first analytics layer.

Ingestion page additions now include:

- persistent FTP run list
- running vs recent FTP run visibility
- stage/event inspection for the latest run
- polling-based visibility that survives page refresh and navigation

Topology page additions now include:

- workbook upload / preview
- snapshot history
- reconciliation summary
- reconciliation detail inspection
- apply snapshot
- run sync-topology

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
