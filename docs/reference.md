# Reference

## 1. System Overview

`lte_pm_platform` ingests LTE PM archives from FTP or local files, records file state, loads raw counters into PostgreSQL, derives logical entities and topology mappings, calculates verified KPI views, and exposes the results through CLI, API, and UI.

## 2. End-to-End Flow

### 1. Source discovery

The system finds PM archives, parses their family, interval, and revision from the filename, and records them in the FTP registry.

Code:

- `src/lte_pm_platform/pipeline/ingest/file_discovery.py`
- `src/lte_pm_platform/pipeline/ingest/ftp_client.py`
- `src/lte_pm_platform/db/repositories/ftp_remote_file_repository.py`

Persisted table:

- `ftp_remote_file`

### 2. Run tracking and staged execution

A manual run or interval run is queued as a persistent FTP run. The worker claims queued runs, scans the configured directories, downloads exact files, and ingests only those files.

Code:

- `src/lte_pm_platform/services/operation_service.py`
- `src/lte_pm_platform/services/ftp_cycle_worker.py`
- `src/lte_pm_platform/pipeline/orchestration/ftp_staged_flow.py`

Persisted tables:

- `ftp_cycle_run`
- `ftp_cycle_run_event`

### 3. Raw ingest

Each archive is streamed, CSV members are parsed, and one normalized row is written per counter cell.

Code:

- `src/lte_pm_platform/pipeline/orchestration/sample_pipeline.py`
- `src/lte_pm_platform/pipeline/parsers/zte_lte_pm.py`
- `src/lte_pm_platform/pipeline/loaders/postgres_loader.py`

Persisted table:

- `pm_ltefdd_sample`

### 4. Entity normalization

Raw PM rows are grouped into deterministic logical entities. The rules depend on `dataset_family`.

Code:

- `src/lte_pm_platform/domain/entity_identity.py`
- `src/lte_pm_platform/db/repositories/entity_reference_repository.py`
- `sql/init/005_analytics_views.sql`

SQL objects:

- `vw_pm_distinct_entities`
- `vw_pm_raw_with_entity`
- `ref_lte_entity_identity`

### 5. Topology loading and enrichment

Topology enters the system in two ways:

- CSV reference tables for region, site, reporting, and entity-site mapping
- workbook upload for snapshot, reconciliation, apply, and activation audit

After topology references are active, `sync-topology` rebuilds the materialized enrichment table used by site and region reads.

Code:

- `src/lte_pm_platform/pipeline/ingest/topology_reference_seed.py`
- `src/lte_pm_platform/pipeline/ingest/topology_workbook.py`
- `src/lte_pm_platform/services/topology_management_service.py`
- `src/lte_pm_platform/db/repositories/topology_reference_repository.py`
- `src/lte_pm_platform/pipeline/orchestration/topology_enrichment.py`

Persisted tables:

- `ref_topology_region`
- `ref_topology_site`
- `ref_topology_reporting_hierarchy`
- `ref_topology_entity_site_map`
- `ref_lte_entity_topology_enrichment`
- `topology_snapshot`
- `topology_snapshot_entity_map`
- `topology_reconciliation_result`
- `topology_reconciliation_detail`
- `topology_activation_audit`

### 6. KPI layer

The KPI layer maps raw counters to semantic aliases, binds KPI formulas to those aliases, and exposes verified KPI views at entity, site, and region grains.

Code:

- `src/lte_pm_platform/db/repositories/semantic_kpi_repository.py`
- `src/lte_pm_platform/services/kpi_service.py`
- `sql/init/006_kpi_foundation.sql`
- `sql/init/007_kpi_views.sql`
- `sql/queries/`

Key SQL objects:

- `ref_semantic_counter_dictionary`
- `ref_semantic_kpi_definition`
- `ref_semantic_kpi_formula_input`
- `vw_semantic_kpi_base_inputs`
- `vw_verified_prb_kpi_entity_time`
- `vw_verified_bler_kpi_entity_time`
- `vw_verified_rrc_kpi_entity_time`
- verified site-time and region-time views in `sql/queries/`

### 7. API and UI

The API exposes ingestion, topology, KPI, and operations routes. The UI is a thin operator layer on top of those routes.

Code:

- API: `src/lte_pm_platform/api/routers/`
- UI client: `ui/src/api/client.ts`
- UI pages: `ui/src/pages/`

Main pages:

- `Overview`
- `Ingestion`
- `KPI Results`
- `Validation`
- `Topology`

## 3. How to Run

### 1. Start PostgreSQL

```bash
docker compose up -d postgres
```

### 2. Create the virtual environment and install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

### 3. Create local config

```bash
cp .env.example .env
```

### 4. Initialize the schema

```bash
python -m lte_pm_platform.cli init-db
```

### 5. Load sample data

Use any supported PM archive path.

```bash
python -m lte_pm_platform.cli load-sample --zip /path/to/archive.tar.gz
python -m lte_pm_platform.cli sync-entities
```

### 6. Start the API

```bash
./.venv/bin/python -m uvicorn lte_pm_platform.api.app:app --host 127.0.0.1 --port 8000
```

### 7. Start the UI

```bash
cd ui
npm install
npm run dev -- --host 127.0.0.1
```

Open:

- API health: `http://127.0.0.1:8000/api/v1/health`
- UI: `http://127.0.0.1:5173`

## 4. How to Use

### Ingest data

#### Single 15-minute interval from the UI

1. Open `#/ingestion`.
2. Look at the `Source intervals` table.
3. Review:
   - `Families`
   - `Missing`
   - `Quality`
   - `Topology`
4. Click `Run` on one interval row.
5. Watch `FTP Runs` and `Latest run events` for status and stage updates.

#### Range or backfill run from the UI

1. Stay on `#/ingestion`.
2. Use the `Run FTP cycle` form.
3. Set `Start`, `End`, `Revision policy`, optional `Families`, and `Limit`.
4. Submit the form.
5. Watch `FTP Runs` and `Latest run events`.

#### API equivalent

- queue a range or interval run:
  - `POST /api/v1/operations/ftp-run-cycle`
- inspect runs:
  - `GET /api/v1/operations/ftp-runs`
  - `GET /api/v1/operations/ftp-runs/{run_id}`
  - `GET /api/v1/operations/ftp-runs/{run_id}/events`

### Load topology

#### Workbook workflow from the UI

1. Open `#/topology`.
2. Upload an `.xlsx` workbook.
3. Preview the snapshot.
4. Run reconciliation.
5. Review blocking errors and warnings.
6. Apply the snapshot only if blocking errors are zero.
7. Run `sync-topology`.

#### CSV workflow from the CLI

```bash
python -m lte_pm_platform.cli load-topology-regions --csv data/reference/regions.csv
python -m lte_pm_platform.cli load-topology-sites --csv data/reference/sites.csv
python -m lte_pm_platform.cli load-topology-reporting --csv data/reference/reporting.csv
python -m lte_pm_platform.cli load-topology-entity-map --csv data/reference/entity_site_map.csv
python -m lte_pm_platform.cli sync-topology
```

#### Verification

```bash
python -m lte_pm_platform.cli list-unmapped-entities --limit 20
python -m lte_pm_platform.cli summarize-site-coverage --limit 20
python -m lte_pm_platform.cli summarize-region-coverage --limit 20
```

### Query KPIs

#### From the UI

1. Open `#/kpi-results`.
2. Pick a KPI family.
3. Pick a grain:
   - `entity-time`
   - `site-time`
   - `region-time`
4. Set `dataset_family`.
5. Set dates if you want a specific time window.
6. Load results.
7. Open `#/validation` when you need validation rows for the same KPI slice.

#### From the API

Routes:

- `GET /api/v1/kpi-results/entity-time`
- `GET /api/v1/kpi-results/site-time`
- `GET /api/v1/kpi-results/region-time`
- `GET /api/v1/kpi-validation/entity-time`
- `GET /api/v1/kpi-validation/site-time`
- `GET /api/v1/kpi-validation/region-time`

## 5. Core Concepts

### `dataset_family`

`dataset_family` identifies the PM source family for a row or a file. The repo uses it to:

- parse archive filenames
- build logical entity keys
- scope KPI queries
- decide which families are required for interval completeness

Examples in this repo:

- `PM/sdr/ltefdd`
- `PM/itbbu/ltefdd`
- `PM/itbbu/itbbuplat`

### 15-minute interval

An interval is the 15-minute time slice parsed from an archive filename and stored as `interval_start`.

The repo uses it to:

- group discovered source files
- queue one interval run from the Ingestion page
- show interval quality and topology visibility

### Logical entity

A logical entity is the deterministic key that identifies one observed PM entity across raw rows.

The key is built from family-specific dimensions:

- `PM/sdr/ltefdd`: `sbnid + enodebid + cellid`
- `PM/itbbu/ltefdd`: `sbnid + enbid + cellid`
- `PM/itbbu/itbbuplat`: `sbnid + meid`

The key is stored in `ref_lte_entity_identity` and reused by topology and KPI reads.

### Topology enrichment

Topology enrichment attaches site, region, and reporting fields to a logical entity.

The result is materialized in `ref_lte_entity_topology_enrichment` and is the source for:

- unmapped entity inspection
- site coverage
- region coverage
- site-time KPI outputs
- region-time KPI outputs

## 6. Subsystems

### Ingestion

#### Registry

`ftp_remote_file` stores discovered source files and their lifecycle state.

It records:

- remote path
- family
- interval
- revision
- status
- download / ingest attempts
- staged path
- error details

#### Run tracking

`ftp_cycle_run` stores one queued or executed FTP run.

`ftp_cycle_run_event` stores stage-level events for that run.

The Ingestion page reads both tables to show run history and latest events.

#### Staged flow

The staged FTP flow is implemented in `ftp_staged_flow.py`.

It does four things:

1. discover candidate files
2. download pending files
3. ingest downloaded files
4. retry failed downloads or ingests when requested

### Entity

Entity identity is the layer between raw PM rows and topology.

It exists because raw rows do not arrive with a stable site or region key. The repo first derives a stable logical entity key, then maps that key into topology.

The rules live in two places:

- Python: `src/lte_pm_platform/domain/entity_identity.py`
- SQL: `vw_pm_distinct_entities` and `vw_pm_raw_with_entity`

### Topology

The topology subsystem has two parts.

#### Reference tables

CSV loads populate:

- `ref_topology_region`
- `ref_topology_site`
- `ref_topology_reporting_hierarchy`
- `ref_topology_entity_site_map`

#### Workbook lifecycle

The workbook lifecycle stores uploads as snapshots before they touch live topology tables.

Flow:

1. parse workbook
2. create snapshot metadata
3. store normalized snapshot rows
4. reconcile snapshot
5. inspect reconciliation details
6. apply snapshot into live reference tables
7. rebuild `ref_lte_entity_topology_enrichment`

### KPI

The KPI subsystem has two layers.

#### Semantic layer

The semantic layer maps raw counters into stable aliases and KPI formula inputs.

Main objects:

- `ref_semantic_counter_dictionary`
- `ref_semantic_kpi_definition`
- `ref_semantic_kpi_formula_input`
- `vw_semantic_kpi_base_inputs`

#### Verified KPI views

The verified layer exposes KPI outputs that the repo treats as usable reads.

Examples:

- PRB entity-time
- BLER entity-time
- RRC entity-time
- PRB site-time / region-time
- BLER site-time / region-time
- RRC site-time / region-time

The API reads these through `SemanticKpiRepository` and `KpiService`.

## 7. Rules

- `dataset_family` is required for `entity-time` KPI results.
- `dataset_family` is required for PRB and BLER `site-time` and `region-time` KPI results.
- `dataset_family` is required for `site-time` and `region-time` KPI validation.
- `interval_start` must align to a 15-minute boundary: `00`, `15`, `30`, or `45` minutes, with zero seconds.
- Source interval completeness is driven only by:
  - `PM/itbbu/ltefdd`
  - `PM/sdr/ltefdd`
- `PM/itbbu/itbbuplat` can appear in `families_present`, but it does not drive `missing_families` or `partial_interval`.
- `partial_interval` means one required LTE PM family is present and another required LTE PM family is missing.
- Interval topology coverage is derived from the exact ingested source files for the displayed intervals and the required LTE PM families only.
- The Ingestion page shows `no topology rows` when no topology-observable rows exist for the required LTE PM families in that interval.
- If topology mappings are missing, `sync-topology` writes `UNMAPPED` rows into `ref_lte_entity_topology_enrichment`. Site and region outputs then stay limited by those missing mappings.
- The Overview page does not auto-load detailed site/region coverage or KPI validation. Those reads are on demand.

## 8. Limitations

- Workbook parsing expects a `4G LTE` sheet and the required workbook columns used by `parse_topology_workbook`.
- FTP run execution uses an in-process worker started by the API. Queued runs execute inside that API process.
- Detailed site and region coverage uses SQL aggregation over topology-enriched PM data. The UI loads it on demand because it is heavier than the overview and interval-summary reads.
- Entity identity rules exist in both Python and SQL. If a family-specific identity rule changes, both implementations must stay aligned.
