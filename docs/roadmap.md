# LTE PM Data Platform Roadmap

## 0. Project Mission

Build a real telecom data platform around ZTE LTE PM archive files.

The system demonstrates:

- data engineering
- infrastructure engineering
- systems engineering
- telecom performance data processing

Platform story:

`FTP -> remote registry and staged acquisition -> archive ingestion -> CSV parsing -> normalized counter storage -> SQL analytics -> KPI computation -> CLI/API/UI access -> manual operator control first`

## 1. Core Stack

Chosen stack:

- Python for ingestion orchestration, parsing, API, and CLI tools
- PostgreSQL for raw fact storage, audit tracking, registry state, and SQL analytics
- SQL for transformations, aggregations, observability, and KPI computation
- Docker Compose for local infrastructure
- Pytest + Ruff for testing and linting guardrails

Not included in the first system version:

- pandas-based ingestion
- Airflow orchestration
- Kafka streaming
- Kubernetes deployment
- Rust/Go optimizations

## 2. Engineering Rules

- Keep modules small and responsibility-based.
- Do not mix parser logic with database logic.
- Do not mix ingestion orchestration with KPI/business logic.
- Stream CSV rows; do not rely on heavy dataframe pipelines.
- Use SQL for analytical transformations.
- Build vertical slices first.
- Add tests early.
- Focus on ZTE LTE PM before attempting multi-vendor abstractions.
- Keep convenience commands as wrappers over the real operational path.
- Make state transitions explicit and recoverable.

## 3. Platform Architecture

### Data Source

- FTP server hosting ZTE LTE PM archives
- local mirrored archive inputs

### Ingestion Pipeline

- FTP discovery
- remote registry tracking
- staged download
- staged ingest
- archive streaming
- CSV parsing
- counter normalization

### Database Layer

- PostgreSQL raw fact tables
- audit and ingestion governance
- FTP remote registry (`ftp_remote_file`)
- reference tables

### Entity Layer

- logical entity identity
- eNB-style network entity keys where supported by family rules
- cell-level identity relationships
- curated site / region / reporting hierarchy enrichment

### Analytics Layer

- SQL observability queries
- coverage analysis
- KPI calculations

### Interface Layer

- CLI operations
- API access
- minimal operator UI

## 4. Current Implementation Status

### Implemented

- archive ingestion pipeline
- CSV streaming parser
- ZTE LTE PM parser
- normalized counter storage
- `file_audit` governance system
- file lifecycle management
- entity identity generation
- family-specific logical entity identity rules
- SQL observability views
- CLI operational commands
- Dockerized Postgres environment
- test suite
- FTP remote registry (`ftp_remote_file`)
- staged FTP discovery, download, ingest, retry, and reconciliation flows
- explicit failure inspection commands and recovery helpers
- scheduler-driven FTP cycle execution
- stronger remote metadata capture in the registry
- topology reference tables for region, site, reporting hierarchy, and entity-to-site mapping
- topology enrichment sync layered on top of logical entity identity
- topology inspection commands for unmapped entities and site/region coverage
- topology-enriched SQL views for entity, site, and region observability
- semantic counter dictionary reference model
- semantic KPI definition and formula input reference model
- CSV-driven semantic KPI loading commands
- SQL views for semantic counter projection, unmapped counters, KPI input coverage, and provisional KPI visibility
- KPI-ready semantic base input aggregation layer
- vendor indicator semantic dictionary and lineage reference layer
- verified vendor indicator seed loading and inspection workflow
- verified KPI reference and execution slices for:
  - PRB
  - BLER
  - direct-mapped RRC
- topology-aware verified KPI site/time rollup views for PRB, BLER, and RRC
- topology-aware verified KPI site/time validation views for PRB, BLER, and RRC
- topology-aware verified KPI region/time rollup views for PRB, BLER, and RRC
- topology-aware verified KPI region/time validation views for PRB, BLER, and RRC
- CLI inspection commands for verified KPI outputs at entity/site/region grains
- CLI validation commands for the verified KPI stack at entity/site/region grains
- FastAPI API layer for health, ingestion, topology, KPI results, KPI validation, and manual operator actions
- minimal operator UI for:
  - Overview
  - Ingestion
  - KPI Results
  - Validation
  - Topology
- entity-time KPI Results stabilization with guardrails, date-bound normalization, and offset paging
- entity-time KPI Validation stabilization for PRB and BLER using fast backend paths
- local topology reference seeding from `LTE Project Parameter-20260301.xlsx`
- materially mapped local topology enrichment after loading topology references and rerunning `sync-topology`
- performant operator-facing PRB and BLER `site-time` / `region-time` API paths using direct fast paths over narrowed raw facts and topology enrichment
- workbook-driven topology snapshot, reconciliation, and guarded activation baseline through the API/UI
- backward-compatible multi-directory FTP source support for explicit PM directory scanning
- persistent FTP cycle run tracking with background execution and operator-visible run state
- read-only source-interval discovery over the existing FTP registry (`ftp_remote_file`)
- operator-facing Source intervals panel on the Ingestion page
- interval-triggered FTP execution for one selected 15-minute source interval using `interval_start`
- normalization of `interval_start` into the existing 15-minute `start` / `end` execution window
- operator UI integration for interval-triggered runs on top of the existing Ingestion page
- interval-level quality visibility showing families present, missing required families, conservative partial-interval status, and interval-scoped topology coverage

### Partially Implemented

#### Verified KPI activation and rollout

Remaining work:

- broader verified KPI families beyond PRB, BLER, and the current direct-mapped RRC slice
- bundled RRC accessibility KPI review
- throughput KPI verification remains blocked pending authoritative volume-lineage evidence

#### Topology authority and topology-quality hardening

Remaining work:

- validate workbook-derived topology snapshots against CM or another authoritative source
- harden reporting hierarchy quality and parentage rules
- resolve remaining unmapped and conflicting workbook mappings conservatively
- define which topology fields remain curated versus derived

#### CM-driven topology mapping analysis

Remaining work:

- analyze available CM or inventory sources for authoritative site / region / reporting mapping
- decide which topology fields should remain curated vs derived
- define the authoritative local-dev and production mapping workflow
- keep improving confidence in site/region rollups through authority checks, not ad hoc expansion

#### Performance baseline and optimization planning

Remaining work:

- establish ingestion and query performance baselines
- profile current Python, SQL, and PostgreSQL bottlenecks
- optimize the existing stack before considering language-level rewrites
- treat Rust/Go as later options only for proven hot paths

#### FTP execution observability and operator continuity

Implemented baseline:

- `ftp_cycle_run` provides persistent run-level execution state
- `ftp_cycle_run_event` records stage-level progress and metrics
- FTP cycles are now enqueued from the API and executed in a background worker
- UI refresh/navigation no longer implies loss of run visibility
- API startup now reconciles stale prior-process `running` rows by marking them `failed` with a restart-interruption error
- the Ingestion page now exposes discovered source intervals from the FTP registry as a read-only operator view
- operators can now trigger a run for one selected discovered 15-minute interval while continuing to use `ftp_cycle_run` and `ftp_cycle_run_event` for execution state
- interval-triggered runs store `interval_start` explicitly in `parameters_json`
- interval-based ingestion aligns the operator workflow with the native PM cadence of 15-minute source intervals
- interval quality visibility is now exposed in the Ingestion page using required LTE PM families for completeness and interval-scoped topology coverage as informational context
- the existing day-range and backfill run capability remains available for manual broader runs

### Planned

- data quality framework
- monitoring / alerting hooks
- broader site/time and region/time optimization outside the current PRB/BLER operator fast paths

## 5. Ingestion Lifecycle

Primary operational path:

`FTP discovery -> registry tracking -> staged download -> staged ingest -> audit recording -> lifecycle archive or reject`

Current operator visibility layer:

`registry-backed source interval discovery -> interval quality and topology visibility in the Ingestion UI -> interval-triggered execution or existing range/backfill controls`

Convenience path:

`ftp-fetch -> internally runs the staged registry-backed flow -> preserves one-command operation for the user`

## 6. Operational Goals

- Ensure ingestion idempotency.
- Maintain strong audit tracking for every ingest operation.
- Provide full observability of remote, staged, and ingested file state.
- Enable deterministic entity identification.
- Preserve implemented cell/eNB identity relationships while expanding later to site/region enrichment.
- Allow safe recovery from partial ingestion failures.
- Ensure all transformations are reproducible via SQL.
- Keep the staged FTP flow as the primary operational path.
- Keep manual operator control ahead of scheduler-first operation.

## 7. Immediate Next Milestone

### CM-driven topology mapping analysis and topology-quality hardening

Target additions:

- validate the current workbook-derived topology snapshot workflow against CM or another authoritative source
- harden the region/site/reporting mapping model where workbook conflicts exist
- close the remaining gap between local development topology and an authoritative production-quality topology source
- retain the current API/UI/operator baseline; do not broaden to new KPI families before topology quality is established

Success condition:

- local development keeps working mapped topology through the documented workflow
- site/time and region/time operator paths remain meaningfully populated in local verification
- the CM/reference-data strategy for authoritative topology mapping is clear enough to support the next stabilization step
- persistent FTP run visibility remains stable while ingestion work moves off the request lifecycle
- the read-only source-interval layer remains aligned with the existing staged FTP flow and manual operator control

### Near-term ingestion operator enhancement

Target additions:

- refine interval-level summaries without changing the current conservative completeness model
- improve operator-facing notes around partial intervals and topology evidence when needed
- keep execution state in the existing persistent FTP run and event model
- retain the current day-range and backfill flow for manual broader runs

Success condition:

- operators can review discovered source intervals before selecting work
- interval-triggered runs continue to fit cleanly into the existing staged FTP flow
- interval rows continue to provide clear factual summaries for operator decision-making
- run details continue to be exposed through the current persistent run/event UI and API
- broader range/backfill execution remains available and unchanged

### Near-term milestone after topology reference-data completion

#### Performance baseline and optimization planning

Target additions:

- establish ingestion performance baselines
- establish query and KPI-inspection baseline timings
- profile the current Python/SQL/PostgreSQL stack
- optimize existing bottlenecks before introducing new technology choices

Success condition:

- the platform has a measured baseline for ingestion and read/query performance
- the main bottlenecks are known and prioritized
- optimization work is driven by evidence rather than assumed hotspots

## 8. Long-Term Vision

This project demonstrates a realistic telecom data platform architecture.

It is intentionally focused on:

- clear ingestion governance
- strong SQL observability
- modular data engineering practices
- staged and recoverable acquisition flow

The design can later expand to support:

- multi-vendor PM data
- multi-technology datasets
- broader API-based access
- distributed ingestion pipelines
