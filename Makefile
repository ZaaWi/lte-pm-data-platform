PYTHON ?= python
ZIP ?= data/input/sample.zip
LIMIT ?= 10
KPI ?= lte_prb_utilization
CSV ?= data/reference/counter_reference_seed_template.csv
EXPECTED ?= 10251

.PHONY: up down init-db load-sample query-sample list-counters top-counters list-counter-reference show-counter load-counter-reference summarize-files summarize-source-files summarize-time summarize-ani summarize-dataset-family summarize-coverage compare-expected-cells list-kpis summarize-kpi ftp-list ftp-fetch test check
.PHONY: summarize-coverage-timeline compare-expected-cells-timeline reconcile-ingest-files backfill-lifecycle-status

up:
	docker compose up -d postgres

down:
	docker compose down

init-db:
	$(PYTHON) -m lte_pm_platform.cli init-db

load-sample:
	$(PYTHON) -m lte_pm_platform.cli load-sample --zip $(ZIP)

query-sample:
	$(PYTHON) -m lte_pm_platform.cli query-sample --limit $(LIMIT)

list-counters:
	$(PYTHON) -m lte_pm_platform.cli list-counters --limit $(LIMIT)

top-counters:
	$(PYTHON) -m lte_pm_platform.cli top-counters --limit $(LIMIT)

list-counter-reference:
	$(PYTHON) -m lte_pm_platform.cli list-counter-reference --limit $(LIMIT)

show-counter:
	$(PYTHON) -m lte_pm_platform.cli show-counter --id $(COUNTER_ID)

load-counter-reference:
	$(PYTHON) -m lte_pm_platform.cli load-counter-reference --csv $(CSV)

summarize-files:
	$(PYTHON) -m lte_pm_platform.cli summarize-files --limit $(LIMIT)

summarize-source-files:
	$(PYTHON) -m lte_pm_platform.cli summarize-source-files --limit $(LIMIT)

summarize-time:
	$(PYTHON) -m lte_pm_platform.cli summarize-time --limit $(LIMIT)

summarize-ani:
	$(PYTHON) -m lte_pm_platform.cli summarize-ani --limit $(LIMIT)

summarize-dataset-family:
	$(PYTHON) -m lte_pm_platform.cli summarize-dataset-family --limit $(LIMIT)

summarize-coverage:
	$(PYTHON) -m lte_pm_platform.cli summarize-coverage --limit $(LIMIT)

compare-expected-cells:
	$(PYTHON) -m lte_pm_platform.cli compare-expected-cells --expected $(EXPECTED)

summarize-coverage-timeline:
	$(PYTHON) -m lte_pm_platform.cli summarize-coverage-timeline --limit $(LIMIT)

compare-expected-cells-timeline:
	$(PYTHON) -m lte_pm_platform.cli compare-expected-cells-timeline --expected $(EXPECTED) --limit $(LIMIT)

reconcile-ingest-files:
	$(PYTHON) -m lte_pm_platform.cli reconcile-ingest-files --limit $(LIMIT)

backfill-lifecycle-status:
	$(PYTHON) -m lte_pm_platform.cli backfill-lifecycle-status --limit $(LIMIT)

list-kpis:
	$(PYTHON) -m lte_pm_platform.cli list-kpis --limit $(LIMIT)

summarize-kpi:
	$(PYTHON) -m lte_pm_platform.cli summarize-kpi --name $(KPI) --limit $(LIMIT)

ftp-list:
	$(PYTHON) -m lte_pm_platform.cli ftp-list

ftp-fetch:
	$(PYTHON) -m lte_pm_platform.cli ftp-fetch --limit $(LIMIT)

test:
	pytest

check:
	ruff check .
	pytest
