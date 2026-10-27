.PHONY: install lint format test run demo ask incident-demo quality lineage docker tf-validate clean

PY ?= python3
VENV ?= .venv
BIN := $(VENV)/bin

install:            ## create a virtualenv and install with dev extras
	$(PY) -m venv $(VENV)
	$(BIN)/pip install -U pip
	$(BIN)/pip install -e ".[dev]"

lint:
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .

format:
	$(BIN)/ruff check --fix .
	$(BIN)/ruff format .

test:
	$(BIN)/pytest --cov=gridlake --cov-report=term-missing

run:                ## end-to-end pipeline over the last 14 days (synthetic EIA data unless EIA_API_KEY is set)
	$(BIN)/gridlake run --days 14

demo: run           ## run, then show quality, lineage and a grounded GraphRAG context pack
	$(BIN)/gridlake quality
	$(BIN)/gridlake lineage
	$(BIN)/gridlake ask "Which balancing authority had the highest renewable share, and which table should I use?" --clearance internal --trace

ask:
	$(BIN)/gridlake ask "$(Q)" --clearance $(or $(CLEARANCE),public)

incident-demo:      ## inject a source fault (FAULT=new_fuel_type|unit_change|feed_outage), then triage it
	-$(BIN)/gridlake run --days 14 --inject-fault $(or $(FAULT),new_fuel_type)
	$(BIN)/gridlake triage --trace
	$(BIN)/gridlake incidents list

docker:
	docker build -t gridlake:local .
	docker build -f Dockerfile.lambda -t gridlake-lambda:local .

tf-validate:
	cd infra/terraform && terraform fmt -check -recursive && terraform init -backend=false -input=false && terraform validate

.PHONY: product-demo
product-demo:       ## describe a dataset; the agent drafts its contract, SQL and tests for approval
	$(BIN)/gridlake run --days 14
	$(BIN)/gridlake propose "$(or $(REQUEST),Monthly renewable share by region for the sustainability report)" --owner $(or $(OWNER),analytics@gridlake.example) --trace
	$(BIN)/gridlake products list

clean:
	rm -rf .gridlake .pytest_cache .ruff_cache .coverage
