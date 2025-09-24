# All services, including user code, dagster infra, and storage
ALL_SERVICES = docker compose --profile localInfra --profile production -f Docker/Docker-compose.yaml
# All services except storage like S3 and Postgres; those are specified in .env  and point to
# remote services on a cloud provider 
PRODUCTION_USING_CLOUD_INFRA = docker compose --profile production -f Docker/Docker-compose.yaml
# Storage needed for local development; userCode runs local, not in Docker
DEV_SERVICES = docker compose --profile localInfra -f Docker/Docker-compose.yaml

# Using uv, install all Python dependencies needed for local development and spin up necessary docker services
deps: init-env
	uv sync --all-groups --locked

# Run pyright to validate types, then use pytest with xdist to run tests in parallel
test: deps init-env
	uv run pyright && uv run pytest -n 20 -x --maxfail=1 -vv --durations=5

# Run static code checks
check: deps init-env
	uv run pyright && uv run ruff check && uv run dg check defs && uv run dg check yaml

# Run pytest with codecoverage
cov: deps init-env
	# Run pytest with coverage and output the results to stdout
	uv run pytest -n 20 -x --maxfail=1 -vv --durations=5 --cov

# Remove artfiacts from local dagster runs, tests, or python installs
clean:
	rm -f .coverage
	rm -f coverage.xml
	rm -rf htmlcov
	rm -rf storage
	rm -rf .logs_queue
	rm -rf .venv/

# Spin up just the services needed for local development
up: init-env
	$(DEV_SERVICES) up -d $(ARGS)

# Run dagster in dev mode using your local Python code
dev: deps up init-env
	DAGSTER_POSTGRES_HOST=0.0.0.0 uv run dg dev

# Run dagster with all Python code in the user code directory as a Docker image
prod: init-env
	$(ALL_SERVICES) up -d $(ARGS)

# Build all docker images services for production
prodBuild: init-env
	$(ALL_SERVICES) build

# Run dagster with dockerized Python code but don't run any storage locally
cloudProd: init-env
	$(PRODUCTION_USING_CLOUD_INFRA) up -d $(ARGS)

# Stop all docker services
down: 
	$(ALL_SERVICES) down

# Pull all docker images
pull:
	$(ALL_SERVICES) pull
	docker pull internetofwater/nabu:latest
	docker pull internetofwater/gleaner:latest

# Copy .env.example to .env only if .env does not exist
init-env:
	@test -f .env || cp .env.example .env

# All targets are cli commands 
.PHONY: deps test cov clean up dev prod cloudProd down pull init-env