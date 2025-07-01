# Geoconnex Scheduler

[![codecov](https://codecov.io/gh/internetofwater/scheduler/graph/badge.svg?token=Krxwoeq7kR)](https://codecov.io/gh/internetofwater/scheduler)

The [Geoconnex](https://docs.geoconnex.us/) scheduler crawls water data metadata on a schedule and synchronizes with a graph database.

- it crawls sitemaps with [`nabu harvest`](https://github.com/internetofwater/nabu/) and downloads it to an S3 bucket
- it syncs data between the S3 bucket and the graph database using [`nabu sync`](https://github.com/internetofwater/nabu/)

For more information about the Geoconnex project generally and how it aims to improve water data infrastructure, see the [Geoconnex docs](https://docs.geoconnex.us/).

## Local / Development Quickstart

> [!IMPORTANT]  
> You must have [`uv`](https://docs.astral.sh/uv/) installed for package management

Install dependencies and spin up necessary Docker services:

```sh
make deps && make dev
```

Then go to [localhost:3000](http://localhost:3000)

## Dockerized / Production Quickstart

- Spin up all services as containers including user code and local db/s3 containers _(make sure to set the `DAGSTER_POSTGRES_HOST` env var to `dagster_postgres`)_

```sh
make prod
```

Spin up user code and essential services but not storage _(You will need to specify your db/s3 endpoints and any other remote services in the `.env` file)_

```sh
make cloudProd
```

_All cloud deployment and infrastructure as code work is contained within the [harvest.geoconnex.us](https://github.com/internetofwater/harvest.geoconnex.us) repo_

## Configuration

- All env vars must be defined in `.env` at the root of the repo
- The `.env.example` file will be copied to `.env` if it does not exist

## Testing

- Spin up the [local dev environment](#local--development-quickstart)
- Run `make test` from the root to execute tests
- If you use VSCode, run the task `dagster dev` in the debug panel to run the full pipeline with the ability to set breakpoints

## Licensing

This repository is a heavily modified version of [gleanerio/scheduler](https://github.com/gleanerio/scheduler) and is licensed under Apache 2.0
