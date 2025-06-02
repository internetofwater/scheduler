# Geoconnex Scheduler

[![codecov](https://codecov.io/gh/internetofwater/scheduler/graph/badge.svg?token=Krxwoeq7kR)](https://codecov.io/gh/internetofwater/scheduler)

The [Geoconnex](https://docs.geoconnex.us/) scheduler crawls water data metadata on a schedule and synchronizes with the graph database.

- it crawls sitemaps with [`nabu harvest`](https://github.com/internetofwater/nabu/) and downloads it to an S3 bucket
- it syncs data between the S3 bucket and the graphdb using [`nabu sync`](https://github.com/internetofwater/nabu/)

For more information about the Geoconnex project generally and how it aims to improve water data infrastructure, see the [Geoconnex docs](https://docs.geoconnex.us/).

## Local / Development Quickstart

> [!IMPORTANT]  
> You must have [`uv`](https://docs.astral.sh/uv/) installed for package management

Install dependencies and spin up necessary Docker services:

```sh
make deps && python3 main.py dev --detach && python3 main.py dagster-dev
```

Then go to [localhost:3000](http://localhost:3000)

_(You can also just run `make dev` for brevity)_

## Dockerized / Production Quickstart

Spin up everything and containerize dagster. _(You will need to specify your db/s3 endpont and any other remote services in the `.env` file)_

```sh
python3 main.py prod
```

You can specify extra flags to build the containers or use a local db/s3 container _(make sure to set the `DAGSTER_POSTGRES_HOST` env var to `dagster_postgres`)_

```sh
python3 main.py prod --local-services --build --detach
```

_All cloud deployment and infrastructure as code work is contained within the [harvest.geoconnex.us](https://github.com/internetofwater/harvest.geoconnex.us) repo_

## Configuration

- All env vars must be defined in `.env` at the root of the repo
  - When you run `main.py` for the first time, it will prompt you to create a new `.env` file by copying the [`.env.example`](./.env.example)
- In both `dev` and `prod` modes you can append either `--build` or `--detach` to pass those args to the underlying compose project.

## Testing

- Spin up the [local dev environment](#local-quickstart)
- Run `make test` from the root to execute tests
- If you use VSCode, run the task `dagster dev` in the debug panel to run the full pipeline with the ability to set breakpoints

## Licensing

This repository is a heavily modified version of [gleanerio/scheduler](https://github.com/gleanerio/scheduler) and is licensed under Apache 2.0
