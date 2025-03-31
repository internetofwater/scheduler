# Geoconnex Scheduler

The [Geoconnex](https://docs.geoconnex.us/) scheduler crawls Geoconnex partner data on a schedule and synchronizes with the Geoconnex graph database.

- it crawls data with [Gleaner](https://github.com/internetofwater/gleaner/) and downloads it to an S3 bucket
- it syncs data between the S3 bucket and the graphdb using [Nabu](https://github.com/internetofwater/nabu/)

For more information about the Geoconnex project generally and how it aims to improve water data infrastructure, see the [Geoconnex docs](https://docs.geoconnex.us/).

## How to use

Configuration for this repository is contained in `.env`. When you run `main.py` for the first time, it will prompt you to create a new `.env` file by copying `.env.example`

In both `dev` and `prod` modes you can append either `--build` or `--detach` to pass those args to the underlying compose project.

## Development

We use `uv` for dependency management. You need to be in an environment that matches the [pinned python version](./.python-version)

- Once you are in that environment run `uv sync` to install dependencies

You need to run 2 commands from the root of the repo to get a dev environment

- `python3 main.py dev` spins up test infrastructure
  - this will spin up a test minio container for testing in addition to the standard services like graphdb, headless chrome etc
- run `python3 main.py dagster-dev` from the root of the repo to spin up dagster
  - this allows us to get hot reloading and use local debugging
  - view the UI at `localhost:3000`

For testing:

- run `pytest` from the root to execute tests
- you can start the task `dagster dev` in the vscode debug panel to run the full geoconnex pipeline with vscode breakpoints enabled

You can use the helper scripts in the [Makefile](./makefile) to run pytest with a set of useful options.

## Production

- Run `python3 main.py prod` to run a production deployment. This will spin up everything and containerize dagster. You will need to specify your s3 container in the `.env` file.
- All deployment and infrastructure as code work is contained within the [harvest.geoconnex.us](https://github.com/internetofwater/harvest.geoconnex.us) repo

## Licensing

This repository is a heavily modified version of https://github.com/gleanerio/scheduler and is licensed under Apache 2.0
