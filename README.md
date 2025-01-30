# Geoconnex Scheduler

The geoconnex scheduler crawls geoconnex partner data on a schedule and uploads it to the geoconnex graph.

- it crawls data with gleaner
- it syncs data with the graphdb using nabu

## How to use

Configuration for this repository is contained in `.env`. When you run `main.py` for the first time, it will prompt you to create a new `.env` file by copying `.env.example`

In both `dev` and `prod` modes you can append either `--build` or `--detach` to pass those args to the underlying compose project.

## Development

We use `uv` for dependency management. You need to be in an environment that matches the [pinned python version](./.python-version)

- Once you are n that environment run `uv sync` to install dependencies

You need to run 2 commands from the root of the repo to get a dev environment

- `python3 main.py dev` spins up test infrastructure
  - this will spin up a test minio container for testing in addition to the standard services like graphdb, headless chrome etc
- run `python3 main.py dagster-dev` from the root of the repo to spin up dagster
  - this allows us to get hot reloading and use local debugging
  - view the UI at `localhost:3000`

For testing:

- run `pytest` from the root to execute tests
- you can start the task `dagster dev` in the vscode debug panel to run the full geoconnex pipeline with vscode breakpoints enabled

## Production

Run `python3 main.py prod` to run a priduction deployment. This will spin up everything and containerize dagster. You will need to specify your s3 container in the `.env` file.
