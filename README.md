# How to use this repository

1. `python3 -m venv .venv`
2. `source .venv/bin/activate`
3. `pip install -r requirements.txt`
4. `python3 main.py all`
5. Navigate to `localhost:3000` to view the dagster UI

You can also quickstart the venv by running `./run.sh`

## Architecture

- run.sh is the entrypoint which calls the following
- `main.py` is a cli which templates a gleaner and nabu config (so we crawl everything in the sitemap) and outputs them in the build dir
- an .env file is sourced for all secrets and configuration for dagster
- these configs and env vars are used inside the user code Docker container
- containers are built and orchestrated with db resources inside a docker swarm
