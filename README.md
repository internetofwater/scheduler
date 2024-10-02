# How to use this repository

- `python3 main.py up`
- Navigate to `localhost:3000` to view the dagster UI

Tear down the docker swarm stack with `python3 main.py down`

## Architecture

- an .env file is sourced for all secrets and configuration for dagster
- these configs and env vars are used inside the user code Docker container
- containers are built and orchestrated with db resources inside a docker swarm
