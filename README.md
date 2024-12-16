# How to use this repository

- `python3 main.py local`
- Navigate to `localhost:3000` to view the dagster UI

Tear down the docker swarm stack with `python3 main.py down`

As you develop, run `python3 main.py refresh` to rebuild the user code image and `python3 main.py test` to run tests inside the container.

## Architecture

- an .env file is sourced for all secrets and configuration for dagster
- these configs and env vars are used inside the user code Docker container
- containers are built and orchestrated with db resources inside a docker swarm
