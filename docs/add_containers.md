# Containers for Dagster Scheduler

## Things to work on

- secrets
- modify compose to use a project variable
- network modify to use passed evn for network names

Future:

- can we use a volume. Use git to pull?

## Build Docker

Needs to be automated with a workflow

something about archive file

## add Stack.

dagster/deployment/compose.yaml make usre that it is the correct version.
image: docker.io/fils/dagster_eco:0.0.44

NOTE: we can use a env ${project} varibale like we do for geocodes\_..\_named.yaml
