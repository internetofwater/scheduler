#!/bin/bash
set -euo pipefail

# https://dockerswarm.rocks/traefik/
helpFunction()
{
   echo "setup dagster network and volume"
   echo "Usage: $0 -e envfile  -u"
   echo -e "\t-e envfile to use"

   exit 1 # Exit script after printing help
}

# CHeck if a .env file exists, otherwise prompt the user to create one by coping a .env.example file
if [ ! -f .env ]; then
   echo "Missing .env file. Do you want to copy .env.example to .env ? (y/n)"
   read answer
   if [ "$answer" == "y" ] || [ "$answer" == "Y" ]; then
      cp .env.example .env
   else
      exit 1
   fi 
fi


# Build local artifacts
if [ ! -d .venv ]; then
    python3 -m venv .venv
fi
source .venv/bin/activate
python3 -m pip install -r requirements.txt

# reset the swarm if it exists
docker swarm leave --force || true
docker swarm init
# creates a network that we can attach to from the swarm
docker network create --driver overlay --attachable docker_example_network

envfile=".env"

if [ -f $envfile ]
  then
    echo "using " $envfile
    export $(sed  '/^[ \t]*#/d' $envfile |  sed '/^$/d' | xargs)
  else
    echo "missing environment file. pass flag, or copy and edit file"
    echo "cp .env.example .env"
    echo "OR"
    echo "cp {yourenv}.env .env"
    exit 1
fi

# Cleanup old configs before creating new ones. Always return true since we don't care if it fails due to it not existing
docker config rm gleaner 2> /dev/null || true
docker config rm nabu 2> /dev/null || true
docker config rm workspace 2> /dev/null || true

python3 main.py all


if [  "$(docker network ls  | grep ${GLEANERIO_HEADLESS_NETWORK})" ] ; then
   echo ${GLEANERIO_HEADLESS_NETWORK} network exists;
      if [ "$(docker info --format '{{.Swarm.LocalNodeState}}')" == "inactive" ]; then
        echo Network is not swarm
      else
        echo Network is swarm
      fi
else
   echo Creating network
   if [ "$(docker info --format '{{.Swarm.LocalNodeState}}')" == "inactive" ]; then
        if docker network create -d bridge --attachable ${GLEANERIO_HEADLESS_NETWORK}; then
           echo "Created network ${GLEANERIO_HEADLESS_NETWORK}"
        else
           echo "ERROR: *** Failed to create local network. "
            exit 1
        fi
   else
        if docker network create -d overlay --attachable ${GLEANERIO_HEADLESS_NETWORK}; then
          echo "Created network ${GLEANERIO_HEADLESS_NETWORK}"
        else
            echo "ERROR: *** Failed to create swarm network.  "
            exit 1
        fi
   fi

fi

#echo NOTE: Verify that the traefik_proxy network  SCOPE is swarm

if [  "$(docker volume ls  | grep ${GLEANERIO_CONFIG_VOLUME})" ] ; then
   echo ${GLEANERIO_CONFIG_VOLUME} volume exists;
else
   echo creating volume
   if `docker volume create ${GLEANERIO_CONFIG_VOLUME}`; then
      echo "Created volume ${GLEANERIO_CONFIG_VOLUME}"
   else
      echo "ERROR: *** Failed to create volume. "
      exit 1
   fi
fi


if [  "$(docker config ls  | grep ${GLEANERIO_GLEANER_CONFIG_PATH})" ] ; then
   echo ${GLEANERIO_GLEANER_CONFIG_PATH} config exists;
else
   echo creating config

      if docker config create gleaner "${GLEANERIO_GLEANER_CONFIG_PATH}"; then
         echo "Created gleaner config gleaner ${GLEANERIO_GLEANER_CONFIG_PATH}"
      else
         echo "ERROR: *** Failed to create gleaner config. "
         exit 1
      fi
fi

if [  "$(docker config ls  | grep ${GLEANERIO_NABU_CONFIG_PATH})" ] ; then
   echo ${GLEANERIO_NABU_CONFIG_PATH} config exists;
else
   echo Moving nabu config into docker

      if docker config create nabu "${GLEANERIO_NABU_CONFIG_PATH}"; then
         echo "Created gleaner config  nabu ${GLEANERIO_NABU_CONFIG_PATH}"
      else
         echo "ERROR: *** Failed to create nabu config. "
          exit 1
      fi
fi

if [  "$(docker config ls  | grep ${GLEANERIO_WORKSPACE_CONFIG_PATH})" ] ; then
   echo ${GLEANERIO_WORKSPACE_CONFIG_PATH} config exists;
else
   echo creating config

      if docker config create workspace "${GLEANERIO_WORKSPACE_CONFIG_PATH}"; then
         echo "Created config workspace ${GLEANERIO_WORKSPACE_CONFIG_PATH}"
      else
         echo "ERROR: *** Failed to create workspace config. "
          exit 1
      fi
fi

mkdir -p /tmp/io_manager_storage

docker build -t docker_example_user_code_image -f ./Docker/Dockerfile_user_code .
docker build -t docker_example_webserver_image -f ./Docker/Dockerfile_dagster .
docker build -t docker_example_daemon_image -f ./Docker/Dockerfile_dagster .

docker stack deploy -c docker-compose.yaml e2edagster --detach=false