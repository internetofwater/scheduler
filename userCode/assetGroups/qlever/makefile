# this makefile is solely for generating the qlever index
# for local testing; it isn't ran in the pipeline and is just a helper

index:
	qlever index --overwrite-existing && mkdir -p index && mv geoconnex.* index/

start:
	cd index && qlever start --image docker.io/adfreiburg/qlever:commit-55c05d4 --name geoconnex --description graph --port 8888

stop:
	qlever stop

restart: stop start
	echo "Restarting"

.PHONY: index start stop restart