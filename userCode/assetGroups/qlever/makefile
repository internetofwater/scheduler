# this makefile is solely for generating the qlever index
# for local testing; it isn't ran in the pipeline and is just a helper

index:
	qlever index --overwrite-existing && mkdir -p index && mv geoconnex.* index/

startLocal:
	cd index && qlever start --image docker.io/adfreiburg/qlever:commit-55c05d4 --name geoconnex --description graph --port 8888

stopLocal:
	qlever stop

restartLocal: stop start
	echo "Restarting"


startDocker:
	docker compose up -d && qlever ui --host-name localhost

stopDocker:
	docker compose down && qlever ui --stop