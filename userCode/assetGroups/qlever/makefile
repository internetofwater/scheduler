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

buildDocker:
	@test -n "$S3_ACCESS_KEY" && test -n "$S3_SECRET_KEY" || echo "S3_ACCESS_KEY and S3_SECRET_KEY must be set"
	docker build --secret id=S3_ACCESS_KEY,env=S3_ACCESS_KEY --secret id=S3_SECRET_KEY,env=S3_SECRET_KEY .

startDocker:
	docker compose up -d && qlever ui --host-name localhost

stopDocker:
	docker compose down && qlever ui --stop