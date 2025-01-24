devInfra:
	docker compose --profile localInfra --profile dagsterStorage -f Docker/Docker-compose.yaml up 

refresh:
	docker compose --profile localInfra --profile dagsterStorage -f Docker/Docker-compose.yaml restart geoconnex_crawler_dagster_user_code

build:
	docker compose --profile localInfra --profile dagsterStorage -f Docker/Docker-compose.yaml build

down:
	docker compose --profile localInfra --profile dagsterStorage -f Docker/Docker-compose.yaml down