COMPOSE_FILES = docker-compose-lakehouse.yml docker-compose-spark.yml docker-compose.yml

all-up:
	docker compose $(foreach f,$(COMPOSE_FILES),-f $(f)) up -d

all-down:
	docker compose $(foreach f,$(COMPOSE_FILES),-f $(f)) down

lakehouse-up:
	docker compose -f docker-compose-lakehouse.yml up -d

lakehouse-down:
	docker compose -f docker-compose-lakehouse.yml down

spark-up:
	docker compose -f docker-compose-spark.yml up -d

spark-down:
	docker compose -f docker-compose-spark.yml down

airflow-up:
	docker compose -f docker-compose.yml up -d airflow airflow-db

airflow-down:
	docker compose -f docker-compose.yml down airflow airflow-db

mlflow-up:
	docker compose -f docker-compose.yml up -d mlflow mlflow-db

mlflow-down:
	docker compose -f docker-compose.yml down mlflow mlflow-db

model-up:
	docker compose -f docker-compose.yml up -d model 

model-down:
	docker compose -f docker-compose.yml down model

api-up:
	docker compose -f docker-compose.yml up -d api

api-down:
	docker compose -f docker-compose.yml down api

ui-up:
	docker compose -f docker-compose.yml up -d ui

ui-down:
	docker compose -f docker-compose.yml down ui
	
spark-bash:
	docker exec -it spark-master bash

model-bash:
	docker exec -it model bash

api-bash:
	docker exec -it api bash

ui-bash:
	docker exec -it ui bash

trino-init:
	docker exec -it trino bash -c "trino --server localhost:8080 --catalog iceberg --file /init/lakehouse_init.sql"

airflow-ssh-spark:
	docker exec -it airflow bash -c "sshpass -p 'manhtdxxx' ssh-copy-id -o StrictHostKeyChecking=no -i /home/airflow/.ssh/id_ecdsa.pub manh@spark-master"

airflow-ssh-model:
	docker exec -it airflow bash -c "sshpass -p 'manhtdxxx' ssh-copy-id -o StrictHostKeyChecking=no -i /home/airflow/.ssh/id_ecdsa.pub manh@model"