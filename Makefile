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
	docker compose -f docker-compose.yml up -d airflow

airflow-down:
	docker compose -f docker-compose.yml down airflow

spark-bash:
	docker exec -it spark-master bash

trino-bash:
	docker exec -it trino bash

airflow-bash:
	docker exec -it airflow bash

trino-init:
	docker exec -it trino bash -c "trino --server localhost:8080 --catalog iceberg --file /init/lakehouse_init.sql"
