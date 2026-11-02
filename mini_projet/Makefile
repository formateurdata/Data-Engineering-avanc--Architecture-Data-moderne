COMPOSE := docker compose -f infra/docker-compose.yml
AIRFLOW_IMAGE := mini-projet-airflow:3.3.2
POSTGRES_HOST ?= localhost
POSTGRES_PORT ?= 15432
POSTGRES_DB ?= boutique
POSTGRES_USER ?= admin
POSTGRES_PASSWORD ?= password

.PHONY: up down reset register-cdc register-connector logs generate reconcile verify

up:
	docker build --file infra/Dockerfile.airflow --tag $(AIRFLOW_IMAGE) .
	$(COMPOSE) up --wait --wait-timeout 180 -d

down:
	$(COMPOSE) down

reset:
	@printf 'Cette opération supprimera tous les volumes du mini-projet. Tapez yes pour confirmer : '; read answer; test "$$answer" = yes
	$(COMPOSE) down -v

register-cdc:
	$(COMPOSE) up --force-recreate debezium-init

register-connector: register-cdc

logs:
	$(COMPOSE) logs -f

generate:
	POSTGRES_HOST=$(POSTGRES_HOST) POSTGRES_PORT=$(POSTGRES_PORT) POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) uv run scripts/generator.py

reconcile:
	uv run scripts/reconcile_silver.py

verify:
	POSTGRES_HOST=$(POSTGRES_HOST) POSTGRES_PORT=$(POSTGRES_PORT) POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) uv run scripts/verify.py
