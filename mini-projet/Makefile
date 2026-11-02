# Le dossier parent peut contenir un caractère accentué écrit sous deux formes Unicode.
# Compose le lit dans PWD et peut recréer les services inutilement ; on laisse Make
# utiliser le chemin réel du disque.
unexport PWD

COMPOSE := docker compose -f infra/docker-compose.yml
AIRFLOW_IMAGE := mini-projet-airflow:3.3.2
GARAGE_INIT_IMAGE := mini-projet-garage-init:2.5.0
POSTGRES_HOST ?= localhost
POSTGRES_PORT ?= 15432
POSTGRES_DB ?= boutique
POSTGRES_USER ?= admin
POSTGRES_PASSWORD ?= password

.PHONY: up down reset register-cdc logs generate

up:
	# Docker Compose 5.6/Buildx 0.37 refuse certains contextes avec Bake ; la
	# construction directe reste portable et ne demande aucune permission spéciale.
	docker build --file infra/Dockerfile.airflow --tag $(AIRFLOW_IMAGE) infra
	docker build --file infra/Dockerfile.garage-init --tag $(GARAGE_INIT_IMAGE) infra
	$(COMPOSE) up --no-build --remove-orphans --wait --wait-timeout 180 -d

down:
	$(COMPOSE) down

reset:
	@printf 'Cette opération supprimera tous les volumes du mini-projet. Tapez yes pour confirmer : '; read answer; test "$$answer" = yes
	$(COMPOSE) down -v

register-cdc:
	$(COMPOSE) up --force-recreate debezium-init

logs:
	$(COMPOSE) logs -f

generate:
	POSTGRES_HOST=$(POSTGRES_HOST) POSTGRES_PORT=$(POSTGRES_PORT) POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) uv run scripts/generator.py
