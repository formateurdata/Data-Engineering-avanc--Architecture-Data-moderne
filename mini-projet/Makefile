# Le dossier parent peut contenir un caractère accentué écrit sous deux formes Unicode.
# Compose le lit dans PWD et peut recréer les services inutilement ; on laisse Make
# utiliser le chemin réel du disque.
unexport PWD

COMPOSE := docker compose -f infra/docker-compose.yml
# Images publiées pour les étudiants. Elles existent pour linux/amd64 (Windows/WSL)
# et linux/arm64 (Mac Apple Silicon).
AIRFLOW_IMAGE ?= ghcr.io/formateurdata/mini-projet-airflow:3.3.2
GARAGE_INIT_IMAGE ?= ghcr.io/formateurdata/mini-projet-garage-init:2.5.0
export AIRFLOW_IMAGE GARAGE_INIT_IMAGE
POSTGRES_HOST ?= localhost
POSTGRES_PORT ?= 15432
POSTGRES_DB ?= boutique
POSTGRES_USER ?= admin
POSTGRES_PASSWORD ?= password

.PHONY: up build-images down reset register-cdc logs generate

up:
	$(COMPOSE) up --pull always --remove-orphans --wait --wait-timeout 180 -d

# Réservé à la maintenance des images. Les étudiants utilisent simplement `make up`.
build-images:
	docker build --file infra/Dockerfile.airflow --tag $(AIRFLOW_IMAGE) infra
	docker build --file infra/Dockerfile.garage-init --tag $(GARAGE_INIT_IMAGE) infra

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
