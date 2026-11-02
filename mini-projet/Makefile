# Le dossier parent peut contenir un caractère accentué écrit sous deux formes Unicode.
# Compose le lit dans PWD et peut recréer les services inutilement ; on laisse Make
# utiliser le chemin réel du disque.
unexport PWD

COMPOSE := docker compose -f infra/docker-compose.yml
POSTGRES_HOST ?= localhost
POSTGRES_PORT ?= 15432
POSTGRES_DB ?= boutique
POSTGRES_USER ?= admin
POSTGRES_PASSWORD ?= password

.PHONY: up down reset register-cdc logs generate

up:
	$(COMPOSE) up --build --remove-orphans --wait --wait-timeout 180 -d

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
