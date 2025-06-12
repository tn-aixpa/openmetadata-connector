COMPOSE_FILE ?= docker/docker-compose-postgres-connector-1-5-3.yml

run:
	docker compose -f $(COMPOSE_FILE) down -v && docker compose -f $(COMPOSE_FILE) up --build
