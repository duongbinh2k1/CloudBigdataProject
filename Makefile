# Local cluster (Podman). With Docker: make up DC="docker compose -f dev/compose.yml"
DC = podman compose -f dev/compose.yml
RUN = $(DC) exec -w /opt/work master
SF ?= 0.1

up:
	$(DC) up -d --build

down:
	$(DC) down

shell:
	$(DC) exec master bash
