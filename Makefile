# Local cluster (Podman). With Docker: make up DC="docker compose -f dev/compose.yml"
DC = podman compose -f dev/compose.yml
RESULTS ?= results/dev/dev

.PHONY: up down shell run analysis

up:
	$(DC) up -d --build

down:
	$(DC) down

shell:
	$(DC) exec master bash

# the same script as on AWS, run in the master container
run:
	$(DC) exec master bash bench/run_all.sh dev

# on the laptop: make analysis, or make analysis RESULTS=results/aws/aws
analysis:
	.venv/bin/python analysis/report.py $(RESULTS)
