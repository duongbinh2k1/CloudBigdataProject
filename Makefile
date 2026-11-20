# Every command runs on the laptop.
#   make up / run / analysis / down              local cluster (containers)
#   make up / run / status / fetch / analysis / down ENV=aws    EC2 cluster
ENV ?= dev

DC = podman compose -f dev/compose.yml
KEY = ~/.ssh/cloud-bigdata-project
MASTER_IP = $(shell terraform -chdir=infra/terraform output -raw master_public_ip 2>/dev/null)
SSH = ssh -i $(KEY) -o StrictHostKeyChecking=accept-new ubuntu@$(MASTER_IP)

.PHONY: up run status fetch analysis down shell cluster-ip

ifeq ($(ENV),aws)

cluster-ip:
	@test -n "$(MASTER_IP)" || { echo "no EC2 cluster: run make up ENV=aws first"; exit 1; }

up:
	terraform -chdir=infra/terraform init
	terraform -chdir=infra/terraform apply
	cd infra/ansible && ansible-playbook cluster.yml

# in tmux on the master, so it keeps running when the laptop is off
run: cluster-ip
	$(SSH) "tmux new -d -s exp 'bash -lc \"cd /opt/work && mkdir -p results/aws && bash bench/run_all.sh aws 2>&1 | tee -a results/aws/run.log\"'"
	@echo "started on the master. Follow with: make status ENV=aws"

status: cluster-ip
	$(SSH) "tmux ls; tail -n 5 /opt/work/results/aws/run.log; grep -c ',ok,' /opt/work/results/aws/runs.csv"

fetch: cluster-ip
	mkdir -p results/aws
	rsync -az -e "ssh -i $(KEY)" ubuntu@$(MASTER_IP):/opt/work/results/aws/ results/aws/

shell: cluster-ip
	$(SSH) -L 8080:master:8080 -L 18080:master:18080 -L 9870:master:9870

down:
	terraform -chdir=infra/terraform destroy

else

up:
	$(DC) up -d --build

run:
	$(DC) exec master bash bench/run_all.sh dev

shell:
	$(DC) exec master bash

down:
	$(DC) down

endif

# on the laptop, from the results of run (and fetch for aws)
analysis:
	.venv/bin/python analysis/report.py results/$(ENV)
