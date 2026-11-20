# Every command runs on the laptop. ENV=dev (default): local cluster in containers.
# ENV=aws: EC2 cluster. Same commands for both:
#   make up         start the cluster
#   make run        whole study (bench/run_all.sh on the master)
#   make status     progress
#   make fetch      copy the results to the laptop
#   make analysis   figures and tables -> analysis/figures/<env>/
#   make shell      shell on the master
#   make down       stop the cluster (aws: delete it)
ENV ?= dev

DC = podman compose -f dev/compose.yml
KEY = $(HOME)/.ssh/cloud-bigdata-project
MASTER_IP = $(shell terraform -chdir=infra/terraform output -raw master_public_ip 2>/dev/null)
SSH = ssh -i $(KEY) -o StrictHostKeyChecking=accept-new ubuntu@$(MASTER_IP)
# Terraform with my current public IP (the only address allowed to SSH into the nodes)
TF = TF_VAR_my_ip_cidr=$$(curl -s https://checkip.amazonaws.com)/32 terraform -chdir=infra/terraform

.PHONY: up run status fetch analysis shell down cluster-ip

ifeq ($(ENV),aws)

up: $(KEY)
	$(TF) init
	$(TF) apply
	cd infra/ansible && ansible-playbook cluster.yml

# in tmux on the master, so it keeps running when the laptop is off
run: cluster-ip
	$(SSH) "tmux new -d -s exp 'bash -lc \"cd /opt/work && mkdir -p results/aws && bash bench/run_all.sh aws 2>&1 | tee -a results/aws/run.log\"'"
	@echo "started on the master. Follow with: make status ENV=aws"

status: cluster-ip
	$(SSH) "tmux ls; tail -n 5 /opt/work/results/aws/run.log; echo runs ok: \$$(grep -c ',ok,' /opt/work/results/aws/runs.csv)"

fetch: cluster-ip
	mkdir -p results/aws
	rsync -az -e "ssh -i $(KEY)" ubuntu@$(MASTER_IP):/opt/work/results/aws/ results/aws/

shell: cluster-ip
	$(SSH) -L 8080:master:8080 -L 18080:master:18080 -L 9870:master:9870

down:
	$(TF) destroy

cluster-ip:
	@test -n "$(MASTER_IP)" || { echo "no EC2 cluster: run make up ENV=aws first"; exit 1; }

$(KEY):
	mkdir -p $(HOME)/.ssh
	ssh-keygen -t ed25519 -N "" -C cloud-bigdata-project -f $(KEY)

else

up:
	$(DC) up -d --build

run:
	$(DC) exec master bash bench/run_all.sh dev

status:
	@echo "runs ok: $$(grep -c ',ok,' results/dev/runs.csv 2>/dev/null || echo 0)"

fetch:
	@echo "nothing to do: the local cluster writes directly to results/dev"

shell:
	$(DC) exec master bash

down:
	$(DC) down

endif

analysis: .venv/bin/python
	.venv/bin/python analysis/report.py results/$(ENV)

# Python environment for the figures, created once
.venv/bin/python:
	/usr/bin/python3 -m venv .venv
	.venv/bin/pip install -q -r analysis/requirements.txt
