# Every command runs on the laptop:  make up | run | status | fetch | shell | down | analysis
KEY = $(HOME)/.ssh/cloud-bigdata-project
MASTER = $(shell terraform -chdir=infra/terraform output -raw master_public_ip 2>/dev/null)
SSH = ssh -i $(KEY) -o StrictHostKeyChecking=accept-new ubuntu@$(MASTER)
# Terraform, with my current public IP: the only address allowed to SSH into the machines
TF = TF_VAR_my_ip_cidr=$$(curl -s https://checkip.amazonaws.com)/32 terraform -chdir=infra/terraform

.PHONY: up run status fetch shell down analysis

# create the 8 EC2 machines (Terraform), then install and start HDFS + Spark (Ansible)
up: $(KEY)
	$(TF) init
	$(TF) apply
	cd infra/ansible && ansible-playbook cluster.yml

# the whole study on the master, inside tmux: it keeps running when the laptop is off
run:
	$(SSH) "mkdir -p /opt/work/results && tmux new -d -s exp 'bash -lc \"bash /opt/work/bench/run_all.sh 2>&1 | tee -a /opt/work/results/run.log\"'"
	@echo "started. Follow with: make status"

status:
	@$(SSH) "tail -n 5 /opt/work/results/run.log; echo runs ok: \$$(grep -c ',ok,' /opt/work/results/runs.csv 2>/dev/null)"

# copy the results of the master to results/ on the laptop
fetch:
	rsync -az -e "ssh -i $(KEY)" ubuntu@$(MASTER):/opt/work/results/ results/

# shell on the master + web interfaces on localhost: HDFS :9870, Spark :8080, History Server :18080
shell:
	$(SSH) -L 9870:master:9870 -L 8080:master:8080 -L 18080:master:18080

# delete every AWS resource (machines, disks, network)
down:
	$(TF) destroy

# figures and tables from results/ (Python virtual environment created the first time)
analysis: .venv/bin/python
	.venv/bin/python analysis/report.py

.venv/bin/python:
	python3 -m venv .venv
	.venv/bin/pip install -q -r analysis/requirements.txt

$(KEY):
	ssh-keygen -t ed25519 -N "" -C cloud-bigdata-project -f $(KEY)
