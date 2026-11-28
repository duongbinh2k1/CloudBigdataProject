KEY = $(HOME)/.ssh/cloud-bigdata-project
MASTER = $(shell terraform -chdir=infra/terraform output -raw master_public_ip)
SSH = ssh -i $(KEY) -o StrictHostKeyChecking=accept-new ubuntu@$(MASTER)
MY_IP = $(shell curl -s https://checkip.amazonaws.com)

.PHONY: up run status fetch shell down analysis

# create the machines and install hadoop + spark
up: $(KEY)
	terraform -chdir=infra/terraform init
	terraform -chdir=infra/terraform apply -var my_ip_cidr=$(MY_IP)/32
	cd infra/ansible && ansible-playbook cluster.yml

# run all the experiments on the master (in tmux)
run:
	$(SSH) "mkdir -p /opt/work/results && tmux new -d -s exp 'bash -l /opt/work/bench/run_all.sh 2>&1 | tee -a /opt/work/results/run.log'"

status:
	$(SSH) "tail -n 5 /opt/work/results/run.log; grep -c ',ok,' /opt/work/results/runs.csv"

# get the results from the master
fetch:
	rsync -az -e "ssh -i $(KEY)" ubuntu@$(MASTER):/opt/work/results/ results/

# ssh to the master (web UIs on localhost:9870, 8080, 18080)
shell:
	$(SSH) -L 9870:master:9870 -L 8080:master:8080 -L 18080:master:18080

# delete the machines
down:
	terraform -chdir=infra/terraform destroy -var my_ip_cidr=$(MY_IP)/32

# figures and tables
analysis: .venv/bin/python
	.venv/bin/python analysis/report.py

.venv/bin/python:
	python3 -m venv .venv
	.venv/bin/pip install -q -r analysis/requirements.txt

$(KEY):
	ssh-keygen -t ed25519 -N "" -f $(KEY)
