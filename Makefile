# Every command runs on the laptop:  make up | run | status | fetch | shell | down | analysis | report
KEY = $(HOME)/.ssh/cloud-bigdata-project
MASTER = $(shell terraform -chdir=infra/terraform output -raw master_public_ip)
SSH = ssh -i $(KEY) -o StrictHostKeyChecking=accept-new ubuntu@$(MASTER)
# my current public IP: the only address allowed to SSH into the machines
MY_IP = $(shell curl -s https://checkip.amazonaws.com)

.PHONY: up run status fetch shell down analysis report

# create the 8 EC2 machines (Terraform), then install and start HDFS + Spark (Ansible)
up: $(KEY)
	terraform -chdir=infra/terraform init
	terraform -chdir=infra/terraform apply -var my_ip_cidr=$(MY_IP)/32
	cd infra/ansible && ansible-playbook cluster.yml

# the whole study on the master, inside tmux: it keeps running when the laptop is off
run:
	$(SSH) "mkdir -p /opt/work/results && tmux new -d -s exp 'bash -l /opt/work/bench/run_all.sh 2>&1 | tee -a /opt/work/results/run.log'"

# last lines of the log, then the number of runs done
status:
	$(SSH) "tail -n 5 /opt/work/results/run.log; grep -c ',ok,' /opt/work/results/runs.csv"

# copy the results of the master to results/ on the laptop
fetch:
	rsync -az -e "ssh -i $(KEY)" ubuntu@$(MASTER):/opt/work/results/ results/

# shell on the master + web interfaces on localhost: HDFS :9870, Spark :8080, History Server :18080
shell:
	$(SSH) -L 9870:master:9870 -L 8080:master:8080 -L 18080:master:18080

# delete every AWS resource (machines, disks, network)
down:
	terraform -chdir=infra/terraform destroy -var my_ip_cidr=$(MY_IP)/32

# figures and tables from results/ (Python virtual environment created the first time)
analysis: .venv/bin/python
	.venv/bin/python analysis/report.py

# report/REPORT.md -> report/REPORT.pdf (pandoc makes an HTML page, Chrome prints it to PDF)
report:
	cd report && pandoc REPORT.md -s --embed-resources --resource-path=. --css style.css -o REPORT.html
	cd report && "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new --print-to-pdf=REPORT.pdf --no-pdf-header-footer REPORT.html
	rm report/REPORT.html

.venv/bin/python:
	python3 -m venv .venv
	.venv/bin/pip install -q -r analysis/requirements.txt

$(KEY):
	ssh-keygen -t ed25519 -N "" -f $(KEY)
