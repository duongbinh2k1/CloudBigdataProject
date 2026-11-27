"""Architecture diagram with a single AWS Cloud frame, with the official AWS Architecture Icons (aws-icons/).
    python3 report/figures/make_architecture.py   ->  report/figures/architecture.svg
"""
import re
from pathlib import Path

HERE = Path(__file__).parent
W, H = 1170, 500
parts = []


def icon(name, x, y, size):
    svg = (HERE / "aws-icons" / f"{name}.svg").read_text()
    svg = re.sub(r"<\?xml[^>]*>|<title>.*?</title>|id=\"[^\"]*\"", "", svg)
    svg = re.sub(r'width="[^"]*" height="[^"]*"', f'x="{x}" y="{y}" width="{size}" height="{size}"', svg, count=1)
    parts.append(svg)


def text(x, y, s, size=12, weight="normal", color="#232F3E", anchor="start"):
    parts.append(f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{color}" '
                 f'text-anchor="{anchor}">{s}</text>')


def arrow(x1, y1, x2, y2, both=False):
    start = ' marker-start="url(#arrow)"' if both else ""
    parts.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="#545B64" stroke-width="1" '
                 f'marker-end="url(#arrow)"{start}/>')


# --- laptop (its "Laptop" row is at the height of the master, so that the SSH line is straight)
parts.append('<rect x="10" y="50" width="205" height="250" fill="#FAFAFA" stroke="#7D8998" stroke-width="1"/>')
text(20, 71, "Operator laptop", 13, "bold", "#7D8998")
icon("Res_Client_48_Light", 26, 76, 44)
text(80, 94, "Laptop", 13, "bold")
text(80, 111, "make up / run / fetch / down", 10.5)
icon("Res_Toolkit_48_Light", 26, 158, 44)
text(80, 176, "Terraform: machines", 11)
text(80, 193, "Ansible: installation", 11)
icon("Res_Documents_48_Light", 26, 232, 44)
text(80, 250, "results/ (rsync)", 11)
text(80, 267, "report.py: figures", 11)

# --- everything inside AWS moves 50 px right, to leave room for the lines from the laptop
parts.append('<g transform="translate(50,0)">')
# --- the only frame: AWS Cloud
parts.append('<rect x="250" y="10" width="860" height="480" fill="none" stroke="#232F3E" stroke-width="1.2"/>')
icon("AWS-Cloud-logo_32", 250, 10, 32)
text(290, 31, "AWS Cloud — Sydney (ap-southeast-2) · 8 × m7i-flex.large (2 vCPU, 8 GB)", 13, "bold")

# laptop -> internet gateway (on the border of the AWS Cloud) -> master: straight lines only
parts.append('<rect x="222" y="128" width="56" height="30" fill="#FFFFFF"/>')
icon("Res_Amazon-VPC_Internet-Gateway_48", 226, 74, 48)
text(250, 140, "Internet", 10.5, anchor="middle")
text(250, 153, "gateway", 10.5, anchor="middle")
arrow(276, 98, 388, 98)
text(332, 90, "SSH 22", 10.5, "bold", "#545B64", anchor="middle")
text(258, 259, "AWS API (Terraform)", 10.5, "bold", "#545B64")

# --- master
icon("Arch_Amazon-EC2_48", 390, 70, 56)
icon("Arch_Amazon-Elastic-Block-Store_48", 452, 70, 56)
text(449, 145, "master", 13, "bold", anchor="middle")
text(449, 161, "m7i-flex.large · EBS gp3 100 GB", 10.5, anchor="middle")
parts.append('<rect x="560" y="72" width="300" height="80" rx="6" fill="#FFFFFF" stroke="#ED7100"/>')
for i, line in enumerate(["HDFS NameNode :9000", "Spark Master :7077", "Spark History Server :18080"]):
    text(576, 95 + 22 * i, line, 12.5)

# --- master <-> workers
arrow(449, 170, 449, 238, both=True)
parts.append('<line x1="400" y1="240" x2="968" y2="240" stroke="#545B64" stroke-width="1"/>')
for k in range(7):
    arrow(400 + 94.6 * k, 240, 400 + 94.6 * k, 268)
text(468, 196, "↓ SSH: start DataNode / Worker, drop page cache, start / stop sar", 11, color="#545B64")
text(468, 214, "↕ register + heartbeat (hdfs://master:9000, spark://master:7077), tasks and results", 11,
     color="#545B64")

# --- 7 workers
for k in range(7):
    x = 400 + 94.6 * k
    icon("Arch_Amazon-EC2_48", x - 24, 270, 48)
    text(x, 334, f"worker{k + 1}", 11.5, "bold", anchor="middle")
text(684, 360, "each worker: HDFS DataNode · Spark executor (2 cores, 3 GB)", 11.5, anchor="middle")

# --- worker <-> worker
parts.append('<path d="M376,385 L376,402 L992,402 L992,385" fill="none" stroke="#2A6FD6" stroke-width="1.2"/>')
for k in range(1, 6):
    parts.append(f'<line x1="{400 + 94.6 * k}" y1="385" x2="{400 + 94.6 * k}" y2="402" stroke="#2A6FD6" '
                 'stroke-width="1.2"/>')
text(684, 424, "worker ↔ worker: shuffle · HDFS", 11.5, "bold", "#2A6FD6", anchor="middle")
text(684, 462, "Firewall: SSH only from the operator's IP; all traffic allowed between the 8 nodes (same network, one availability zone)",
     10.5, color="#545B64", anchor="middle")

parts.append("</g>")
# lines from the laptop (outside the translated group)
arrow(215, 98, 274, 98)
arrow(215, 255, 298, 255)

svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="Helvetica, Arial, sans-serif">
<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="9" markerHeight="9" orient="auto-start-reverse">
<path d="M0,0 L10,5 L0,10 z" fill="#545B64"/></marker></defs>
<rect width="{W}" height="{H}" fill="#FFFFFF"/>
{chr(10).join(parts)}
</svg>
"""
(HERE / "architecture.svg").write_text(svg)
print("written", HERE / "architecture.svg")
