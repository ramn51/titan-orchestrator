# Two-VM cloud bundle

Package from Windows PowerShell:

```powershell
.\package_cloud.ps1 -SkipBuild
```

Package from Linux, macOS, or another Unix system:

```bash
chmod +x package_cloud.sh
./package_cloud.sh --skip-build
```

The portable implementation is `package_cloud.py` and uses only the Python
standard library. Omit the skip-build option to run Maven first.

Omit `-SkipBuild` when Maven and Java 17+ are configured and you want the script
to rebuild the engine first. Bundles are written to `dist/cloud/`.

Install prerequisites on both Debian VMs:

```bash
sudo apt update
sudo apt install -y default-jre-headless python3 python3-venv python3-pip unzip
```

On the Master VM:

```bash
unzip titan-master-bundle.zip
cd titan-master-bundle
chmod +x start_master.sh
./start_master.sh
```

The dashboard stays private on `127.0.0.1:5000` by default. The safest access
method is an SSH tunnel. To opt into direct browser access, first restrict the
GCP TCP 5000 firewall rule to your current public IP as a `/32`, then start with:

```bash
TITAN_DASHBOARD_HOST=0.0.0.0 ./start_master.sh
```

Open `http://MASTER_EXTERNAL_IP:5000/`. Never expose this unauthenticated
job-control interface to `0.0.0.0/0`.

On the Worker VM, use the Master's private VPC address:

```bash
unzip titan-worker-bundle.zip
cd titan-worker-bundle
chmod +x start_worker.sh
./start_worker.sh MASTER_INTERNAL_IP 8080 GENERAL true
```

The same bundles run on Windows PowerShell:

```powershell
# Master machine
.\start_master.ps1

# Worker machine
.\start_worker.ps1 -MasterHost MASTER_INTERNAL_IP -WorkerPort 8080 -Capability GENERAL
```

The generated Master configuration disables autoscaling so the controlled
two-VM benchmark cannot silently add local workers. The generated Worker
configuration advertises two slots for a 2-vCPU VM. Edit the bundle's
`config.properties` while services are stopped to change either setting.

Manage the Master bundle with:

```bash
./start_master.sh status
./start_master.sh stop
```
