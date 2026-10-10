# Two-VM cloud bundle

Package from Windows PowerShell:

```powershell
.\package_cloud.ps1 -SkipBuild
```

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

On the Worker VM, use the Master's private VPC address:

```bash
unzip titan-worker-bundle.zip
cd titan-worker-bundle
chmod +x start_worker.sh
./start_worker.sh MASTER_INTERNAL_IP 8080 GENERAL true
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
