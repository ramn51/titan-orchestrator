
# 📚 Python SDK Reference

The Titan Python SDK allows you to programmatically define jobs, interact with the TitanStore data bus, and manage distributed artifacts.

---

## 1. Core Classes

### `TitanClient`
The main entry point for connecting to the cluster.

```python
from titan_sdk import TitanClient

# No constructor args — host and port are configured via
# TITAN_HOST / TITAN_PORT environment variables (default: 127.0.0.1:9090)
client = TitanClient()
```

**DAG submission**

| Method | Description |
| --- | --- |
| `client.submit_job(job)` | Dispatches a single `TitanJob` to the cluster. |
| `client.submit_dag(name, jobs, agent_run_id=None)` | Submits a list of linked `TitanJob` objects as a single named DAG. Pass `agent_run_id` to link multiple DAG submissions into one logical agent run visible in the Dashboard. |
| `client.get_job_status(job_id)` | Securely queries the Master for a job's internal system status. |
| `client.fetch_logs(job_id)` | Retrieves the stdout/stderr logs for a specific job ID. |

**File transfer**

| Method | Description |
| --- | --- |
| `client.upload_file(filepath)` | Uploads a single file to the Master's `uploads/` directory. Returns `"UPLOAD_SUCCESS"` on success. |
| `client.upload_project_folder(path)` | Zips and uploads a local folder to the Master's artifact registry. |
| `client.publish_artifact(key, filename)` | **Worker-side.** Uploads `filename` to Master and registers the basename in TitanStore under `key`. Pair with `get_artifact` on the orchestrator side. |
| `client.get_artifact(key, save_path=None)` | **Orchestrator-side.** Reads the filename registered under `key`, downloads it from Master, and saves to `save_path` (defaults to `/tmp/<filename>`). Returns `True` on success. |
| `client.fetch_artifact(filename, save_path=None)` | Low-level download by filename from Master's `uploads/` directory. |
| `client.deploy_script(filepath)` | Deploys a worker script to Master's `perm_files/` directory. Returns `"DEPLOY_SUCCESS"` on success. |

**TitanStore (shared KV)**

| Method | Description |
| --- | --- |
| `client.store_put(key, value)` | Saves a string value to the distributed TitanStore. |
| `client.store_get(key)` | Retrieves a string value from the distributed TitanStore. |
| `client.store_sadd(key, member)` | Adds a member to a distributed Set. Returns `1` if new, `0` if already exists. |
| `client.store_smembers(key)` | Returns a Python list of all members in the specified Set. |

### `TitanJob`

Represents a unit of work to be executed on the cluster.

```python
from titan_sdk import TitanJob

job = TitanJob(
    job_id="train_v1",
    filename="scripts/train.py",
    requirement="GPU",     # Optional: "GPU" or "GENERAL"
    priority=10,           # Optional: Higher numbers schedule first
    parents=["data_prep"], # Optional: List of parent Job IDs
    is_archive=False       # Set True if deploying a ZIP/Service
)
```

These are the constructor parameters:

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `job_id` | `str` | **Required** | Unique identifier for this execution step. |
| `filename` | `str` | **Required** | Absolute or relative path to the script or artifact. |
| `job_type` | `str` | `"RUN_PAYLOAD"` | Defines execution mode (e.g., use `"SERVICE"` for long-running processes). |
| `args` | `str` | `None` | Command-line arguments passed to the executed script. |
| `parents` | `list` | `None` | List of parent `job_id`s that must complete successfully before this task runs. |
| `port` | `int` | `0` | Port number to bind to (Required if deploying a long-running Service). |
| `is_archive` | `bool` | `False` | Set to `True` if deploying a zipped project folder. |
| `priority` | `int` | `1` | Queue priority. Higher numbers are scheduled first. |
| `delay` | `int` | `0` | Artificial delay (in seconds/ms depending on scheduler) before execution. |
| `affinity` | `bool` | `False` | If `True`, Titan attempts to route this task to the exact same physical node as its parent task. |
| `requirement`| `str` | `"GENERAL"` | Hardware capability routing tag (e.g., `"GPU"`, `"HIGH_MEM"`). |
| `hitl_message` | `str` | `None` | When set, the SDK automatically injects a Human-in-the-Loop gate after this job. The string is shown to the operator in the Dashboard. See [HITL Pipelines](../examples/hitl.md). |
| `max_wait_seconds` | `int` | `172800` | Maximum time (in seconds) the HITL gate will wait for a human decision before auto-failing. Default is 48 hours. Only applies when `hitl_message` is set. |

---

## 2. Defining DAGs Programmatically

You can build dependency graphs using the SDK's API instead of YAML.

```python
from titan_sdk import TitanClient, TitanJob

client = TitanClient()

# Step 1: Define the Root Job (No parents)
task_a = TitanJob(
    job_id="extract_data",
    filename="etl/extract.py",
    priority=5
)

# Step 2: Define a Dependent Job
task_b = TitanJob(
    job_id="train_model",
    filename="ml/train.py",
    requirement="GPU",
    parents=["extract_data"]  # <--- Defines the dependency
)

# Step 3: Submit them as a unified DAG
client.submit_dag("nightly_pipeline", [task_a, task_b])
print("DAG Submitted!")
```

### Linking multiple DAGs into one Agent Run

For agentic workflows that submit several DAGs sequentially, pass the same `agent_run_id` to each `submit_dag` call. This links all stages into a single run entry in the Dashboard's **Agent Runs** view.

```python
import uuid
run_id = uuid.uuid4().hex[:12]

client.submit_dag("PLAN",    [planner_job],   agent_run_id=run_id)
# ... wait for planner, read results ...
client.submit_dag("EXECUTE", executor_jobs,   agent_run_id=run_id)
# ... wait, evaluate ...
client.submit_dag("SYNTH",   [synthesis_job], agent_run_id=run_id)
```

### Submitting a YAML pipeline (`submit_yaml`)

If your pipeline is already defined in a [Titan YAML file](../examples/yaml.md), `submit_yaml` parses it and submits it as a DAG — no need to build `TitanJob` objects by hand. It is also the one SDK call with a **built-in blocking wait**.

```python
result = client.submit_yaml(
    "pipeline.yaml",
    agent_run_id=None,   # optional — links this DAG to an Agent Run in the Dashboard
    wait=False,          # if True, block until every job reaches a terminal state
    poll_interval=2,     # seconds between status polls (only used when wait=True)
    timeout=300,         # max seconds to wait before giving up (only used when wait=True)
)
```

| Parameter | Default | Description |
|---|---|---|
| `yaml_path` | *(required)* | Path to a Titan YAML pipeline file. |
| `agent_run_id` | `None` | Links this DAG to a logical agent run in the Dashboard's **Agent Runs** view. |
| `wait` | `False` | If `True`, blocks until all jobs reach a terminal state (`COMPLETED`, `FAILED`, `REJECTED`, `ERROR`). |
| `poll_interval` | `2` | Seconds between status polls. Only used when `wait=True`. |
| `timeout` | `300` | Maximum seconds to wait before giving up. Only used when `wait=True`. |

**Returns** the master's submit-response string — or `False` if `wait=True` and the DAG did not finish within `timeout`.

!!! note "`wait=True` is poll-based, and blocks the calling thread"
    Under the hood this polls `get_job_status` every `poll_interval` seconds until each job is terminal — there is no async future. Because it waits for *terminal* states, an unapproved [HITL gate](../examples/hitl.md) will keep it blocking, so set `timeout` generously (e.g. `timeout=3600`) when a human decision is in the loop.

```python
# Sequential agentic loop — each stage blocks until the previous one finishes
run_id = uuid.uuid4().hex[:12]
client.submit_yaml("bootstrap.yaml",     agent_run_id=run_id, wait=True)
client.submit_yaml("arena_cycle.yaml",   agent_run_id=run_id, wait=True)
client.submit_yaml("export_deploy.yaml", agent_run_id=run_id, wait=True)
```

### Tearing down a service (`stop_service`)

A job submitted with `job_type="SERVICE"` (or `"DEPLOY_PAYLOAD"`) is **long-running** — it deploys and stays alive until you stop it. `stop_service` tears it down: the Master forwards an `OP_STOP` to the worker hosting the service, which forcibly terminates the service's process tree (and does **not** trigger the auto-restart supervisor, since the stop is intentional).

```python
client = TitanClient()

# Deploy a long-running service on port 8099
svc = TitanJob(job_id="my-api", filename="api_server.py",
               job_type="SERVICE", port=8099)
client.submit_job(svc)

# ... later, tear it down ...
resp = client.stop_service("my-api")   # -> "STOPPED: DAG-my-api"
```

- **`service_id`** is the job id you deployed under. The `DAG-` prefix is added automatically (services are keyed as `DAG-<job_id>` on the Master), so `"my-api"` and `"DAG-my-api"` are equivalent.
- **Returns** the Master's response — `"STOPPED: DAG-<id>"` on success, or an `"ERROR: ... not found"` string if the id isn't a live service.

!!! warning "Wait for the deploy to register before stopping"
    A service is only added to the Master's live registry **after** its post-deploy port verification completes — a couple of seconds *after* the port first becomes reachable. Stopping in that window returns `not found`. If you deploy and tear down programmatically in the same script, wait until the deploy job reports `COMPLETED` first:

    ```python
    client.submit_job(svc)
    while (client.get_job_status("DAG-my-api") or "").upper() != "COMPLETED":
        time.sleep(1)
    client.stop_service("my-api")
    ```

    In a DAG, model teardown as a final job that depends on the work jobs, so the graph sequences it for you.

---

## 3. Using the Distributed Data Bus (TitanStore)

Tasks running on completely different physical nodes can share state, pass intermediate variables, or track metrics using Titan's built-in persistence layer.

**File 1: `task_a.py` (Producer)**

```python
from titan_sdk import TitanClient

client = TitanClient()
# Save a result globally before the task exits
client.store_put("task_123_accuracy", "98.5")
client.store_sadd("processed_files", "batch_A.csv")
```

**File 2: `task_b.py` (Consumer)**

```python
from titan_sdk import TitanClient

client = TitanClient()
# Retrieve the data passed from Task A
accuracy = client.store_get("task_123_accuracy")
completed_files = client.store_smembers("processed_files")

print(f"Downstream task received accuracy: {accuracy}")
```

---

## 4. File Artifacts

Use artifacts when a worker produces a file that the orchestrator (or a downstream worker) needs to read. TitanStore only holds strings — for binary files or large text outputs, use the artifact system.

**Pattern: worker publishes → orchestrator downloads**

```python
# worker_script.py (runs on a worker node)
from titan_sdk import TitanClient

client = TitanClient()

# Write the output file to the local workspace
with open("report.md", "w") as f:
    f.write(final_report)

# Upload to master and register under a TitanStore key
client.publish_artifact(f"run:{run_id}:report", "report.md")
```

```python
# orchestrator.py (runs on your machine)
from titan_sdk import TitanClient

client = TitanClient()

# Download the file by key — saves to /tmp/report.md by default
client.get_artifact(f"run:{run_id}:report", save_path=f"/tmp/report_{run_id}.md")
```

!!! warning "Remote workers must upload files explicitly"
    `titan_workspace/shared` is **local to the worker node**. For local workers (same machine as Master) files appear in the Dashboard automatically. For **remote workers** (RunPod, GCP, SSH tunnel), files written to disk stay on the remote machine and will never appear in Dashboard → Workspace Files unless explicitly uploaded.

    Use `upload_file` for simple downloads from the Dashboard, or `publish_artifact` / `get_artifact` when the orchestrator needs to read the file programmatically:

    - `upload_file("output.txt")` — file appears in Dashboard Workspace Files, downloadable by a human
    - `publish_artifact(key, "output.txt")` — file uploaded and registered in TitanStore; retrieve with `get_artifact(key)` from the orchestrator
