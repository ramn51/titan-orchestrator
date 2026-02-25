
# 📚 Python SDK Reference

The Titan Python SDK allows you to programmatically define jobs, interact with the TitanStore data bus, and manage distributed artifacts.

---

## 1. Core Classes

### `TitanClient`
The main entry point for connecting to the cluster.

```python
from titan_sdk import TitanClient

# Initialize connection (defaults to localhost:9090)
client = TitanClient(host="localhost", port=9090)

```

| Method | Description |
| --- | --- |
| `client.submit_job(job)` | Dispatches a single `TitanJob` to the cluster. |
| `client.submit_dag(name, jobs)` | Submits a list of linked `TitanJob` objects as a single DAG. |
| `client.get_job_status(job_id)` | Securely queries the Master for a job's internal system status. |
| `client.fetch_logs(job_id)` | Retrieves the stdout/stderr logs for a specific job ID. |
| `client.upload_project_folder(path)` | Zips and uploads a local folder to the Master's artifact registry. |
| `client.upload_file(filepath)` | Uploads a single file to the Master's artifact registry. |
| `client.store_put(key, value)` | Saves a string value to the distributed TitanStore (Data Bus). |
| `client.store_get(key)` | Retrieves a string value from the distributed TitanStore. |
| `client.store_sadd(key, member)` | Adds a member to a distributed Set. Returns 1 if new, 0 if exists. |
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
    parents=["extract_data"] # <--- Defines the dependency
)

# Step 3: Submit them as a unified DAG
client.submit_dag("nightly_pipeline", [task_a, task_b])
print("DAG Submitted!")
```

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
