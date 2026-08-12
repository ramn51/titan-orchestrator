# Titan RPC Specification & Payload Translation



In the Titan architecture, the Master node does not simply forward messages from the Python SDK to the Worker nodes. It acts as an intelligent **API Gateway**. 

When a client submits a high-level intent, the Master intercepts the payload, injects system-generated state (such as the `Job_ID`), and translates the request into specific execution primitives for the Worker.

---

## 🔄 The Execution Translation Layer

This layer handles the staging and execution of code. For many of these commands, the Master utilizes a **Two-Phase Commit** pattern: it first instructs the worker to stage the file, and only upon success does it send the execution command.

### 1. Standard Scripts (`RUN_PAYLOAD`)
When a user executes a standalone script, the Master stages the file and appends the system-generated Job ID before execution.

* **Client Sends (`OP_RUN`):** `RUN_PAYLOAD | script.py | arg1 | <base64_string> | GPU`
* **Master Translation Step 1 (`OP_STAGE`):** `script.py | <base64_string>`
* **Master Translation Step 2 (`OP_RUN`):** `JOB-123 | script.py | arg1`

### 2. Detached Services (`DEPLOY_PAYLOAD`)
Similar to script execution, but the final command instructs the Worker to launch the artifact as a detached background daemon on a specified port.

* **Client Sends (`OP_DEPLOY`):** `DEPLOY_PAYLOAD | Worker.jar | <base64_string> | 8085`
* **Master Translation Step 1 (`OP_STAGE`):** `Worker.jar | <base64_string>`
* **Master Translation Step 2 (`OP_START_SERVICE`):** `Worker.jar | WRK-8085-uuid | 8085`

### 3. Archive Execution (`RUN_ARCHIVE`)
For complex projects packaged as ZIP files, the Master resolves the internal archive pointer locally to extract the specific entry point before pushing the payload to the worker.

* **Client Sends (`OP_SUBMIT_JOB`):** `RUN_ARCHIVE | my_project.zip/main.py`
* **Master Translation (`OP_RUN_ARCHIVE`):** `JOB-123 | main.py | <base64_content_of_main_py>`

### 4. Archive Services (`START_ARCHIVE_SERVICE`)
Deploys a long-running service directly from a bundled ZIP archive.

* **Client Sends (`OP_SUBMIT_JOB`):** `START_ARCHIVE_SERVICE | web.zip/server.py | --prod | 8080`
* **Master Translation (`OP_START_SERVICE_ARCHIVE`):** `JOB-123 | server.py | 8080 | <base64_content_of_server_py>`

---

## 🛑 The Control Translation Layer

For administrative and lifecycle commands, the Client SDK specifies the target. The Master leverages its internal connection state to locate the exact Worker socket, simplifying the command sent over the network.

### 1. Shutting Down a Worker
The Client only knows the Worker's IP and Port. The Master handles the socket routing.

* **Client Sends (`OP_KILL_WORKER`):** `192.168.1.5|8081` *(Target definition)*
* **Master Translation (`OP_KILL_WORKER`):** `NOW` *(The Master identifies the correct socket and sends the immediate execution command)*

### 2. Stopping a Remote Service
* **Client Sends (`OP_STOP`):** `DAG999_job_1`
* **Master Translation (`OP_STOP`):** `DAG999_job_1` *(The payload remains unchanged, but the Master performs the heavy lifting of routing it strictly to the assigned Worker)*

---

## 🕸️ DAG Job Definition Standard

When submitting a Directed Acyclic Graph (DAG) using `OP_SUBMIT_DAG`, multiple jobs are sent as a single string separated by semicolons (`;`). To prevent data corruption, the Titan Master uses a strict **Outside-In Parsing** algorithm. 

Every individual job within a DAG must adhere to the following pipe-delimited format:

`ID | SKILL | <COMMAND_PAYLOAD> | PRIORITY | DELAY_MS | [DEPENDENCIES] | OPTIONAL_FLAGS`

### Field Breakdown

| Field | Description | Example |
| :--- | :--- | :--- |
| `ID` | The unique identifier for the job. The Master will automatically prefix this with `DAG-` if not provided. | `extract_data` |
| `SKILL` | The capability required by the Worker to execute this job. | `GENERAL`, `GPU`, `PYTHON` |
| `<COMMAND_PAYLOAD>` | The standard execution payload. | `RUN_PAYLOAD|calc.py|UEsDBB...` |
| `PRIORITY` | Integer queue priority on an open scale — **higher numbers are scheduled first** (not a capped 0–2 enum). Default `1`. | `1` |
| `DELAY_MS` | Time in milliseconds to wait before placing the job in the active queue. | `0` (immediate) |
| `[DEPENDENCIES]` | A comma-separated list of parent Job IDs wrapped in brackets. | `[extract_data, clean_data]` |
| `OPTIONAL_FLAGS` | System modifiers. Currently supports `AFFINITY` or `STICKY`. | `AFFINITY` |

---

## 📡 Scheduler Server RPC Endpoints

The `SchedulerServer` acts as the primary ingress point for the Titan cluster. It accepts formatted TCP payloads from clients and workers, dispatches them to the internal engine, and returns standardized responses.

### Worker & Cluster Management

| OpCode | Payload String Format | Example Payload | Engine Action |
| :--- | :--- | :--- | :--- |
| `OP_REGISTER` | `workerPort||capability||isPerm` | `8081||GPU||true` | Registers a new worker node in the Scheduler's internal map. |
| `OP_KILL_WORKER` | `HOST|PORT` | `192.168.1.5|8081` | Sends a fatal shutdown signal to a specific Worker JVM. |
| `OP_STATS` | *(Empty / Ignored)* | ` ` | Returns cluster statistics as a formatted string. |
| `OP_STATS_JSON` | *(Empty / Ignored)* | ` ` | Returns cluster statistics serialized as a JSON string. |
| `OP_CLEAN_STATS` | *(Empty / Ignored)* | ` ` | Clears the `LiveServiceMap` to remove stale dashboard data. |

### Job & DAG Execution

| OpCode | Payload String Format | Example Payload | Engine Action |
| :--- | :--- | :--- | :--- |
| `OP_SUBMIT_JOB` | `Job_Definition_String` | `RUN_PAYLOAD|script.py|base64...` | Parses a single job definition and adds it to the execution queue. |
| `OP_SUBMIT_DAG` | `job1_def;job2_def;...` | `job1;job2;job3` | Splits the string by `;` and submits multiple linked jobs simultaneously. |
| `OP_RUN` | `filename|requirement` | `train.py|GPU` | Wraps a script from `perm_files` into a job and queues it. |
| `OP_STOP` | `serviceId` | `DAG999_job_1` | Forcibly terminates a running service or process tree. |
| `OP_UNREGISTER_SERVICE`| `serviceId` | `DAG999_job_1` | Silently removes a service from the Master's active tracking map. |
| `OP_JOB_COMPLETE` | `jobId|status|result`| `DAG999_job_1|COMPLETED|Ok` | Triggers the Master's DAG resolution logic to unlock dependent nodes. |
| `OP_GET_JOB_STATUS` | `jobId` | `DAG-test_job_123` | Queries TitanStore for the exact execution state of a job. |

### Asset & File Distribution

| OpCode | Payload String Format | Example Payload | Engine Action |
| :--- | :--- | :--- | :--- |
| `OP_UPLOAD_ASSET` | `FILENAME|BASE64_CONTENT` | `data.csv|UEsDBBQ...` | Decodes the Base64 stream and saves the asset directly to the `perm_files/` directory. |
| `OP_FETCH_ASSET` | `filename` | `model_weights.pt` | Reads a file from `perm_files/` and returns it encoded as a Base64 string. |
| `OP_DEPLOY` | `filename|port|requirement`| `Worker.jar|9050|GENERAL` | Wraps a file into a deployment job to stage and execute it on a worker. |

### Logging & Telemetry

| OpCode | Payload String Format | Example Payload | Engine Action |
| :--- | :--- | :--- | :--- |
| `OP_LOG_STREAM` | `jobId|logLine` | `job_123|Epoch 1 complete` | Streams a single log line into the Master's in-memory buffer. |
| `OP_LOG_BATCH` | `jobId|line1\nline2...` | `job_123|Line1\nLine2` | Splits a block of text by `\n` and ingests multiple log lines simultaneously. |
| `OP_GET_LOGS` | `jobId` | `job_123` | Retrieves the log history from the in-memory buffer or the disk archive. |

### TitanStore (Key-Value Engine)

| OpCode | Payload String Format | Example Payload | Engine Action |
| :--- | :--- | :--- | :--- |
| `OP_KV_SET` | `key|value` | `model_acc|0.95` | Prepends `user:` and writes the value to the integrated database. |
| `OP_KV_GET` | `key` | `model_acc` | Queries the database for `user:<key>` and returns the stored value. |
| `OP_KV_SADD` | `key|member` | `active_users|uuid-456` | Adds a member to a distributed Set. Returns "1" (new) or "0" (exists). |
| `OP_KV_SMEMBERS` | `key` | `active_users` | Retrieves a Set and formats it as a comma-separated string. |

---

## ⚙️ Worker Node RPC Endpoints (Master ➡️ Worker)

While the Scheduler manages the cluster state, the `RpcWorkerServer` listens for direct execution commands from the Master. The Worker node parses these payloads to manage local OS processes, stage files, and report telemetry.

| OpCode | Expected Payload Format | Worker Action & Response |
| :--- | :--- | :--- |
| `OP_HEARTBEAT` | *(Empty / Ignored)* | The worker calculates its active thread count vs. `MAX_THREADS` and responds with `PONG | activeLoad | maxCapacity`. |
| `OP_STAGE` | `filename | <base64_string>` | Synchronously decodes the Base64 string and writes it to the local `titan_workspace`. Returns `FILE_SAVED` or an error. |
| `OP_RUN` | `jobId | filename | args` | Spawns a background thread to execute the script via `ScriptExecutorHandler`. Upon completion, it automatically opens a new socket to send `OP_JOB_COMPLETE` back to the Master. |
| `OP_START_SERVICE` | `filename | serviceId | port` | Synchronously launches the file as a detached background daemon, registers the PID, and returns `DEPLOYED_SUCCESS`. |
| `OP_STOP` | `serviceId` | Looks up the Java `Process` object in the active map and executes a recursive `destroyForcibly()` on the OS process tree. |
| `OP_KILL_WORKER` | `NOW` (or any string) | Triggers a 100ms busy-wait followed by a hard `System.exit(0)` to gracefully terminate the JVM. |
| `OP_RUN_ARCHIVE` | `jobId | entryFile | <base64_zip>` | Synchronously unzips the payload using `WorkspaceManager`, resolves the entry file path, and executes it asynchronously. |
| `OP_START_SERVICE_ARCHIVE`| `serviceId | entryFile | port | <base64_zip>` | Synchronously unzips the payload, resolves the entry file, and launches it as a detached background daemon. |