# 💻 Operator Manual (CLI)

The Titan CLI is a Java-based interactive shell that connects directly to the Master node. It allows cluster operators to view real-time system health, manually deploy services, and kill rogue tasks.

## Connecting to the Cluster

After building the project, run `TitanCLI`. It connects to the master on `localhost:9090`.

```bash
java -cp target/classes titan.TitanCLI
```

**Supported Commands:**

| Command                              | Description                                                                                 |
|--------------------------------------|---------------------------------------------------------------------------------------------|
| `stats`                              | View cluster health, active nodes, and job queues.                                          |
| `json`                               | View the cluster status as JSON.                                                           |
| `status <job_id>`                  | Read a job's current status.                                                               |
| `logs <job_id>`                    | Read a job's logs.                                                                         |
| `cancel <job_id>`                  | Cancel a job and its dependent jobs.                                                       |
| `approve <gate_id>`                | Approve a waiting human gate, for example `approve DAG-hitl-gate-prepare`.                 |
| `reject <gate_id>`                 | Reject a waiting human gate.                                                               |
| `store get <key>`                  | Read a TitanStore value (`kv get` also works).                                             |
| `store set <key> <value>`          | Write a TitanStore value (`kv set` also works).                                            |
| `run <file>`                         | Immediately execute a script from `perm_files`.                                             |
| `deploy <file> <port>`                | Deploy a service and wait for its listening port. Supports capability requirements. Ex: `deploy api.py 9000 GPU` |
| `deploy Worker.jar <port> [capability]` | Manually spawn a new Worker node on a specific port with its capability (GPU or GENERAL)    |
| `resolve <service_id>`               | Return the listening `host|port` for a ready service.                                       |
| `stop <service_id>`                  | Gracefully stop a running service or job.                                                   |
| `shutdown <host> <port>`             | Remotely decommission a specific worker node.                                               |
| `dag <dag_string>`                   | Submit a raw DAG string (Advanced users).                                                   |
| `upload <local_path>`                | Upload a file to server storage (`uploads`).                                                |


## The CLI Interface

When you run the stats command, the CLI provides a real-time snapshot of the ActiveJobQueue, the WaitingQueue (Blocked DAGs), and the exact load on every connected worker node.
It also explicitly lists all **Live Services** currently hosted on the cluster.

```
==========================================
    [INFO] TITAN DISTRIBUTED ORCHESTRATOR    
==========================================
Connected to: localhost:9090
Type `help` to list all commands and examples.

titan> stats
--- TITAN SYSTEM MONITOR ---
Active Workers:    3
Execution Queue:   0 jobs
Blocked (DAG):     1 jobs
-------------------------------
Worker Status:
 • [8080] Load: 2/4 (50%)    | Skills: [GENERAL]
    └── ⚙️ Service ID: DAG-JOB_SPAWN
    └── ⚙️ Service ID: DAG-step-4-server
 • [8081] Load: 0/4 (0%)     | Skills: [GENERAL]
```



### How to Read the Monitor
- **Execution Queue:** The number of tasks that are fully unblocked and waiting for an available worker slot. If this number is high and stays high, your cluster is under-provisioned and needs more workers.

- **Blocked (DAG):** Tasks that are safely waiting in the WaitingQueue because their parent dependencies have not finished yet.

- **Load (e.g., 2/4 (50%)):** Indicates that this specific worker is currently executing 2 concurrent tasks out of its maximum thread capacity of 4.

- **Skills:** The capability tags assigned to that node (e.g., [GENERAL], [GPU]), dictating what kind of tasks the Master is allowed to route to it.

- **Live Services:** The CLI explicitly lists long-running processes hosted on the node under the worker stats.

        WRK- Prefix: Indicates a dynamically spawned Worker instance.

        TSK- Prefix: Indicates a deployed long-running service (like an API, web server, or persistent agent).

- **Ephemeral Scripts are Hidden:** The list only shows live services. Fast, ephemeral scripts are not printed by name in the tree to prevent console spam; their execution is simply reflected in the active Load percentage and the total jobs information.
