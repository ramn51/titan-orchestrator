# 🚀 Getting Started

Get a Titan Master, Worker node, and the UI Dashboard running locally in under 5 minutes.

### Prerequisites
Before you begin, ensure you have the following installed on your machine:

- **Java 17+** (For the Core Engine)
- **Python 3.10+** (For the SDK, CLI, and Dashboard)
- **Maven** (To build the Java project)

---

## Building the Engine

### 1. Clone the Engine
Titan compiles down into a single, ultra-lightweight "Uber-JAR". We will build it and place it in the `perm_files` directory, which acts as Titan's artifact registry.

```bash
git clone https://github.com/ramn51/titan-orchestrator.git
cd titan-orchestrator
```

### Option 1.1: Run via IntelliJ (Recommended for Dev)

If you are developing Titan, simply open the project in IntelliJ IDEA and run the Main classes directly:

1. **Master:** Run `titan.TitanScheduler`
2. **Worker:** Run `titan.TitanWorker` (Defaults to Port 8080, Capability: GENERAL, Permanent: False)
3. **CLI:** Run `titan.TitanCli`

### Option 1.2. Build the core engine
```bash
mvn clean package
```

### 2. Stage the binary for execution (Optional, there will be a Worker.jar already there)
```bash
cp target/titan-orchestrator-1.0-SNAPSHOT.jar perm_files/Worker.jar
```

### 3. Configure the Runtime (titan.properties)

Titan uses an Adapter Pattern for its state management, meaning the persistence layer is entirely pluggable. To connect the Master to your Redis (TitanStore) instance for state recovery and data-bus features, create a titan.properties file in the root directory where you run the JAR.

Create `titan.properties`:

Properties
```properties
# TitanStore (Redis) Connection
titan.redis.host=localhost
titan.redis.port=6379

# Cluster Tuning
titan.worker.heartbeat.interval=10
titan.worker.pool.size=10
```
> Note: If this file is missing, Titan will gracefully degrade to sensible defaults (purely in-memory execution with no persistence or recovery) or attempt to connect to Redis on localhost:6379.


## Start the Cluster

### 5. Start TitanMaster and TitanWorker


**Terminal 1 (The Master Scheduler):**
This starts the control plane on default port `9090`. It will listen for worker heartbeats and incoming DAG submissions.

```bash
java -cp target/titan-orchestrator-1.0-SNAPSHOT.jar titan.TitanMaster

```

**Terminal 2 (The Default Worker Node):**
This starts a general-purpose hardware node. By default, it connects to the local Master on port `9090` and opens itself for task execution on port `8080`.

```bash
java -cp target/titan-orchestrator-1.0-SNAPSHOT.jar titan.TitanWorker

```

*(You should immediately see a "Worker Registered" log appear in Terminal 1).*

> **⚡ Advanced Worker Configurations**
> You can easily spawn specialized nodes by passing arguments: `[Port] [MasterIP] [MasterPort] [Capability] [isPermanent]`

> *Example: Spawn a persistent GPU worker on port 8081:*
```bash 
java -cp target/titan-orchestrator-1.0-SNAPSHOT.jar titan.TitanWorker 8081 localhost 9090 GPU true
```
> 
> 

---

### 5. Install the Python SDK

The Titan Python SDK allows you to submit jobs, define DAGs, and interact with the cluster programmatically.

Open a third terminal window and install the SDK in editable mode:

```bash
pip install -e .
```

---

### 6. Run Your First Task

Let's deploy a pre-configured YAML DAG to the cluster using the Titan CLI.

```bash
python titan_sdk/titan_cli.py deploy titan_test_suite/examples/yaml_based_static_tests/dag_structure_test
```

**What just happened?**

1. The CLI parsed the YAML definition and zipped the required Python scripts.
2. It dispatched the payload to the Master node via Titan's custom binary protocol.
3. The Master resolved the dependency graph and routed the tasks to your idle Worker node.
4. The Worker executed the code in an isolated workspace!

---

### 7. Open the Dashboard (Optional)

Titan includes a lightweight Flask dashboard to visualize cluster health and stream live logs. To spin it up, run:

```bash
python3 ./perm_files/server_dashboard.py
```

Navigate to **`http://localhost:5000`** in your browser to see your Worker node's live CPU/Thread load and the history of the DAG you just ran.

