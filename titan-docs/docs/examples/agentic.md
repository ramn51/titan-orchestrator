# 🐍 Programmatic & Agentic Workflows

While YAML is excellent for static, predictable pipelines, modern infrastructure often requires dynamic decision-making. 

The **Titan Python SDK** allows you to break out of static configurations and use standard Python code to generate execution graphs programmatically. This enables everything from simple conditional logic to fully autonomous AI agents that generate their own execution paths at runtime.

Once the SDK is installed (`pip install -e .`), run any example script directly with `python <script_name>.py`.

The examples live in `titan_test_suite/examples/` — agentic examples under `agents_examples/`, static DAG examples under `yaml_based_static_tests/` and `dynamic_dag_custom/`. See the [full worked examples table](#full-worked-examples) below.

---

## 1. Defining DAGs Programmatically (The Basics)

Using the SDK, you construct units of work using the `TitanJob` class and define their dependencies (`parents`). When you submit them as a list, Titan's Master node automatically resolves the DAG and distributes the workload.

```python
from titan_sdk import TitanClient, TitanJob

client = TitanClient()

# 1. Define the Root Task
task_a = TitanJob(
    job_id="extract_data",
    filename="etl/extract.py",
    priority=5
)

# 2. Define a Dependent Task
task_b = TitanJob(
    job_id="train_model",
    filename="ml/train.py",
    requirement="GPU",
    parents=["extract_data"] # <--- Defines the dependency
)

# 3. Submit them as a unified DAG
client.submit_dag("nightly_pipeline", [task_a, task_b])
print("DAG Submitted Successfully!")
```

```mermaid
flowchart LR
    A[extract_data: extract.py<br> Priority: 5] --> B[train_model: train.py<br> Requirement: GPU]
    
    style A fill:#1e293b,stroke:#f9a826,stroke-width:2px,color:#ffffff
    style B fill:#1e293b,stroke:#bf360c,stroke-width:2px,color:#ffffff
```

## 2. Dynamic Logic
_Best for_: __Conditional logic, real-time load balancing, and dynamic infrastructure.__

Because you are using pure Python, you can use standard if/else logic to decide which execution graph to build based on real-time cluster stats, database queries, or external API calls.

### The "Logic Switch"
In this scenario, our script checks the current traffic load. If traffic is high, it submits a single, lightweight task. If traffic is low, it dynamically generates a massive parallel "Deep Analysis" DAG.


```python
from titan_sdk import TitanClient, TitanJob

client = TitanClient()
traffic_load = get_current_traffic() # Your custom monitoring logic

if traffic_load > 80:
    print("[HIGH] High Traffic. Switching to lightweight 'FAST' pipeline.")
    fast_job = TitanJob(job_id="fast_scan", filename="fast_path.py")
    client.submit_job(fast_job)
else:
    print("[LOW] Normal Traffic. Generating distributed 'DEEP' analysis DAG.")
    # Programmatically generate 10 parallel tasks
    deep_tasks = [
        TitanJob(job_id=f"deep_{i}", filename="deep_path.py") 
        for i in range(10)
    ]
    client.submit_dag("DEEP_PIPELINE", deep_tasks)
```

```mermaid
flowchart LR
    Traffic{"Traffic > 80?"}

    %% The Fast Path
    Traffic -->|Yes: High Traffic| Fast["fast_scan: fast_path.py<br>⚡ Fast"]

    %% The Deep Path
    Traffic -->|No: Normal Traffic| DAG["DEEP_PIPELINE<br>📦 submit_dag()"]
    
    %% The Fan-out of the Deep Path
    DAG --> D0["deep_0"]
    DAG --> D1["deep_1"]
    DAG --> D9["... up to deep_9"]

    style Traffic fill:#1e293b,stroke:#f9a826,stroke-width:2px,color:#ffffff
    style Fast fill:#1e293b,stroke:#ef4444,stroke-width:2px,color:#ffffff
    style DAG fill:#1e293b,stroke:#1de9b6,stroke-width:2px,color:#ffffff
    style D0 fill:#1e293b,stroke:#64748b,stroke-width:2px,color:#ffffff
    style D1 fill:#1e293b,stroke:#64748b,stroke-width:2px,color:#ffffff
    style D9 fill:#1e293b,stroke:#64748b,stroke-width:2px,color:#ffffff
```

## 3. Agentic Workflows & LLMs
_Best for_: __AI Agents, Self-Healing loops, and recursive execution.__

In this mode, Titan acts as the physical execution substrate for Software Agents. Because Titan is completely decoupled, an LLM (like Gemini) can evaluate the output of a previous task and dynamically formulate a brand new TitanJob to execute.

The task graph itself is generated dynamically during execution.

### The "Self-Healing" Loop
Imagine an agent that monitors a distributed job. It fetches the remote execution logs, and if it detects a critical failure, it programmatically creates a new "Patch" job to remediate the issue on the fly.



```python
from titan_sdk import TitanClient, TitanJob

client = TitanClient()
job_id = "flaky_training_run"

# 1. Fetch remote execution logs via Titan's protocol
logs = client.fetch_logs(job_id)

# 2. AI Evaluation: An LLM Agent analyzes the physical execution logs
decision = llm_agent.analyze_failure(logs)

# 3. Act on the LLM's dynamic decision
if decision.action == "DEPLOY_PATCH":
    print(f"[AGENT] Root cause: {decision.reason}. Deploying patch...")

    # Programmatically create a remediation job on the fly
    fix_job = TitanJob(
        job_id=f"{job_id}_fix", 
        filename="scripts/safe_mode_patch.py",
        # The agent dynamically requests specific capabilities based on the failure!
        requirement=decision.recommended_hardware # e.g., "HIGH_MEM"
    )

    # 4. Dispatch the new task to the cluster
    client.submit_job(fix_job)
else:
    print("[AGENT] Run healthy or failure is non-critical. Sequence complete.")
```

```mermaid
flowchart LR
    Logs["fetch_logs('flaky_run')<br> Get Execution Logs"] --> LLM["llm_agent.analyze(logs)<br> AI Evaluation"]
    
    LLM --> Decision{"Action ==<br>'DEPLOY_PATCH'?"}

    Decision -->|Yes: Root Cause Found| Patch["TitanJob(req='HIGH_MEM')<br> client.submit_job()"]
    Decision -->|No: Non-Critical| Done["Run Healthy<br> Sequence Complete"]

    style Logs fill:#1e293b,stroke:#64748b,stroke-width:2px,color:#ffffff
    style LLM fill:#1e293b,stroke:#d946ef,stroke-width:2px,color:#ffffff
    style Decision fill:#1e293b,stroke:#f9a826,stroke-width:2px,color:#ffffff
    style Patch fill:#1e293b,stroke:#ef4444,stroke-width:2px,color:#ffffff
    style Done fill:#1e293b,stroke:#1de9b6,stroke-width:2px,color:#ffffff
```

## 4. Stateful Agents (Using TitanStore for Memory)
*Best for: Recursive agents, distributed web scrapers, and global state tracking.*

When an autonomous agent decides to respawn itself or dispatch sub-agents across different physical nodes, it loses its local Python variables. To maintain a "train of thought" across the cluster, agents can use Titan's built-in `TitanStore` as a shared global memory.

### The "Recursive Agent" Pattern
In this example, an autonomous agent tries to execute a fragile network task. If it fails, it increments a global counter in TitanStore and spawns a clone of itself to try again. If it fails 3 times, it realizes the system is down and escalates to a human instead of infinite-looping.

**File: `autonomous_agent.py`**
```python
from titan_sdk import TitanClient, TitanJob
import random

client = TitanClient()

# 1. Agent wakes up and checks its global memory
# (Defaults to 0 if this is the first time running)
current_attempts = int(client.store_get("agent_retry_count") or 0)

print(f"[INFO] Agent Booting... (Attempt {current_attempts + 1} of 3)")

if current_attempts >= 3:
    print("[ESCALATION] Task failed 3 times. Halting recursion and alerting human.")
    client.store_put("agent_retry_count", "0") # Reset for future runs
    exit(1)

# 2. Attempt the fragile task (e.g., calling an unstable API)
api_success = random.choice([True, False])

if api_success:
    print("[SUCCESS] Task completed! Agent terminating.")
    client.store_put("agent_retry_count", "0") # Reset memory
else:
    print("[WARNING] Task failed. Agent logging failure and respawning...")
    
    # 3. Update global memory so the NEXT clone knows what happened
    client.store_put("agent_retry_count", str(current_attempts + 1))
    
    # 4. Agent dynamically spawns a clone of itself to try again!
    retry_job = TitanJob(
        job_id=f"agent_retry_{current_attempts + 1}", 
        filename="autonomous_agent.py"
    )
    client.submit_job(retry_job)
```


```mermaid
flowchart LR
    %% State Load
    StateGet["1. agent_retry_count<br> client.store_get()"] --> DecisionRetry{"Attempt >= 3?"}
    
    %% Escalation Path
    DecisionRetry -->|Yes: Halting| ResetEsc["client.store_put(0)<br>🧹 Reset State"]
    ResetEsc --> Escalate["[ESCALATION]<br>Alert Human"]

    %% Execution Path
    DecisionRetry -->|No: Try Again| FragileTask{"2. fragile_api()<br> try/except"}
    
    %% Success Path
    FragileTask -->|Success| ResetDone["client.store_put(0)<br> Reset State"]
    ResetDone --> Terminate["[TERMINATE]<br>Sequence Done"]
    
    %% Failure Path
    FragileTask -->|Failure| StatePut["3. Increment Count<br> client.store_put( Attempts + 1 )"]
    
    %% The Spawning Action
    StatePut --> SubmitJob["4. submit_job()<br> Dynamic Spawn"]
    
    %% The Loop back
    SubmitJob -.->|"New Clone Wakes Up"| StateGet

    %% Dark-Mode Styling
    style StateGet fill:#1e293b,stroke:#1de9b6,stroke-width:2px,color:#ffffff
    style StatePut fill:#1e293b,stroke:#1de9b6,stroke-width:2px,color:#ffffff
    style ResetEsc fill:#1e293b,stroke:#64748b,stroke-width:2px,color:#ffffff
    style ResetDone fill:#1e293b,stroke:#64748b,stroke-width:2px,color:#ffffff
    style DecisionRetry fill:#1e293b,stroke:#f9a826,stroke-width:2px,color:#ffffff
    style FragileTask fill:#1e293b,stroke:#f9a826,stroke-width:2px,color:#ffffff
    style Terminate fill:#1e293b,stroke:#22c55e,stroke-width:3px,color:#ffffff
    style Escalate fill:#1e293b,stroke:#ef4444,stroke-width:3px,color:#ffffff
    style SubmitJob fill:#1e293b,stroke:#bf360c,stroke-width:3px,color:#ffffff,stroke-dasharray: 5 5
```

**Why this is powerful:**
You only need to submit autonomous_agent.py to the cluster once. From that point on, the agent manages its own lifecycle, hops between available hardware nodes using Titan's scheduler, and uses the distributed store to remember its past failures until the job succeeds.

---

## Full Worked Examples

These end-to-end agents are ready to run and demonstrate the patterns above with real LLM calls, TitanStore coordination, and agentic loops:

| Example | Pattern | What it shows |
|---|---|---|
| [Quickstart Agent](quickstart-agent.md) | Writer → Critic loop | Simplest agentic loop in ~60 lines |
| [Research Agent](research-agent.md) | Planner → Researchers → Evaluator → Synthesizer | LLM-driven SYNTHESIZE / DEEPEN decision |
| [Code Generation Agent](code-gen-agent.md) | Planner → Generators → Reviewer → Fixers → Integrator | Targeted fix loop — only flagged components re-run |
| [Research Pipeline](research-pipeline.md) | Fan-out → HITL gate → Fan-in | Human-in-the-Loop approval before synthesis |


