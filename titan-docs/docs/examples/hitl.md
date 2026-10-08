# Human approval gates

The Python SDK can pause a DAG after a job while someone reviews its output. Set `hitl_message` on the job that needs review:

```python
from titan_sdk import TitanClient, TitanJob

prepare = TitanJob(
    job_id="prepare",
    filename="prepare.py",
    hitl_message="Check the generated report before publishing it.",
    max_wait_seconds=1800,
)
publish = TitanJob(job_id="publish", filename="publish.py", parents=["prepare"])

client = TitanClient()
print(client.submit_dag("review-report", [prepare, publish]))
```

The SDK inserts `DAG-hitl-gate-prepare` between `prepare` and `publish`. When `prepare` completes, the gate starts and records a `WAITING` decision in TitanStore. Open **DAG Pipelines → review-report** to read the review message and select **Approve** or **Reject**. The CLI offers the same decision:

```text
approve DAG-hitl-gate-prepare
reject DAG-hitl-gate-prepare
```

Approval completes the gate and releases its dependent jobs. Rejection or timeout fails the gate; dependent jobs do not run. The gate polls every five seconds, so a decision may take a few seconds to appear in the graph. `max_wait_seconds` can be from 1 second to 48 hours; the default is 48 hours.

The gate is a worker job and occupies one worker slot while it waits. Give the cluster enough worker capacity for unrelated jobs, and choose a practical timeout. Redeploy clears the previous decision before the new run starts. The dashboard and SDK must use the same DAG manifest path to show the submitted pipeline.
