import os
from titan_sdk import TitanClient, TitanJob

def script(name):
    base = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts")
    return os.path.join(base, name)

def run():
    client = TitanClient()

    # Stage 1 — Ingest (root)
    ingest = TitanJob(job_id="INGEST", filename=script("ingest.py"))

    # Stage 2 — Parallel transforms (fan-out from ingest)
    transform_a = TitanJob(job_id="TRANSFORM_A", filename=script("transform.py"), args="SHARD_A", parents=["INGEST"])
    transform_b = TitanJob(job_id="TRANSFORM_B", filename=script("transform.py"), args="SHARD_B", parents=["INGEST"])
    transform_c = TitanJob(job_id="TRANSFORM_C", filename=script("transform.py"), args="SHARD_C", parents=["INGEST"])

    # Stage 3 — Aggregate (fan-in from all 3 transforms)
    aggregate = TitanJob(job_id="AGGREGATE", filename=script("aggregate.py"), parents=["TRANSFORM_A", "TRANSFORM_B", "TRANSFORM_C"])

    # Stage 4 — Validate (sequential after aggregate)
    validate = TitanJob(job_id="VALIDATE", filename=script("validate.py"), parents=["AGGREGATE"])

    # Stage 5 — Report (final)
    report = TitanJob(job_id="REPORT", filename=script("report.py"), parents=["VALIDATE"])

    dag = [ingest, transform_a, transform_b, transform_c, aggregate, validate, report]

    print("[ETL] Submitting complex ETL pipeline (7 jobs, 5 stages)...")
    result = client.submit_dag("ETL_PIPELINE", dag)
    print(f"[ETL] Response: {result}")

if __name__ == "__main__":
    run()
