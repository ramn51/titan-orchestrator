"""
HITL Pipeline Demo
==================
This example shows a 3-stage ML pipeline with a Human-in-the-Loop gate
between preprocessing and training:

    preprocess  →  hitl-review  →  train

1. preprocess   : Scans the dataset, writes a summary report
2. hitl-review  : Blocks and waits for a human to Approve or Reject
                  via the Titan Dashboard (DAG Pipelines view)
3. train        : Runs only after the human approves step 2

How to run:
    # Make sure the cluster is up first:
    #   ./titan-dev up
    python run_hitl_demo.py

Then open http://localhost:5000/dags in your browser.
An amber banner will appear when the HITL gate is reached.
Click Approve to let training proceed, or Reject to fail the pipeline.
"""

import sys
import os

# Allow running from any directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from titan_sdk.titan_sdk import TitanClient, TitanJob

client = TitanClient()

# Resolve paths relative to perm_files (where the worker reads scripts from)
PERM = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'perm_files'))

# Stage 1 — Preprocessing
# hitl_message triggers an automatic approval gate after this job completes.
# The SDK injects a 'hitl-gate-hitl-preprocess' job and re-wires train's dependency.
preprocess = TitanJob(
    job_id       = "hitl-preprocess",
    filename     = os.path.join(PERM, "preprocess.py"),
    hitl_message = "Data looks good? Approve to start training.",
)

# Stage 2 — Training (depends on preprocess; SDK re-wires this to wait for the gate)
train = TitanJob(
    job_id   = "hitl-train",
    filename = os.path.join(PERM, "train.py"),
    parents  = ["hitl-preprocess"],
)

print("Submitting HITL demo pipeline...")
result = client.submit_dag("hitl-ml-pipeline", [preprocess, train])
print(f"Submission result: {result}")
print()
print("Open http://localhost:5000/dags to monitor the pipeline.")
print("When the amber HITL banner appears, click Approve or Reject.")
