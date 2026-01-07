import time, sys
import os

from titan_sdk import TitanClient, TitanJob

# 1. Setup Dummy Files
def create_dummy_files():
    with open("dummy_gpu.py", "w") as f:
        f.write("import time\nprint('Running heavy GPU math...')\ntime.sleep(2)\nprint('GPU Task Done')")

    with open("dummy_cpu.py", "w") as f:
        f.write("print('Running standard CPU task')")

def run_test():
    create_dummy_files()
    client = TitanClient()

    print("--- TEST 1: SDK Direct Submission ---")

    # Job 1: General CPU Task
    job_cpu = TitanJob(
        job_id="TEST-CPU-01",
        filename="dummy_cpu.py",
        job_type="RUN_PAYLOAD",
        requirement="GENERAL"  # Explicitly GENERAL
    )

    # Job 2: GPU Task
    job_gpu = TitanJob(
        job_id="TEST-GPU-01",
        filename="dummy_gpu.py",
        job_type="RUN_PAYLOAD",
        requirement="GPU"      # <--- The Feature we are testing
    )

    # Submit as a small DAG (Independent jobs)
    print("Submitting CPU Job...")
    client.submit_job(job_cpu)

    print("Submitting GPU Job...")
    client.submit_job(job_gpu)

    print("✅ Jobs Submitted. Check Titan Server logs for '[Req: GPU]'")

if __name__ == "__main__":
    run_test()