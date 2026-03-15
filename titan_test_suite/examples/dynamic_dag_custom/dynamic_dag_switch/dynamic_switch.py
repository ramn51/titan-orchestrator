#  Copyright 2026 Ram Narayanan
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import time, os, sys


from titan_sdk import TitanClient, TitanJob

# HELPER: Get absolute path to the local scripts folder
def get_script_path(script_name):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "scripts", script_name)

def run_dynamic_pipeline():
    client = TitanClient()

    # 1. Define the two possible strategies
    # Strategy A: Fast (Single Node)
    fast_job = TitanJob(
            job_id="FAST_SCAN",
            filename=get_script_path("quick_scan.py"),
            priority=10
        )

    # Strategy B: Deep (Distributed / Fan-Out)
    job_root = TitanJob(job_id="DEEP_ROOT", filename=get_script_path("prepare_data.py"))
    job_w1 = TitanJob(job_id="WORKER_1", filename=get_script_path("analyze_chunk.py"), parents=["DEEP_ROOT"])
    job_w2 = TitanJob(job_id="WORKER_2", filename=get_script_path("analyze_chunk.py"), parents=["DEEP_ROOT"])
    deep_dag = [job_root, job_w1, job_w2]

    # 2. The Decision Layer (Logic-Driven Infrastructure)
    # In a real scenario, you might ping an API or check client.fetch_logs()
    print("[LOGIC] Checking Cluster Metrics...")

    # Simulating a high-traffic event
    traffic_load = 50

    if traffic_load > 80:
        print(f"[CRITICAl] High Traffic ({traffic_load}%). Switching to 'FAST' pipeline to save resources.")
        client.submit_job(fast_job)
    else:
        print(f"[HEALTHY] Normal Traffic ({traffic_load}%). Running 'DEEP' analysis.")
        client.submit_dag("DEEP_PIPELINE", deep_dag)

if __name__ == "__main__":
    run_dynamic_pipeline()