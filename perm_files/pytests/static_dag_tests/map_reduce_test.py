import time
import os, sys

from titan_sdk import TitanClient, TitanJob

def run_test():
    client = TitanClient()
    
    # Get absolute paths to our payloads
    # (Assuming you run this script from the folder where the files are)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    gen_script = os.path.join(base_dir, "generator.py")
    map_script = os.path.join(base_dir, "mapper.py")
    red_script = os.path.join(base_dir, "reducer.py")

    print("--- BUILDING MAP REDUCE DAG ---")

    # 1. SETUP JOB (The Generator)
    # Args: chunk_a.txt chunk_b.txt
    job_gen = TitanJob(
        job_id="MR-01-GEN",
        filename=gen_script,
        job_type="RUN_PAYLOAD",
        args="chunk_a.txt chunk_b.txt"
    )

    # 2. MAPPER JOBS (Run in Parallel)
    # Note: They both DEPEND on MR-01-GEN
    
    # Mapper A: Reads chunk_a, writes out_a
    job_map_a = TitanJob(
        job_id="MR-02-MAP-A",
        filename=map_script,
        job_type="RUN_PAYLOAD",
        args="chunk_a.txt out_a.txt",
        parents=["DAG-MR-01-GEN"] # Depend on Generator
    )

    # Mapper B: Reads chunk_b, writes out_b
    job_map_b = TitanJob(
        job_id="MR-03-MAP-B",
        filename=map_script,
        job_type="RUN_PAYLOAD",
        args="chunk_b.txt out_b.txt",
        parents=["DAG-MR-01-GEN"] # Depend on Generator
    )

    # 3. REDUCER JOB (The Final Step)
    # Depends on BOTH Mappers
    job_reduce = TitanJob(
        job_id="MR-04-REDUCE",
        filename=red_script,
        job_type="RUN_PAYLOAD",
        args="final_report.txt out_a.txt out_b.txt",
        parents=["DAG-MR-02-MAP-A", "DAG-MR-03-MAP-B"]
    )

    # Submit all 4 jobs as one DAG
    jobs = [job_gen, job_map_a, job_map_b, job_reduce]
    
    print("🚀 Submitting MapReduce DAG...")
    client.submit_dag("MapReduce_Test", jobs)

    # --- MONITORING LOOP ---
    print("⏳ Monitoring execution...")
    monitor_job = "DAG-MR-04-REDUCE" # We only care when the last one finishes
    
    while True:
        logs = client.fetch_logs(monitor_job)
        print("LOGS:", logs)
        
        # DEBUG: Print the raw response length to see if we get ANY data
        if logs:
            print(f"[DEBUG] Received {len(logs)} chars of logs.")
            # print(f"Preview: {logs[:50]}...") # Uncomment to see content
        else:
            print(f"[DEBUG] No logs received yet...")

        if "Grand Total calculated" in logs:
            print("\n✅ SUCCESS! Pipeline Finished.")
            print("--- REDUCER OUTPUT ---")
            print(logs)
            break
        elif "ERROR" in logs:
            print("❌ FAILURE DETECTED")
            print(logs)
            break
            
        time.sleep(2)

if __name__ == "__main__":
    run_test()