import time
from titan_sdk import TitanClient, TitanJob

def run_test():
    print("=== 🚀 TITAN SDK STANDALONE TEST ===")
    
    # Initialize client (uses 127.0.0.1:9090 by default based on your SDK)
    client = TitanClient()

    # --- TEST 1: The Distributed KV Store ---
    print("\n1️⃣ Testing Distributed KV Store (RedisJava)...")
    try:
        client.store_put("system:test_ping", "HELLO_FROM_PYTHON")
        val = client.store_get("system:test_ping")
        print(f"   Returned Value: '{val}'")
        if val != "HELLO_FROM_PYTHON":
            print("   ❌ KV Store mismatch. Is RedisJava running?")
            return
        print("   ✅ KV Store is working perfectly!")
    except Exception as e:
        print(f"   ❌ KV Store crashed: {e}")
        return

    # --- TEST 2: Job Dispatch ---
    print("\n2️⃣ Testing Binary Protocol Job Dispatch...")
    job_id = f"test_job_{int(time.time())}"
    
    try:
        # We reuse the extract.py script you already made
        test_job = TitanJob(
            job_id=job_id,
            filename="extract.py",
            job_type="RUN_PAYLOAD",
            requirement="GENERAL",
            is_archive=False
        )
        
        resp = client.submit_job(test_job)
        print(f"   Master Submission Response: {resp}")
        if "ERROR" in str(resp):
            return
    except Exception as e:
        print(f"   ❌ Dispatch crashed: {e}")
        return

    # --- TEST 3: Execution Polling (Simulating Dagster's wait) ---
    print(f"\n3️⃣ Polling RedisJava for execution state...")
    status_key = f"job:{job_id}:status"
    
    master_job_id = f"DAG-{job_id}"
    print(f"\n3️⃣ Polling Master for execution state of {master_job_id}...")

    for attempt in range(10): 
        # Use the new, dedicated system API!
        status = client.get_job_status(master_job_id)
        print(f"   [Attempt {attempt+1}] Status: {status}")
        
        if status == "COMPLETED":
            print("   ✅ Worker picked up and finished the job successfully!")
            break
        elif status == "FAILED":
            print("   ❌ Worker failed the job!")
            break
            
        time.sleep(2)
    else:
        print("   ⚠️ Timeout: Worker never completed the job. Check Java Worker logs.")

if __name__ == "__main__":
    run_test()