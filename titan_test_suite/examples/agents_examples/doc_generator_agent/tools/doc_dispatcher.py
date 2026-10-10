import os
import time
from titan_sdk import TitanClient, TitanJob

TARGET_DIRS = ["src/main/java/titan", "titan_sdk"]
CURRENT_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_SCRIPT_DIR, "."))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_SCRIPT_DIR, "../../../../../"))

# Point this to the actual script the workers will run
WORKER_SCRIPT_NAME = "doc_gemini_task.py" 
WORKER_SCRIPT_PATH = os.path.join(CURRENT_SCRIPT_DIR, WORKER_SCRIPT_NAME)

def seed_and_dispatch():
    client = TitanClient()
    batch_jobs = []

    print(f"Uploading execution logic to cluster: {WORKER_SCRIPT_NAME}")
    if os.path.exists(WORKER_SCRIPT_PATH):
        client.upload_file(WORKER_SCRIPT_PATH)
    else:
        print(f"Could not find {WORKER_SCRIPT_PATH}. Exiting.")
        return

    for target_dir in TARGET_DIRS:
        scan_path = os.path.join(PROJECT_ROOT, target_dir)
        if not os.path.exists(scan_path): continue

        for root, _, files in os.walk(scan_path):
            for file in files:
                if file.endswith((".java", ".py")) and file not in ["doc_dispatcher.py", WORKER_SCRIPT_NAME]:
                    abs_path = os.path.join(root, file)

                    abs_path = abs_path.replace("\\", "/")
                    
                    with open(abs_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # 1. Store RAW text in TitanStore
                    raw_key = f"RAW:{abs_path}"
                    client.store_put(raw_key, content)
                    
                    # 2. Dispatch job with the key as the argument
                    job_id = f"DOC_GEN_{file.replace('.', '_')}"
                    batch_jobs.append(TitanJob(
                        job_id=job_id,
                        filename=WORKER_SCRIPT_NAME, 
                        args=raw_key  
                    ))

    if batch_jobs:
        agent_run_id = f"docgen-{int(time.time() * 1000)}"
        client.submit_dag(f"AUTO_DOC_ENGINE_{agent_run_id}", batch_jobs,
                          agent_run_id=agent_run_id,
                          agent_run_name="Documentation Agent",
                          agent_stage="GENERATE", agent_stage_order=0)
        print(f"Seeding complete. {len(batch_jobs)} files dispatched for documentation.")
    else:
        print("No valid files found to document.")

if __name__ == "__main__":
    seed_and_dispatch()
