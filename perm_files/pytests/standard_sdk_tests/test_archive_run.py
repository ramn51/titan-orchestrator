import os,sys
import time


from titan_sdk import TitanClient, TitanJob

# 1. Setup Dummy Project
PROJECT_NAME = "agent_v1"
os.makedirs(PROJECT_NAME, exist_ok=True)

# Create main.py (Entry Point)
with open(f"{PROJECT_NAME}/main.py", "w") as f:
    f.write("""
import utils
import time
print(" [MAIN] Agent Started!")
print(f" [MAIN] Imported Utils Message: {utils.get_message()}")
print(" [MAIN] Sleeping to simulate work...")
time.sleep(2)
print(" [MAIN] Done.")
""")

# Create utils.py (Dependency)
with open(f"{PROJECT_NAME}/utils.py", "w") as f:
    f.write("""
def get_message():
    return "Dependency Loading Works!"
""")

# 2. Run Test
client = TitanClient()

print("\n--- STEP 1: Upload Project ---")
resp = client.upload_project_folder(PROJECT_NAME)
print(f"Upload Response: {resp}")

print("\n--- STEP 2: Submit Archive Job ---")
# Pointer format: zip_name.zip/entry_file
pointer = f"{PROJECT_NAME}.zip/main.py"

# Create Job with is_archive=True
job = TitanJob(
    job_id=f"ARCHIVE-TEST-{int(time.time())}", 
    filename=pointer, 
    job_type="TASK", 
    is_archive=True
)

resp = client.submit_job(job)
print(f"Job Submission: {resp}")

# 3. Monitor
print("\n--- STEP 3: Tailing Logs ---")
for _ in range(5):
    logs = client.fetch_logs(job.id)
    if "Log not found" not in logs:
        print(logs)
        if "Done" in logs:
            break
    time.sleep(1)

# Cleanup
import shutil
shutil.rmtree(PROJECT_NAME)