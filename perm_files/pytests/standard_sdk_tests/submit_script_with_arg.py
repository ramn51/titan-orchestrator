import sys
import os


current_dir = os.path.dirname(os.path.abspath(__file__))
from titan_sdk import TitanClient, TitanJob

def run_test():
    client = TitanClient()
    
    # 2. CALCULATE ABSOLUTE PATH
    # This says: "The file is in the exact same folder as I am."
    target_file = os.path.join(current_dir, "args_tester.py")
    
    print(f"[TEST] Submitting file: {target_file}")
    print("[TEST] With arguments: 'Hello Titan 2025'")

    # 3. Create Job with Absolute Path
    job = TitanJob(
        job_id="TEST_ARGS_001", 
        filename=target_file,  # <--- Passing full path
        job_type="RUN_PAYLOAD", 
        args="Hello Titan 2025" 
    )

    client.submit_job(job)
    print("[TEST] Job Submitted. Check Worker Logs!")

if __name__ == "__main__":
    # Verify the file actually exists before running
    target = os.path.join(current_dir, "args_tester.py")
    if not os.path.exists(target):
        print(f"❌ ERROR: args_tester.py does not exist at: {target}")
        print("Please create it inside 'perm_files/pytests/standard_sdk_tests/'")
    else:
        run_test()