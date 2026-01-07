import time
import json
import os
from openai import OpenAI
from titan_sdk import TitanClient, TitanJob

# --- CONFIGURATION ---
client = OpenAI(api_key="API_KEY")

# --- THE DOCTOR'S BRAIN ---
SYSTEM_PROMPT = """
You are the "Code Healer", a specialized debugging agent on Titan.
Your ONLY goal is to fix crashing Python scripts.

AVAILABLE TOOLS:
1. TEST: Runs a script to see if it works.
   - Action: "RUN", file: "test_runner.py", args: "<target_file>"
   
2. READ: Reads the content of a file.
   - Action: "RUN", file: "file_manager.py", args: "READ <target_file>"

3. WRITE: Overwrites a file with fixed code.
   - Action: "RUN", file: "file_manager.py", args: "WRITE <target_file> <BASE64_CONTENT>"

STRATEGY (The Debug Loop):
1. DIAGNOSE: Run 'test_runner.py' on the target.
2. ANALYZE: If it fails, Read the file. Look at the Error Log.
3. FIX: Rewrite the file with the fix.
4. VERIFY: Run 'test_runner.py' again.
5. FINISH: If the logs show "[PASS]", return action "FINISH".
"""

def ask_doctor(history, user_goal):
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"PATIENT: {user_goal}"}
    ]
    # Feed previous attempts into the context
    for log in history:
        messages.append({"role": "user", "content": f"PREVIOUS_ATTEMPT_LOGS: {log}"})

    completion = client.chat.completions.create(
        model="gpt-4-turbo",
        messages=messages,
        temperature=0.0
    )
    raw = completion.choices[0].message.content
    # (Insert your clean_json_response helper here)
    return json.loads(raw.replace("```json", "").replace("```", "").strip())

def run_healer(target_file):
    titan = TitanClient()
    history = []
    
    print(f"🚑 HEALER DISPATCHED for: {target_file}")
    
    for i in range(3): # Max 5 attempts to fix
        print(f"\n--- Cycle {i+1} ---")
        
        # 1. Ask the Doctor what to do
        plan = ask_doctor(history, f"The file {target_file} is broken. Fix it.")
        print(f"💊 Prescription: {plan.get('comment')}")
        
        if plan['action'] == "FINISH":
            print("✅ PATIENT CURED! Exiting.")
            return

        # 2. Execute the treatment
        jobs = []
        for j in plan['jobs']:
            job = TitanJob(
                # job_id=f"HEAL-{i}-{j['file'][:4]}",
                job_id = f"DAG-HEALER-CYCLE-{i}-{j['file'][:4]}",
                filename=j['file'],
                job_type="RUN_PAYLOAD",
                args=j.get('args', "") # <--- Uses your new Args feature!
            )
            jobs.append(job)

        titan.submit_dag(f"Healer_Cycle_{i}", jobs)
        
        # 3. Read the Chart (Logs)
        time.sleep(4) # Wait for execution
        cycle_log = ""
        for j in jobs:
            log = titan.fetch_logs(j.id)
            print(f"   📋 Result: {log.strip()[:100]}...")
            cycle_log += f"Job {j.filename} Result:\n{log}\n"
        
        history.append(cycle_log)

if __name__ == "__main__":
    # Make sure you have 'buggy_math.py' created first!
    run_healer("buggy_math.py")