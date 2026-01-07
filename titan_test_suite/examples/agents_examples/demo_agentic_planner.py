import os
import json
import time
from openai import OpenAI
from titan_sdk import TitanClient, TitanJob

# --- CONFIGURATION ---
API_KEY = "YOUR_OPENAI_API_KEY"
client = OpenAI(api_key=API_KEY)

# --- THE SYSTEM PROMPT---
SYSTEM_PROMPT = """
You are the Orchestrator for the "Titan Distributed Supercomputer".
Your goal is to execute the USER'S REQUEST by scheduling jobs on the cluster.

AVAILABLE TOOLS (Scripts):
1. 'generate_code.py' -> Generates text/code files.
   - Use this for creating logic, UI, or config files.
   - It automatically writes files to the shared workspace.
2. 'calc.py' -> Performs CPU heavy calculation.
   - Use this to test system performance or run computation.
3. 'log_viewer.py' -> Web Service.
   - Use this if the user asks for a dashboard/server.
   - REQUIRES 'port' parameter (default 9991).
4. 'Worker.jar' -> Scaling Infrastructure.
   - Use this to spawn new worker nodes.

OUTPUT FORMAT:
You must strictly return a JSON object. Do not write markdown.
Schema:
{
  "action": "SUBMIT" or "FINISH",
  "comment": "Short explanation of your plan",
  "jobs": [
    {
      "id": "Short_Unique_ID",
      "file": "Filename from tools list",
      "type": "RUN_PAYLOAD" or "DEPLOY_PAYLOAD",
      "port": Integer (Optional, for services),
      "deps": ["ID_of_Parent_Job"] (Optional, for dependencies)
    }
  ]
}

RULES:
1. If the user request requires multiple steps (e.g., "Create A, then B, then Merge"), send them ALL in one JSON list with 'deps' linking them.
2. If the user request is simple (e.g., "Run calc"), send just one job.
3. If the History shows the task is done, return "action": "FINISH".
"""

def clean_json_response(response_text):
    """Helper to strip markdown ```json ... ``` if the LLM adds it."""
    clean_text = response_text.strip()
    if clean_text.startswith("```json"):
        clean_text = clean_text.replace("```json", "").replace("```", "")
    return clean_text

def ask_llm(history, user_goal):
    """Sends context to OpenAI and gets the Execution Plan."""
    
    # Construct the conversation
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"GOAL: {user_goal}"}
    ]
    
    # Add execution history so the LLM knows what happened previously
    for log in history:
        messages.append({"role": "user", "content": f"SYSTEM_LOGS: {log}"})

    print("🧠 Contacting OpenAI...")
    completion = client.chat.completions.create(
        model="gpt-4-turbo", # Or gpt-3.5-turbo
        messages=messages,
        temperature=0.0 # Strict logic
    )
    
    raw_content = completion.choices[0].message.content
    try:
        return json.loads(clean_json_response(raw_content))
    except json.JSONDecodeError:
        print(f"❌ JSON Error. Raw LLM output: {raw_content}")
        return {"action": "FINISH", "comment": "LLM Failed to produce JSON."}

# --- THE AGENT LOOP ---
def run_titan_agent(user_goal):
    titan = TitanClient()
    history = []
    iteration = 0
    
    print(f"\n🚀 STARTING AGENT\n🎯 GOAL: \"{user_goal}\"\n")

    while True:
        iteration += 1
        
        # 1. PLAN
        plan = ask_llm(history, user_goal)
        
        print(f"🤖 LLM Decision: {plan.get('comment', 'No comment')}")
        
        if plan["action"] == "FINISH":
            print("✅ Goal Achieved.")
            break
            
        # 2. COMPILE (JSON -> SDK Objects)
        sdk_jobs = []
        track_ids = []
        
        for j in plan["jobs"]:
            # Auto-prefix for Shared Workspace safety
            safe_id = f"DAG-{j['id']}"
            safe_deps = [f"DAG-{d}" for d in j.get("deps", [])]
            
            job = TitanJob(
                job_id=safe_id,
                filename=j['file'],
                job_type=j['type'],
                parents=safe_deps,
                port=j.get('port', 0)
            )
            sdk_jobs.append(job)
            track_ids.append(safe_id)

        # 3. EXECUTE
        batch_name = f"Agent_Iter_{iteration}"
        titan.submit_dag(batch_name, sdk_jobs)
        
        # 4. OBSERVE
        print("⏳ Waiting for Cluster execution...")
        time.sleep(5) # Give Titan time to process
        
        logs_accumulated = ""
        for tid in track_ids:
            logs = titan.fetch_logs(tid)
            # Basic log cleanup
            clean_log = logs.strip().replace('\n', ' ')
            print(f"   📄 Output ({tid}): {clean_log[:80]}...") 
            logs_accumulated += f"Job {tid} Output: {logs}\n"
            
        history.append(logs_accumulated)
        time.sleep(1)

if __name__ == "__main__":
    # --- TEST CASE 1: Simple Task ---
    # input("Press Enter to run SIMPLE TEST...")
    # run_titan_agent("Run a quick calculation to check if the system is online.")

    # --- TEST CASE 2: Complex Generation (The 'Snake Game' Scenario) ---
    input("Press Enter to run CODE GEN TEST...")
    run_titan_agent("Create a modular snake game. I need a logic module, a UI module, and a main script that merges them.")