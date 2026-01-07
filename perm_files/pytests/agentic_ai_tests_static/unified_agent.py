import time
import json
from titan_sdk import TitanClient, TitanJob


# --- MOCK LLM (Replace with OpenAI/DeepSeek API) ---
def ask_llm(history, user_goal):
    """
    Decides the next move based on conversation history.
    Returns a JSON object: {"action": "SUBMIT", "jobs": [...]} OR {"action": "FINISH"}
    """
    
    # SCENARIO 1: Mode A (Architect) - LLM sees a request for a full system
    if len(history) == 0 and "dashboard" in user_goal:
        print("🧠 LLM: Detecting complex goal. Generating full DAG plan (Mode A)...")
        return {
            "action": "SUBMIT",
            "comment": "Deploying infrastructure in one go.",
            "jobs": [
                {"id": "SPAWN", "file": "Worker.jar", "type": "DEPLOY_PAYLOAD", "port": 8086},
                {"id": "CALC",  "file": "calc.py",    "type": "RUN_PAYLOAD",    "deps": ["SPAWN"]},
                {"id": "VIEW",  "file": "log_viewer.py","type": "DEPLOY_PAYLOAD", "port": 9991, "deps": ["CALC"]}
            ]
        }

    # SCENARIO 2: Mode B (ReAct) - LLM sees a request for exploration
    elif len(history) == 0 and "explore" in user_goal:
        print("🧠 LLM: Detecting exploration goal. Running step 1 (Mode B)...")
        return {
            "action": "SUBMIT", 
            "comment": "Let's check if the environment is healthy first.",
            "jobs": [
                {"id": "CHECK_ENV", "file": "calc.py", "type": "RUN_PAYLOAD"}
            ]
        }
    
    # SCENARIO 2 (Continued): LLM reacts to the output of step 1
    elif len(history) > 0 and "Result: 5050" in history[-1]:
        print("🧠 LLM: Previous step succeeded. Finishing.")
        return {"action": "FINISH", "comment": "Environment is healthy."}

    return {"action": "FINISH", "comment": "No further actions needed."}


# --- THE UNIFIED LOOP ---
def run_agent(user_goal):
    client = TitanClient()
    history = []
    iteration = 0
    
    print(f"🤖 Agent initialized with goal: \"{user_goal}\"\n")

    while True:
        iteration += 1
        print(f"--- Iteration {iteration} ---")
        
        # 1. THINK: Ask LLM what to do
        response = ask_llm(history, user_goal)
        
        if response["action"] == "FINISH":
            print(f"✅ Agent Completed: {response['comment']}")
            break

        print(f"🤔 LLM Plan: {response['comment']}")

        # 2. ACT: Convert JSON jobs to TitanSDK objects
        titan_jobs = []
        target_ids = [] # Keep track of IDs to poll for logs
        
        for j in response["jobs"]:
            # Auto-prefix with DAG- to ensure Shared Workspace access across iterations
            # This is the secret sauce for persistence!
            safe_id = f"DAG-{j['id']}" 
            safe_deps = [f"DAG-{d}" for d in j.get("deps", [])]
            
            job = TitanJob(
                job_id=safe_id,
                filename=j['file'],
                job_type=j['type'],
                parents=safe_deps,
                port=j.get('port', 0)
            )
            titan_jobs.append(job)
            target_ids.append(safe_id)

        # Submit the batch (Length 1 or Length 10, SDK doesn't care)
        client.submit_dag(f"Agent-Iter-{iteration}", titan_jobs)
        
        # 3. OBSERVE: Wait for this specific batch to finish
        # In a real system, you might poll OP_STATS until "COMPLETED"
        print("⏳ Waiting for execution...")
        time.sleep(5) 
        
        # Fetch logs for all jobs in this batch to feed back to LLM
        batch_logs = ""
        for tid in target_ids:
            log = client.fetch_logs(tid)
            print(f"   📄 Logs from {tid}: {log.strip()[:50]}...") # Truncated for display
            batch_logs += f"Job {tid} Output: {log}\n"
            
        history.append(batch_logs)

if __name__ == "__main__":
    # UNCOMMENT ONE TO TEST:
    
    # Test Mode A (Architect)
    run_agent("I want to deploy a full dashboard environment.")
    
    # Test Mode B (Researcher)
    # run_agent("I want to explore the environment.")