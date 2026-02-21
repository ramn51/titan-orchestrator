import time
import dagster as dg
from titan_sdk import TitanClient, TitanJob

class TitanEngine(dg.ConfigurableResource):
    """
    The official bridge to the Titan Distributed Engine.
    Handles TCP dispatch and synchronous polling.
    """
    def run_task(self, context: dg.AssetExecutionContext, script_path: str, requirement: str = "GENERAL", affinity: bool = False):
        client = TitanClient()
        
        # Extract the logical asset name (e.g., "raw_data")
        step_name = context.asset_key.path[-1]
        
        t_job = TitanJob(
            job_id=step_name,
            filename=script_path,
            job_type="RUN_PAYLOAD", 
            requirement=requirement,
            affinity=affinity, 
            is_archive=False 
        )
        
        context.log.info(f"🚀 Routing '{step_name}' to Titan (Req: {requirement}, Affinity: {affinity})")
        client.submit_job(t_job)
        
        # Synchronous Polling Loop via your dedicated Status API
        master_job_id = f"DAG-{step_name}"
        while True:
            status = client.get_job_status(master_job_id)
            if status == "COMPLETED":
                # OPTIONAL: Fetch logs even on success for full visibility
                worker_logs = client.fetch_logs(master_job_id)
                context.log.info(f"✅ Titan Worker Logs:\n{worker_logs}")
                context.log.info(f"✅ Titan finished physical execution of '{step_name}'!")
                return True
                
            elif status == "FAILED":
                # CRITICAL: Fetch logs on failure to debug within Dagster
                worker_logs = client.fetch_logs(master_job_id)
                context.log.error(f"❌ Titan Worker failed. Remote Logs:\n{worker_logs}")
                raise Exception(f"❌ Titan Worker failed on '{step_name}'.")
                
            time.sleep(2)

# --- 2. The Logical Assets ---
@dg.asset
def raw_data(context: dg.AssetExecutionContext, titan: TitanEngine):
    """Step 1: General extraction task."""
    titan.run_task(context, script_path="extract.py", requirement="GENERAL", affinity=False)

@dg.asset
def trained_model(context: dg.AssetExecutionContext, titan: TitanEngine, raw_data):
    """Step 2: Heavy GPU task. Forced to same node as Step 1."""
    titan.run_task(context, script_path="train.py", requirement="GPU", affinity=True)

@dg.asset
def evaluation_metrics(context: dg.AssetExecutionContext, titan: TitanEngine, trained_model):
    """Step 3: Lightweight CPU task. No affinity needed."""
    titan.run_task(context, script_path="eval.py", requirement="GENERAL", affinity=False)

# --- 3. Workspace Configuration ---
defs = dg.Definitions(
    assets=[raw_data, trained_model, evaluation_metrics],
    # We bind the Titan engine to the workspace here
    resources={"titan": TitanEngine()} 
)