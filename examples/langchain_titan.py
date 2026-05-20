"""
Minimal LangChain + Titan validation example.

Tests the five core Titan tools as LangChain tools, submits a 2-job DAG,
polls until complete, and reads the logs.

Requirements (optional — only needed for the agent portion):
    pip install langchain-core langchain-anthropic
    # or: pip install langchain-core langchain-openai

Run:
    python examples/langchain_titan.py

With no LangChain installed, the script validates the Titan tool wrappers
directly (no LLM involved). With LangChain installed it runs a full agent loop.
"""

import json
import os
import sys
import tempfile
import time

# ── Project root on path ───────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from titan_sdk.titan_sdk import TitanClient, TitanJob

# ── Core Titan tool implementations ───────────────────────────────────────────
# These are plain functions — no LangChain dependency.
# The @tool decorator is added below only when LangChain is available.

def _titan_submit_dag(dag_name: str, jobs_json: str) -> str:
    """Submit a multi-job DAG. jobs_json: [{job_id, script_content, parents, requirement}]"""
    job_defs = json.loads(jobs_json)
    client = TitanClient()
    jobs = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for jd in job_defs:
            path = os.path.join(tmpdir, f"{jd['job_id']}.py")
            with open(path, "w") as f:
                f.write(jd.get("script_content", "pass"))
            jobs.append(TitanJob(
                job_id=jd["job_id"],
                filename=path,
                requirement=jd.get("requirement", "GENERAL"),
                parents=jd.get("parents", []),
            ))
        result = client.submit_dag(dag_name, jobs)
    return str(result)


def _titan_get_status(job_id: str) -> str:
    """Get current job status. Handles DAG- prefix automatically."""
    client = TitanClient()
    prefixed = job_id if job_id.startswith("DAG-") else f"DAG-{job_id}"
    return client.get_job_status(prefixed)


def _titan_get_logs(job_id: str) -> str:
    """Fetch stdout/stderr for a job."""
    client = TitanClient()
    prefixed = job_id if job_id.startswith("DAG-") else f"DAG-{job_id}"
    return client.fetch_logs(prefixed)


def _titan_store_put(key: str, value: str) -> str:
    """Write a value to TitanStore (shared KV across jobs)."""
    return str(TitanClient().store_put(key, value))


def _titan_store_get(key: str) -> str:
    """Read a value from TitanStore."""
    return str(TitanClient().store_get(key))


# ── Direct validation (no LangChain required) ─────────────────────────────────

def run_direct_validation():
    """Validate all five Titan tools without any LangChain dependency."""
    ts = int(time.time())
    dag_name = f"lc-validate-{ts}"
    job_a = f"lc-write-{ts}"
    job_b = f"lc-read-{ts}"
    kv_key = f"lc:test:{ts}"

    print("\n── Direct tool validation (no LangChain) ──────────────────────")

    # 1. store_put
    print(f"[1] store_put({kv_key!r}, 'hello-titan')")
    print(f"    → {_titan_store_put(kv_key, 'hello-titan')}")

    # 2. store_get
    print(f"[2] store_get({kv_key!r})")
    print(f"    → {_titan_store_get(kv_key)}")

    # 3. submit_dag — job A writes to TitanStore, job B reads it
    jobs = json.dumps([
        {
            "job_id": job_a,
            "script_content": (
                f"import sys, os\n"
                f"sys.path.insert(0, {repr(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))})\n"
                f"from titan_sdk.titan_sdk import TitanClient\n"
                f"TitanClient().store_put('{kv_key}:result', 'written-by-job-a')\n"
                f"print('job-a: wrote to TitanStore')\n"
            ),
        },
        {
            "job_id": job_b,
            "script_content": (
                f"import sys, os\n"
                f"sys.path.insert(0, {repr(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))})\n"
                f"from titan_sdk.titan_sdk import TitanClient\n"
                f"val = TitanClient().store_get('{kv_key}:result')\n"
                f"print(f'job-b: read from TitanStore → {{val}}')\n"
            ),
            "parents": [job_a],
        },
    ])

    print(f"\n[3] submit_dag('{dag_name}', 2 jobs)")
    print(f"    → {_titan_submit_dag(dag_name, jobs)}")

    # 4. get_status — poll until terminal
    print(f"\n[4] polling get_status for both jobs...")
    terminal = {"COMPLETED", "FAILED", "DEAD", "ERROR"}
    for jid in [job_a, job_b]:
        deadline = time.time() + 30
        while time.time() < deadline:
            status = _titan_get_status(jid)
            print(f"    {jid} → {status}")
            if status.upper() in terminal:
                break
            time.sleep(2)

    # 5. get_logs
    print(f"\n[5] get_logs for both jobs:")
    for jid in [job_a, job_b]:
        logs = _titan_get_logs(jid)
        print(f"    [{jid}]\n    {logs.strip()}")

    print("\n── Validation complete ─────────────────────────────────────────\n")


# ── LangChain agent (optional) ────────────────────────────────────────────────

def run_langchain_agent():
    """Run a minimal LangChain agent loop using the Titan tools."""
    try:
        from langchain_core.tools import tool
        from langchain_core.messages import HumanMessage
    except ImportError:
        print("\n[LangChain] langchain-core not installed.")
        print("  Install with: pip install langchain-core langchain-anthropic")
        print("  Skipping agent run.\n")
        return

    # Wrap the plain functions as LangChain tools
    titan_submit_dag  = tool(_titan_submit_dag)
    titan_get_status  = tool(_titan_get_status)
    titan_get_logs    = tool(_titan_get_logs)
    titan_store_put   = tool(_titan_store_put)
    titan_store_get   = tool(_titan_store_get)

    lc_tools = [titan_submit_dag, titan_get_status, titan_get_logs,
                titan_store_put, titan_store_get]

    # Try Anthropic first, fall back to OpenAI
    llm = None
    try:
        from langchain_anthropic import ChatAnthropic
        llm = ChatAnthropic(model="claude-3-5-haiku-20241022").bind_tools(lc_tools)
        print("\n[LangChain] Using Anthropic claude-3-5-haiku")
    except ImportError:
        pass

    if llm is None:
        try:
            from langchain_openai import ChatOpenAI
            llm = ChatOpenAI(model="gpt-4o-mini").bind_tools(lc_tools)
            print("\n[LangChain] Using OpenAI gpt-4o-mini")
        except ImportError:
            pass

    if llm is None:
        print("\n[LangChain] No LLM provider found.")
        print("  Install one of: langchain-anthropic, langchain-openai")
        return

    print("[LangChain] Submitting agent query...")
    ts = int(time.time())
    response = llm.invoke([HumanMessage(
        content=(
            f"Submit a single Titan job with job_id 'lc-agent-{ts}' "
            f"in a DAG called 'lc-agent-dag-{ts}'. "
            f"The script should print 'hello from LangChain agent'. "
            f"Then check its status until it is COMPLETED, and show me the logs."
        )
    )])

    print(f"\n[LangChain] Agent response:\n{response.content}\n")
    if hasattr(response, "tool_calls") and response.tool_calls:
        print(f"[LangChain] Tool calls made: {[tc['name'] for tc in response.tool_calls]}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_direct_validation()
    run_langchain_agent()
