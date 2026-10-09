"""Constructor validation and run-boundary checks."""

import json
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dag_viewer.submission import GraphError, normalize, payload_for, redeploy, submit
from dag_viewer.routes import create_blueprint
from flask import Flask
from titan_sdk.dag_journal import latest_run, runs_for as journal_runs_for


def test_submit_and_redeploy(tmp_path):
    tmp = tmp_path
    script = tmp / "task.py"
    script.write_text("print('done')", encoding="utf-8")
    body = {"name": "DEMO", "jobs": [
        {"id": "ONE", "filename": "task.py"},
        {"id": "TWO", "filename": "task.py", "depends_on": ["ONE"]},
    ]}
    name, jobs = normalize(body, tmp)
    payload = payload_for(jobs)
    assert "TWO|RUN_PAYLOAD|task.py|" in payload
    assert payload.endswith("[ONE]")
    manifest = tmp / "manifest.json"
    calls = []
    rpc = lambda opcode, value: calls.append((opcode, value)) or "DAG_ACCEPTED"
    submit(rpc, 4, manifest, name, jobs, payload)
    saved = latest_run(manifest, "DEMO")
    assert saved["jobs"]["DAG-TWO"]["deps"] == ["DAG-ONE"]
    assert saved["payload"] == payload
    first_run = saved["run_ts"]
    time.sleep(0.002)
    redeploy(rpc, 4, manifest, "DEMO")
    assert calls == [(4, payload), (4, payload)]
    assert latest_run(manifest, "DEMO")["run_ts"] > first_run
    assert len(journal_runs_for(manifest, "DEMO")) == 2


def test_reject_cycle_and_escape(tmp_path):
    tmp = tmp_path
    (tmp / "task.py").write_text("pass", encoding="utf-8")
    cyclic = {"name": "BAD", "jobs": [
        {"id": "A", "filename": "task.py", "depends_on": ["B"]},
        {"id": "B", "filename": "task.py", "depends_on": ["A"]},
    ]}
    try:
        normalize(cyclic, tmp)
        assert False, "cycle accepted"
    except GraphError as exc:
        assert "cycle" in str(exc)
    cyclic["jobs"][1]["depends_on"] = []
    cyclic["jobs"][0]["filename"] = "../outside.py"
    try:
        normalize(cyclic, tmp)
        assert False, "path escape accepted"
    except GraphError as exc:
        assert "under" in str(exc)


def test_constructor_routes(tmp_path):
    tmp = tmp_path
    scripts = tmp / "scripts"
    scripts.mkdir()
    (scripts / "task.py").write_text("print('ok')", encoding="utf-8")
    manifest = tmp / "manifest.json"
    app = Flask(__name__)
    app.register_blueprint(create_blueprint(lambda op, payload: "DAG_ACCEPTED", 9, 22,
                                             manifest, tmp, scripts_root=scripts))
    with app.test_client() as client:
        assert client.get("/api/perm_files").json == {"files": ["task.py"]}
        body = {"name": "FROM_UI", "jobs": [{"id": "first", "filename": "task.py",
                "job_type": "run", "depends_on": []}],
                "_nodes": [{"id": "node-1"}], "_edges": []}
        assert client.post("/api/dag/submit", json=body).status_code == 200
        assert client.get("/api/dag/constructor_state/FROM_UI").json["nodes"][0]["id"] == "node-1"
        assert client.post("/api/dag/redeploy/DAG-FROM_UI").status_code == 200
        assert "DAG-first" in latest_run(manifest, "FROM_UI")["jobs"]


if __name__ == "__main__":
    for case in (test_submit_and_redeploy, test_reject_cycle_and_escape, test_constructor_routes):
        with TemporaryDirectory() as directory:
            case(Path(directory))
        print(f"PASS {case.__name__}")
