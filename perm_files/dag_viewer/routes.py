"""HTTP boundary for the DAG viewer; engine transport stays in server_dashboard."""

import json
import io
import os
import re
from pathlib import Path

from flask import Blueprint, abort, jsonify, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

from .catalog import DagCatalog
from .graph import PREVIEW_LIMIT, dot_graph, preview_dag, render_graph
from .timeline import merge_persisted_spans, timeline_for
from .submission import GraphError, NAME, normalize, payload_for, read_manifest, redeploy, submit, write_manifest


def create_blueprint(communicate, stats_opcode, logs_opcode, manifest_path, yaml_root,
                     submit_opcode=0x04, scripts_root=None, cancel_opcode=0x56):
    viewer = Blueprint("dag_viewer", __name__)
    catalog = DagCatalog(Path(manifest_path), Path(yaml_root))
    scripts = Path(scripts_root or Path(__file__).resolve().parents[1] / "dag_samples")
    workspace = Path(os.environ.get("TITAN_DAG_WORKSPACE_DIR", Path(manifest_path).parent / "titan_workspace"))
    draft_path = Path(manifest_path).parent / ".dag_constructor_states.json"

    def pipeline_page(dags, query, page_number):
        matches = [dag for dag in dags if query.casefold() in dag["name"].casefold()]
        pages = max(1, (len(matches) + 29) // 30)
        page_number = max(1, min(page_number, pages))
        return matches[(page_number - 1) * 30:page_number * 30], len(matches), pages, page_number

    def snapshot():
        raw = communicate(stats_opcode, "")
        if not raw:
            return {"online": False, "dags": catalog.snapshot(None)}
        try:
            stats = json.loads(raw[raw.index("{"):])
        except (ValueError, TypeError):
            return {"online": False, "dags": catalog.snapshot(None)}
        endpoints = []
        if any(worker.get("services") for worker in stats.get("workers", [])):
            board_raw = communicate(stats_opcode, "queue|500")
            try:
                endpoints = json.loads(board_raw[board_raw.index("{"):]).get("services", [])
            except (ValueError, TypeError, AttributeError):
                pass
        return {"online": True, "dags": catalog.snapshot(stats, endpoints)}

    def engine_stats(dag=None, run_end=None):
        raw = communicate(stats_opcode, "")
        try:
            root = Path(manifest_path).parent
            return merge_persisted_spans(
                json.loads(raw[raw.index("{"):]) if raw else {},
                root / ".titan_execution_timeline.jsonl",
                root / "telemetry" if dag else None,
                int(dag.get("submitted") or 0) if dag else 0,
                run_end,
                tuple(job["id"] for job in dag.get("jobs", [])) if dag else ())
        except (ValueError, TypeError):
            return {}

    def current_dag(dag_id):
        return next((dag for dag in snapshot()["dags"] if dag["id"] == dag_id), None)

    def hitl_gates(dag_id):
        dag = current_dag(dag_id)
        if dag is None:
            abort(404)
        gates = []
        for job in dag["jobs"]:
            message = job.get("spec", {}).get("hitl_message")
            if not job["id"].startswith("DAG-hitl-gate-") or not message:
                continue
            decision = communicate(0x61, "titan:hitl:status:" + job["id"])
            gates.append({"id": job["id"], "message": message,
                          "decision": decision, "job_status": job["status"]})
        return gates

    @viewer.get("/api/dag/hitl/<path:dag_id>")
    def list_hitl_gates(dag_id):
        return jsonify({"gates": hitl_gates(dag_id)})

    @viewer.post("/api/dag/hitl/<path:dag_id>/<path:gate_id>")
    def decide_hitl_gate(dag_id, gate_id):
        gate = next((item for item in hitl_gates(dag_id) if item["id"] == gate_id), None)
        if gate is None:
            return jsonify({"error": "HITL gate not found in this pipeline."}), 404
        decision = (request.get_json(silent=True) or {}).get("decision")
        if decision not in ("APPROVED", "REJECTED"):
            return jsonify({"error": "Decision must be APPROVED or REJECTED."}), 400
        if gate["job_status"] != "RUNNING" or gate["decision"] != "WAITING":
            return jsonify({"error": "Gate is not currently waiting for a decision."}), 409
        response = communicate(0x60, f"titan:hitl:status:{gate_id}|{decision}")
        if response != "OK":
            return jsonify({"error": f"Could not save decision: {response or 'No response'}"}), 502
        return jsonify({"status": "ok", "decision": decision})

    @viewer.post("/api/dag/cancel/<path:dag_id>/<path:job_id>")
    def cancel_job(dag_id, job_id):
        dag = current_dag(dag_id)
        job = next((item for item in dag["jobs"] if item["id"] == job_id), None) if dag else None
        if job is None:
            return jsonify({"error": "Job not found in this pipeline."}), 404
        if job["status"] not in {"WAITING", "PENDING", "RUNNING"}:
            return jsonify({"error": "This job has already finished."}), 409
        response = communicate(cancel_opcode, job_id)
        if not response:
            return jsonify({"error": "Master did not respond to cancellation."}), 502
        if response.startswith(("ERROR", "ALREADY_TERMINAL")):
            return jsonify({"error": response}), 409 if response.startswith("ALREADY_TERMINAL") else 502
        return jsonify({"status": "ok", "response": response})

    @viewer.post("/api/dag/cancel/<path:dag_id>")
    def cancel_pipeline(dag_id):
        dag = current_dag(dag_id)
        if dag is None:
            return jsonify({"error": "Pipeline not found."}), 404
        active = [job["id"] for job in dag["jobs"] if job["status"] in {"WAITING", "PENDING", "RUNNING"}]
        if not active:
            return jsonify({"error": "Pipeline has no active jobs."}), 409
        responses = {job_id: communicate(cancel_opcode, job_id) for job_id in active}
        errors = {job_id: response for job_id, response in responses.items()
                  if not response or response.startswith("ERROR")}
        if errors:
            return jsonify({"error": "Some jobs could not be cancelled.", "responses": responses}), 502
        return jsonify({"status": "ok", "responses": responses})

    def workspace_files(dag_id, job_id):
        dag = next((item for item in snapshot()["dags"] if item["id"] == dag_id), None)
        if not dag or not any(job["id"] == job_id for job in dag["jobs"]):
            abort(404)
        shared = (workspace / "shared").resolve()
        run_started = int(dag.get("submitted") or 0)
        if not shared.is_dir():
            return []
        files = []
        log = shared / f"{job_id}.log"
        metadata = catalog.metadata(job_id)
        log_offset = metadata.get("log_offset", 0) if isinstance(metadata, dict) else 0
        artifact_root = shared / "artifacts" / job_id
        candidates = [log]
        if artifact_root.is_dir() and not artifact_root.is_symlink() and artifact_root.resolve().is_relative_to(shared):
            candidates.extend(sorted(artifact_root.rglob("*")))
        for path in candidates:
            if len(files) >= 200:
                break
            if not path.is_file() or not path.resolve().is_relative_to(shared):
                continue
            if path != log and not path.resolve().is_relative_to(artifact_root.resolve()):
                continue
            if run_started and path.stat().st_mtime_ns // 1_000_000 < run_started:
                continue
            relative = path.relative_to(shared).as_posix()
            size = max(0, path.stat().st_size - log_offset) if path == log and isinstance(log_offset, int) else path.stat().st_size
            files.append({"name": relative, "size": size,
                          "kind": "Job log" if path == log else "Job artifact",
                          "url": url_for("dag_viewer.download_file", dag_id=dag_id,
                                         job_id=job_id, filename=relative)})
        return files

    def shared_files():
        shared = (workspace / "shared").resolve()
        if not shared.is_dir():
            return []
        files = []
        for path in sorted(shared.rglob("*")):
            if len(files) >= 200:
                break
            if not path.is_file() or not path.resolve().is_relative_to(shared):
                continue
            relative = path.relative_to(shared)
            if path.suffix == ".log" or relative.parts[0] == "artifacts":
                continue
            name = relative.as_posix()
            files.append({"name": name, "size": path.stat().st_size,
                          "kind": "Unassigned shared file",
                          "url": url_for("dag_viewer.download_shared_file", filename=name)})
        return files

    @viewer.get("/api/dag/files/<path:dag_id>/<path:job_id>")
    def list_files(dag_id, job_id):
        return jsonify({"files": workspace_files(dag_id, job_id)})

    @viewer.get("/api/dag/shared_files")
    def list_shared_files():
        return jsonify({"files": shared_files()})

    @viewer.get("/api/dag/files/<path:dag_id>/<path:job_id>/<path:filename>")
    def download_file(dag_id, job_id, filename):
        if filename not in {item["name"] for item in workspace_files(dag_id, job_id)}:
            abort(404)
        if filename == f"{job_id}.log":
            metadata = catalog.metadata(job_id)
            offset = metadata.get("log_offset") if isinstance(metadata, dict) else None
            if isinstance(offset, int) and offset >= 0:
                content = (workspace / "shared" / filename).read_bytes()
                return send_file(io.BytesIO(content[min(offset, len(content)):]),
                                 as_attachment=True, download_name=filename, mimetype="text/plain")
        return send_file((workspace / "shared" / filename).resolve(), as_attachment=True)

    @viewer.get("/api/dag/shared_file/<path:filename>")
    def download_shared_file(filename):
        if filename not in {item["name"] for item in shared_files()}:
            abort(404)
        return send_file((workspace / "shared" / filename).resolve(), as_attachment=True)

    @viewer.get("/api/dag_status")
    def status():
        data = snapshot()
        if request.args.get("view") == "page":
            query = request.args.get("q", "")[:100].strip()
            page_dags, total, pages, number = pipeline_page(
                data["dags"], query, request.args.get("page", 1, type=int))
            selected_id = request.args.get("selected")
            selected = next((dag for dag in data["dags"] if dag["id"] == selected_id), None)
            summaries = [{key: dag[key] for key in ("id", "status", "completed", "total")}
                         for dag in page_dags]
            if selected:
                summaries = [dag for dag in summaries if dag["id"] != selected_id]
                summaries.append(selected)
            data = {"online": data["online"], "dags": summaries,
                    "total": total, "pages": pages, "page": number}
        return jsonify(data), (200 if data["online"] else 502)

    @viewer.get("/api/dag/graph/<path:dag_id>/<format_name>")
    def graph_export(dag_id, format_name):
        dag = current_dag(dag_id)
        if dag is None:
            abort(404)
        requested_run = request.args.get("run", type=int)
        if requested_run and requested_run != dag["submitted"]:
            dag = catalog.historical_snapshot(dag["name"], requested_run)
            if dag is None:
                abort(404)
        if format_name == "svg":
            content, mimetype = render_graph(dag, standalone=True), "image/svg+xml"
        elif format_name == "dot":
            content, mimetype = dot_graph(dag), "text/vnd.graphviz"
        else:
            abort(404)
        filename = secure_filename(dag_id) + "." + format_name
        return send_file(io.BytesIO(content.encode("utf-8")), mimetype=mimetype,
                         as_attachment=True, download_name=filename)

    @viewer.get("/api/dag/timeline/<path:dag_id>")
    def timeline_data(dag_id):
        stats = engine_stats()
        dag = next((item for item in catalog.snapshot(stats) if item["id"] == dag_id), None)
        if dag is None:
            return jsonify({"error": "Pipeline not found."}), 404
        requested_run = request.args.get("run", type=int)
        runs = catalog.runs_for(dag["name"])
        run_end = None
        if requested_run and requested_run != dag["submitted"]:
            dag = catalog.historical_snapshot(dag["name"], requested_run)
            if dag is None:
                return jsonify({"error": "Run not found."}), 404
            run_end = min((run["run_ts"] for run in runs if run["run_ts"] > requested_run), default=None)
        stats = engine_stats(dag, run_end)
        span_limit = max(0, min(25000, request.args.get("limit", default=5000, type=int)))
        error_offset = max(0, request.args.get("error_offset", default=0, type=int))
        error_limit = max(0, min(100, request.args.get("error_limit", default=100, type=int)))
        return jsonify(timeline_for(dag, stats, run_end, span_limit, error_offset, error_limit))

    @viewer.get("/timeline")
    def timeline_page():
        dags = snapshot()["dags"]
        requested = request.args.get("dag")
        selected = next((item for item in dags if item["id"] == requested), None)
        if selected is None and dags:
            selected = dags[0]
        runs = catalog.runs_for(selected["name"]) if selected else []
        requested_run = request.args.get("run", type=int)
        if selected and requested_run and requested_run != selected["submitted"]:
            selected = catalog.historical_snapshot(selected["name"], requested_run)
            if selected is None:
                abort(404)
        return render_template("execution_timeline.html", dags=dags, selected=selected, runs=runs)

    @viewer.get("/dag-constructor")
    def constructor():
        return render_template("dag_constructor.html")

    @viewer.get("/api/dag/scripts")
    def available_scripts():
        return jsonify(sorted(str(path.relative_to(scripts)) for path in scripts.rglob("*.py")
                              if path.is_file())) if scripts.exists() else jsonify([])

    @viewer.get("/api/perm_files")
    def perm_files():
        return jsonify({"files": available_scripts().json})

    @viewer.post("/api/upload_script")
    def upload_script():
        uploaded = request.files.get("file")
        if not uploaded:
            return jsonify({"error": "Choose a Python script."}), 400
        filename = secure_filename(uploaded.filename or "")
        if not filename.endswith(".py") or not filename or uploaded.content_length and uploaded.content_length > 256_000:
            return jsonify({"error": "Upload a Python script under 256 KB."}), 400
        content = uploaded.read(256_001)
        if len(content) > 256_000:
            return jsonify({"error": "Script exceeds 256 KB."}), 400
        scripts.mkdir(parents=True, exist_ok=True)
        destination = scripts / filename
        if destination.exists():
            return jsonify({"error": "A script with that name already exists."}), 409
        destination.write_bytes(content)
        return jsonify({"filename": filename})

    @viewer.get("/api/dag/constructor_states")
    def constructor_states():
        drafts = read_manifest(draft_path)
        return jsonify({"states": [{"name": name} for name in sorted(drafts)]})

    @viewer.get("/api/dag/constructor_state/<path:name>")
    def constructor_state(name):
        draft = read_manifest(draft_path).get(name)
        return (jsonify(draft), 200) if draft else (jsonify({"error": "Draft not found"}), 404)

    @viewer.post("/api/dag/save_draft")
    def save_draft():
        body = request.get_json(silent=True) or {}
        name = str(body.get("name", "")).strip()
        nodes, edges = body.get("nodes"), body.get("edges")
        if not NAME.fullmatch(name) or not isinstance(nodes, list) or not isinstance(edges, list) or len(nodes) > 100 or len(edges) > 300:
            return jsonify({"error": "Invalid draft name or graph size."}), 400
        drafts = read_manifest(draft_path)
        drafts[name] = {"nodes": nodes, "edges": edges}
        write_manifest(draft_path, drafts)
        return jsonify({"status": "ok"})

    @viewer.post("/api/dag/submit")
    def submit_dag():
        try:
            body = request.get_json(silent=True)
            name, jobs = normalize(body, scripts)
            response = submit(communicate, submit_opcode, manifest_path, name, jobs,
                              payload_for(jobs))
            if isinstance(body.get("_nodes"), list) and isinstance(body.get("_edges"), list):
                drafts = read_manifest(draft_path)
                drafts[name] = {"nodes": body["_nodes"], "edges": body["_edges"]}
                write_manifest(draft_path, drafts)
            return jsonify({"status": "ok", "response": response, "dag_id": f"DAG-{name}"})
        except GraphError as exc:
            return jsonify({"error": str(exc)}), 400
        except OSError as exc:
            return jsonify({"error": f"Could not read script or save manifest: {exc}"}), 500

    @viewer.post("/api/dag/redeploy/<path:dag_id>")
    def redeploy_dag(dag_id):
        name = dag_id[4:] if dag_id.startswith("DAG-") else dag_id
        try:
            response = redeploy(communicate, submit_opcode, manifest_path, name)
            return jsonify({"status": "ok", "response": response, "dag_id": f"DAG-{name}"})
        except GraphError as exc:
            return jsonify({"error": str(exc)}), 400
        except OSError as exc:
            return jsonify({"error": f"Could not save manifest: {exc}"}), 500

    @viewer.get("/dags")
    @viewer.get("/dags/<path:dag_id>")
    def page(dag_id=None):
        data = snapshot()
        selected = next((d for d in data["dags"] if d["id"] == dag_id), None)
        query = request.args.get("q", "")[:100].strip()
        requested_page = request.args.get("page", 1, type=int)
        if dag_id and "page" not in request.args and not query:
            index = next((i for i, dag in enumerate(data["dags"]) if dag["id"] == dag_id), 0)
            requested_page = index // 30 + 1
        page_dags, match_count, page_count, page_number = pipeline_page(data["dags"], query, requested_page)
        if selected is None and page_dags and dag_id is None:
            selected = page_dags[0]
        runs = catalog.runs_for(selected["name"]) if selected else []
        requested_run = request.args.get("run", type=int)
        historical = bool(selected and requested_run and requested_run != selected["submitted"])
        if historical:
            selected = catalog.historical_snapshot(selected["name"], requested_run)
            if selected is None:
                abort(404)
        job_id = request.args.get("job")
        job = next((j for j in selected["jobs"] if j["id"] == job_id), None) if selected else None
        if job is None and selected and selected["jobs"]:
            job = next((item for item in selected["jobs"] if not item["parents"]), selected["jobs"][0])
        logs = None
        if job and not historical:
            metadata = catalog.metadata(job["id"])
            offset = metadata.get("log_offset") if isinstance(metadata, dict) else None
            log_path = workspace / "shared" / f"{job['id']}.log"
            if isinstance(offset, int) and offset >= 0 and log_path.is_file():
                content = log_path.read_bytes()
                logs = content[min(offset, len(content)):].decode("utf-8", errors="replace")[-200_000:]
            else:
                logs = communicate(logs_opcode, job["id"])
        return render_template("dag_viewer.html", data=data, page_dags=page_dags,
                               match_count=match_count, page_count=page_count,
                               page_number=page_number, query=query,
                               preview_limit=PREVIEW_LIMIT, selected=selected, job=job, runs=runs,
                               historical=historical, logs=logs,
                               graph=render_graph(preview_dag(selected), job["id"] if job else None,
                                                  requested_run if historical else None)
                               if selected else "")

    return viewer
