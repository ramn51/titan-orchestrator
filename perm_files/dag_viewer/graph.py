"""A small, dependency-aware SVG renderer. No graph runtime in the browser."""

from collections import defaultdict, deque
from html import escape
import json
from urllib.parse import quote

PREVIEW_LIMIT = 120


def job_depths(jobs):
    """Topological layers without recursive calls for very deep workflows."""
    children = defaultdict(list)
    incoming = {}
    depths = {jid: 0 for jid in jobs}
    for jid, job in jobs.items():
        parents = {parent for parent in job["parents"] if parent in jobs}
        incoming[jid] = len(parents)
        for parent in parents:
            children[parent].append(jid)
    ready = deque(jid for jid, count in incoming.items() if count == 0)
    while ready:
        parent = ready.popleft()
        for child in children[parent]:
            depths[child] = max(depths[child], depths[parent] + 1)
            incoming[child] -= 1
            if incoming[child] == 0:
                ready.append(child)
    return depths  # Cyclic or malformed jobs remain at a finite layer.


def preview_dag(dag, limit=PREVIEW_LIMIT):
    """Keep the earliest dependency layers for the interactive graph."""
    jobs = {job["id"]: job for job in dag["jobs"]}
    depths = job_depths(jobs)
    ordered = sorted(jobs, key=lambda jid: (depths[jid], jid))
    return {**dag, "jobs": [jobs[jid] for jid in ordered[:limit]]}


def dot_graph(dag):
    """Portable full graph export, including nodes outside the preview."""
    lines = ["digraph TitanDAG {", "  rankdir=LR;"]
    ids = {job["id"] for job in dag["jobs"]}
    for job in dag["jobs"]:
        lines.append(f'  {json.dumps(job["id"], ensure_ascii=False)} '
                     f'[label={json.dumps(job["id"], ensure_ascii=False)}];')
    for job in dag["jobs"]:
        for parent in job["parents"]:
            if parent in ids:
                lines.append(f'  {json.dumps(parent, ensure_ascii=False)} -> '
                             f'{json.dumps(job["id"], ensure_ascii=False)};')
    return "\n".join(lines + ["}", ""])


EXPORT_STYLE = """
.edge{fill:none;stroke:#69847f;stroke-width:2}
.node rect{fill:#192c24;stroke:#47a77d;stroke-width:1.5}
.node circle{fill:#d7ffeb}
.node text{fill:#e4f8ef;font:700 14px 'Segoe UI',Arial,sans-serif}
.node .node-type{font-size:10px;letter-spacing:1px;fill:#b7dbc6}
.node .node-status{font-size:11px;fill:#b7dbc6}
.node .node-label{color:#e4f8ef;font:700 14px/16px 'Segoe UI',Arial,sans-serif;
  overflow:hidden;overflow-wrap:anywhere;display:-webkit-box;-webkit-box-orient:vertical;
  -webkit-line-clamp:2}
.node[data-status=COMPLETED] rect{fill:#35ae73;stroke:#4be39a}
.node[data-status=COMPLETED] text{fill:#082417}
.node[data-status=COMPLETED] .node-label{color:#082417}
.node[data-status=COMPLETED] circle{fill:#082417}
.node[data-status=RUNNING] rect{fill:#302b1b;stroke:#f4c765}
.node[data-status=RUNNING] circle{fill:#f4c765}
.node[data-status=FAILED] rect,.node[data-status=DEAD] rect{fill:#302025;stroke:#ff777f}
.node[data-status=FAILED] circle,.node[data-status=DEAD] circle{fill:#ff777f}
.node[data-status=WAITING] rect,.node[data-status=PENDING] rect{fill:#1b2c38;stroke:#83c6f4}
.node[data-status=WAITING] circle,.node[data-status=PENDING] circle{fill:#83c6f4}
.node[data-status=CANCELLED] rect{fill:#283137;stroke:#8e9da8}
.node[data-status=UNKNOWN] rect{fill:#20292e;stroke:#72828c}
.node[data-service-state=READY] rect{fill:#176348;stroke:#55e1a0}
.node[data-service-state=READY] circle{fill:#74efb4}
.node[data-service-state=READY] .node-status{fill:#d9ffec}
.node[data-service-state=STARTING] rect,.node[data-service-state=STOPPING] rect{fill:#40351d;stroke:#f4c765}
.node[data-service-state=STARTING] .node-status,.node[data-service-state=STOPPING] .node-status{fill:#ffe5a6}
.node[data-service-state=STOPPED] rect{fill:#243035;stroke:#82949d}
.node[data-service-state=STOPPED] circle{fill:#82949d}
.node[data-service-state=STOPPED] .node-status{fill:#c4d0d5}
.node .service-type-chip{fill:#285846;stroke:#75d9a4}
.node .service-type-chip + .node-type{fill:#d9ffea}
"""


def render_graph(dag, selected_job=None, run_ts=None, standalone=False):
    jobs = {job["id"]: job for job in dag["jobs"]}
    depths = job_depths(jobs)
    columns = defaultdict(list)
    for jid in jobs:
        columns[depths[jid]].append(jid)
    for items in columns.values():
        items.sort()
    positions = {jid: (35 + layer * 285, 35 + row * 130)
                 for layer, items in columns.items() for row, jid in enumerate(items)}
    width = max(900, 70 + (max(columns, default=0) + 1) * 285)
    height = max(350, 75 + max((len(items) for items in columns.values()), default=1) * 130)
    parts = [f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-label="Dependency graph" '
             'xmlns="http://www.w3.org/2000/svg">',
             '<defs><marker id="arrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5" '
             'orient="auto"><path d="M0 0 L7 3.5 L0 7" fill="#64748b"/></marker></defs>']
    if standalone:
        parts.append(f'<style type="text/css">{EXPORT_STYLE}</style>')
        parts.append(f'<rect width="{width}" height="{height}" fill="#0f1a1b"/>')
    for job in jobs.values():
        x, y = positions[job["id"]]
        for parent in job["parents"]:
            if parent in positions:
                px, py = positions[parent]
                parts.append(f'<path class="edge" d="M{px + 190} {py + 47} C{px + 238} {py + 47},'
                             f'{x - 48} {y + 47},{x} {y + 47}" marker-end="url(#arrow)"/>')
    for jid, job in jobs.items():
        x, y = positions[jid]
        label = escape(jid[4:] if jid.startswith("DAG-") else jid)
        href = f'/dags/{quote(dag["id"], safe="")}?job={quote(jid, safe="")}'
        if run_ts is not None:
            href += f'&run={run_ts}'
        status = escape(job["status"])
        display_status = escape(job.get("service_lifecycle") or job["status"])
        job_type = job.get("spec", {}).get("job_type", "run")
        type_label = "SERVICE" if str(job_type).lower() in ("service", "deploy_payload") else (
            "HUMAN GATE" if job.get("spec", {}).get("hitl_message") else "TASK")
        type_chip = '<rect class="service-type-chip" x="10" y="6" width="66" height="20" rx="4"/>' if type_label == "SERVICE" else ""
        chosen = ' selected' if jid == selected_job else ''
        parts.append(f'<a href="{escape(href, quote=True)}"><g class="node{chosen}" data-job-id="{escape(jid, quote=True)}" '
                     f'data-status="{status}" data-service-state="{display_status if type_label == "SERVICE" else ""}" '
                     f'transform="translate({x} {y})">'
                     f'<title>{escape(jid)}</title>'
                     '<rect width="190" height="94" rx="8"/><circle cx="164" cy="18" r="5"/>'
                     f'{type_chip}'
                     f'<text class="node-type" x="14" y="21">{type_label}</text>'
                     '<foreignObject x="14" y="35" width="162" height="34">'
                     f'<div xmlns="http://www.w3.org/1999/xhtml" class="node-label">{label}</div>'
                     '</foreignObject>'
                     f'<text class="node-status" x="14" y="82">{display_status}</text></g></a>')
    return "".join(parts) + "</svg>"
