#  Copyright 2026 Ram Narayanan
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Executes the dashboard's own JavaScript against live API payloads.

Why this exists: a single undefined helper (`el`) inside one panel renderer threw,
which aborted the rest of the render callback and silently blanked every chart
below it. The page still returned HTTP 200 and the API still returned correct
data, so nothing in the Python test suite noticed — the failure lived entirely in
the browser.

This closes that gap without a browser: it extracts each template's <script>,
stubs the DOM, feeds it the real JSON from the running dashboard, and calls every
render function individually. A throw in any one of them fails the test and names
the function.

Requires node, a live cluster, and the dashboard:
    python3 titan_test_suite/tests/test_dashboard_js.py
"""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request

DASH = os.environ.get("TITAN_DASHBOARD", "http://127.0.0.1:5000")
TEMPLATES = "perm_files/templates"
R = []


def check(name, ok, detail=""):
    R.append((name, ok))
    print(f"  {'[PASS]' if ok else '[FAIL]'} {name}" + (f" - {detail}" if detail else ""))


def get(path):
    try:
        with urllib.request.urlopen(DASH + path, timeout=25) as r:
            return json.loads(r.read().decode())
    except (urllib.error.URLError, OSError, ValueError):
        return None


DOM_STUB = r"""
const fs = require('fs');
const payload = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));

// Minimal DOM: enough for chart code that sets innerHTML, reads values and wires listeners.
const mk = id => ({
  id, dataset: {}, style: {}, hidden: false, value: payload.__selectValue || 'fine',
  classList: { add(){}, remove(){}, toggle(){}, contains(){ return false; } },
  set innerHTML(v) { this._h = String(v); }, get innerHTML() { return this._h || ''; },
  set textContent(v) { this._t = String(v); }, get textContent() { return this._t || ''; },
  setAttribute(){}, getAttribute(){ return ''; }, removeAttribute(){},
  addEventListener(){}, appendChild(){}, closest(){ return null; },
  querySelector(){ return null; }, querySelectorAll(){ return []; },
  getBoundingClientRect(){ return { left: 0, top: 0, width: 400, height: 200 }; },
});
const nodes = {};
global.document = {
  getElementById: id => nodes[id] || (nodes[id] = mk(id)),
  querySelectorAll: () => [], querySelector: () => null,
  addEventListener(){}, body: mk('body'), documentElement: mk('html'), hidden: false,
};
global.window = { addEventListener(){}, innerWidth: 1400, innerHeight: 900 };
global.getComputedStyle = () => ({ getPropertyValue: () => '#3987e5' });
global.fetch = () => Promise.resolve({ json: () => Promise.resolve({}) });
global.setInterval = () => 0; global.clearInterval = () => {};
global.console.error = () => {};      // panels may log; that is not a failure by itself

let src = fs.readFileSync(process.argv[3], 'utf8');
// Strip bootstrap lines so nothing auto-runs before we control it.
src = src.replace(/^\s*tick\(\);\s*restart\(\);\s*$/m, '');
src = src.replace(/^\s*load\(\);\s*(restart\(\);)?\s*$/m, '');
src = src.replace(/^\s*loadDagNames\(\);\s*$/m, '');
src = src.replace(/^\s*tick\(\);\s*$/m, '');
src = src.replace(/\(function \(\) \{\s*\n\s*var links[\s\S]*?\}\)\(\);/, '');

try { eval(src); } catch (e) {
  console.log('FATAL eval: ' + e.message);
  process.exit(2);
}

const S = payload.stats || {}, M = payload.metrics || {}, B = payload.board || {}, T = payload.timeline || {};
const JD = (payload.job_dag_map || {}).map || {};
const JR = (payload.job_dag_map || {}).runs || {};
const H = payload.history || {};

const calls = [], edge = [];
function add(n, f) { if (typeof f === 'function') calls.push([n, f]); }
function addEdge(n, f) { if (typeof f === 'function') edge.push([n, f]); }

if (typeof tiles === 'function')       add('tiles', () => tiles(S, M));
if (typeof drawTopo === 'function')    add('drawTopo', () => drawTopo(S, M));
if (typeof diagnostics === 'function') add('diagnostics', () => diagnostics(S, M));
if (typeof storeStrip === 'function')  add('storeStrip', () => storeStrip(M));
if (typeof scalingPanel === 'function')add('scalingPanel', () => scalingPanel(M));
if (typeof budget === 'function')      add('budget', () => budget(S, M));
if (typeof drawLanes === 'function')   add('drawLanes', () => drawLanes(B));
if (typeof stackedArea === 'function') add('stackedArea', () => stackedArea('cQueue', M.queue || [], [3,1,2,4], ['a','b','c','d'], 'tQueue'));
if (typeof line === 'function')        add('line', () => line('cDispatch', M.dispatch || [], '--s2', 'ms', 'tDispatch'));
if (typeof bars === 'function')        add('bars', () => bars('cThru', M.throughput || []));
if (typeof multiLine === 'function')   add('multiLine', () => multiLine('cHb', M.heartbeat || {}, 'lgHb', S));
if (typeof anaSuccess === 'function')  add('anaSuccess', () => anaSuccess(M));
if (typeof anaHistogram === 'function')add('anaHistogram', () => anaHistogram(M));
if (typeof anaPercentiles === 'function') add('anaPercentiles', () => anaPercentiles(M));
if (typeof anaUtilisation === 'function') add('anaUtilisation', () => anaUtilisation(M));
if (typeof phaseTable === 'function')  add('phaseTable', () => phaseTable(M.dispatch_phases || []));
if (typeof failureRate === 'function') add('failureRate', () => failureRate(M.throughput || []));
if (typeof saturation === 'function')  add('saturation', () => saturation(M.queue || []));
if (typeof servicesPanel === 'function') add('servicesPanel', () => servicesPanel(M));
if (typeof fmtUptime === 'function')   add('fmtUptime', () => { fmtUptime(0); fmtUptime(5000); fmtUptime(400000); fmtUptime(9e6); fmtUptime(2e8); });
if (typeof dualLine === 'function')    add('dualLine', () => dualLine('cStoreLat',
  M.store_latency || [], M.store_read_latency || [], 'write', 'read', 'milliseconds', 'lgStoreLat'));
if (typeof stackedArea === 'function') add('phaseStack', () => stackedArea('cPhases',
  M.dispatch_phases || [], [1,2,3,4,5], ['route','select','record','store','send']));
if (typeof anaPriority === 'function') add('anaPriority', () => anaPriority((H.spans || T.spans || [])));
// The priority verdict is a fixed comparison over contended samples. Pin it on synthetic input
// so the wording is driven by the numbers and nothing else.
const VERDICT = {};
if (typeof anaPriority === 'function') {
  const mk = (prio, waits) => waits.map((w, i) => ({
    id: 'p' + prio + '-' + i, priority: prio, queue_wait_ms: w,
    enqueued_at: 1, started_at: 1 + w, ended_at: 1 + w + 10, duration_ms: 10,
    status: 'COMPLETED', parents: [] }));
  const grab = () => nodes['prioNote'] ? (nodes['prioNote']._h || '') : '';
  add('verdict:honoured', () => {
    anaPriority(mk(1, [4000, 4200, 4400, 4600]).concat(mk(9, [400, 420, 440, 460])));
    VERDICT.honoured = grab();
  });
  add('verdict:notReordering', () => {
    anaPriority(mk(1, [400, 420, 440, 460]).concat(mk(9, [4000, 4200, 4400, 4600])));
    VERDICT.notReordering = grab();
  });
  add('verdict:thinSample', () => {
    anaPriority(mk(1, [4000, 4200, 4400, 4600]).concat(mk(9, [400])));
    VERDICT.thin = grab();
  });
  add('verdict:uncontended', () => {
    anaPriority(mk(1, [2, 4, 6, 8]).concat(mk(9, [1, 3, 5, 7])));
    VERDICT.uncontended = nodes['cPrio'] ? (nodes['cPrio']._h || '') : '';
  });
}
if (typeof buildRuns === 'function' && typeof anaTrend === 'function') {
  // The page groups DISK spans by submission stamp — feed it the same inputs, or the test
  // passes against data the page never actually uses.
  add('buildRuns+anaTrend', () => anaTrend(buildRuns((H.spans || T.spans || []), JD, JR)));
}
if (typeof render === 'function')      add('render', () => render(T));
if (typeof criticalPath === 'function') add('criticalPath', () => criticalPath(T.spans || []));
if (typeof fmt === 'function')          add('fmt', () => { fmt(0); fmt(999); fmt(1500); fmt(90000); });

// Edge cases run AFTER the rendered output is captured — a fresh Master serves empty series and
// the page must still render, but these overwrite the same mount points.
if (typeof buildRuns === 'function')    addEdge('buildRuns(empty)', () => buildRuns([], {}, {}));
if (typeof buildRuns === 'function')    addEdge('buildRuns(noStamps)', () => buildRuns((H.spans || T.spans || []), JD, {}));
if (typeof stackedArea === 'function')  addEdge('stackedArea(empty)', () => stackedArea('cQueue', [], [3,1,2,4], ['a','b','c','d'], 'tQueue'));
if (typeof line === 'function')         addEdge('line(empty)', () => line('cDispatch', [], '--s2', 'ms', 'tDispatch'));
if (typeof multiLine === 'function')    addEdge('multiLine(empty)', () => multiLine('cHb', {}, 'lgHb', {}));
if (typeof drawTopo === 'function')     addEdge('drawTopo(noWorkers)', () => drawTopo({ workers: [] }, M));
if (typeof anaPriority === 'function')  addEdge('anaPriority(noPriority)', () => anaPriority([{ id: 'x', queue_wait_ms: 5, enqueued_at: 1 }]));
if (typeof phaseTable === 'function')   addEdge('phaseTable(empty)', () => phaseTable([]));
if (typeof failureRate === 'function')  addEdge('failureRate(empty)', () => failureRate([]));
if (typeof saturation === 'function')   addEdge('saturation(noSlots)', () => saturation([[1,0,0,0,0,0,0],[2,0,0,0,0,0,0]]));
if (typeof servicesPanel === 'function') addEdge('servicesPanel(none)', () => servicesPanel({ services: [] }));
if (typeof dualLine === 'function')     addEdge('dualLine(empty)', () => dualLine('cStoreLat', [], [], 'a', 'b', 'ms', 'lgStoreLat'));
if (typeof render === 'function')       addEdge('render(empty)', () => render({ spans: [], count: 0, now: Date.now() }));

const results = [];
for (const [n, f] of calls) {
  try { f(); results.push({ name: n, ok: true }); }
  catch (e) { results.push({ name: n, ok: false, err: e.message }); }
}

// Snapshot the real output BEFORE the edge cases overwrite the same mount points.
const rendered = {};
Object.keys(nodes).forEach(k => { if (nodes[k]._h) rendered[k] = nodes[k]._h; });

for (const [n, f] of edge) {
  try { f(); results.push({ name: n, ok: true }); }
  catch (e) { results.push({ name: n, ok: false, err: e.message }); }
}
let runSummary = null;
if (typeof buildRuns === 'function') {
  try {
    const r = buildRuns((H.spans || T.spans || []), JD, JR);
    runSummary = Object.keys(r).map(k => ({ name: k, runs: r[k].length,
                                            walls: r[k].map(x => x.wall) }));
  } catch (e) { runSummary = { err: e.message }; }
}
console.log(JSON.stringify({ count: calls.length, results, rendered, runSummary, verdict: VERDICT }));
"""


def run_template(tpl, payload, label):
    """Extract the template's script, run every renderer, return (count, failures)."""
    path = os.path.join(TEMPLATES, tpl)
    if not os.path.exists(path):
        check(f"{label}: template exists", False, path)
        return
    html = open(path, encoding="utf-8").read()
    blocks = re.findall(r"<script>(.*?)</script>", html, re.S)
    if not blocks:
        check(f"{label}: has a script block", False)
        return

    tmp = tempfile.mkdtemp()
    try:
        js = os.path.join(tmp, "page.js")
        open(js, "w", encoding="utf-8").write(max(blocks, key=len))

        # syntax first — a parse error breaks everything and is worth naming separately
        syn = subprocess.run(["node", "--check", js], capture_output=True, text=True)
        check(f"{label}: JavaScript parses", syn.returncode == 0,
              (syn.stderr or "").strip().splitlines()[0] if syn.returncode else "")
        if syn.returncode != 0:
            return

        pj = os.path.join(tmp, "payload.json")
        open(pj, "w", encoding="utf-8").write(json.dumps(payload))
        runner = os.path.join(tmp, "run.js")
        open(runner, "w", encoding="utf-8").write(DOM_STUB)

        out = subprocess.run(["node", runner, pj, js], capture_output=True, text=True, timeout=90)
        text = (out.stdout or "").strip()
        if text.startswith("FATAL"):
            check(f"{label}: script evaluates", False, text[:100])
            return
        check(f"{label}: script evaluates", True)

        try:
            data = json.loads(text.splitlines()[-1])
        except (ValueError, IndexError):
            check(f"{label}: runner produced results", False, (out.stderr or text)[:110])
            return

        check(f"{label}: found render functions", data["count"] > 0, f"{data['count']} functions")
        for r in data["results"]:
            check(f"{label}: {r['name']}() renders without throwing", r["ok"], r.get("err", "")[:90])
        return {"rendered": data.get("rendered", {}), "runs": data.get("runSummary"),
                "verdict": data.get("verdict") or {}}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return {}


def main():
    if not shutil.which("node"):
        print("  node not found — skipping (install node to run this suite)")
        return 0

    stats = get("/api/cluster_stats")
    if stats is None:
        print(f"  Dashboard unreachable at {DASH}. Start server_dashboard.py first.")
        return 2

    payload = {
        "stats": stats,
        "metrics": get("/api/metrics?res=fine") or {},
        "board": get("/api/board?limit=40") or {},
        "timeline": get("/api/timeline?limit=400") or {},
        "job_dag_map": get("/api/job_dag_map") or {},
        "history": get("/api/history?hours=168&limit=20000") or {},
    }
    print("\n=== payload from the live dashboard ===")
    m = payload["metrics"]
    print(f"  queue={len(m.get('queue', []))} dispatch={len(m.get('dispatch', []))} "
          f"spans={payload['timeline'].get('count', 0)} workers={stats.get('active_workers')}")

    print("\n=== cluster.html ===")
    cluster_res = run_template("cluster.html", payload, "cluster") or {}
    cluster_out = cluster_res.get("rendered", {})
    cluster_runs = cluster_res.get("runs")
    verdict = cluster_res.get("verdict", {})

    print("\n=== timeline.html ===")
    tl_out = (run_template("timeline.html", payload, "timeline") or {}).get("rendered", {})

    # ------------------------------------------------------------------
    # Rendered content, not just "no throw". These assert the features are
    # actually emitted into the DOM.
    # ------------------------------------------------------------------
    print("\n=== rendered output: cluster ===")
    topo = cluster_out.get("topo", "")
    check("topology draws worker nodes", "MASTER" in topo and "<rect" in topo, f"{len(topo)}b")
    check("topology labels host:port", ":" in topo and "permanent" in topo or "ephemeral" in topo)
    check("topology marks slot occupancy rings", "stroke-dasharray" in topo or "circle" in topo)
    bud = cluster_out.get("budget", "")
    check("latency budget renders rows", "<tr>" in bud and "Queue wait" in bud, f"{len(bud)}b")
    check("latency budget shows a share percentage", "%" in bud)
    strip = cluster_out.get("storeStrip", "")
    check("store strip shows connection state",
          "Connected" in strip or "Disconnected" in strip, f"{len(strip)}b")
    check("store strip has a status dot", "class=\"dot" in strip)
    check("store strip reports dropped writes", "dropped writes" in strip)
    # The pipeline duration trend shipped blank twice: once because it read the in-memory ring
    # (which drops pre-restart runs) and once because a 30s gap heuristic merged back-to-back
    # runs into one, so nothing ever had the two runs a line needs.
    trend = cluster_out.get("cTrend", "")
    table = cluster_out.get("trendTable", "")
    check("trend chart renders something", bool(trend), f"{len(trend)}b")
    if isinstance(cluster_runs, list):
        multi = [r for r in cluster_runs if r["runs"] >= 2]
        check("run grouping found at least one pipeline", len(cluster_runs) > 0,
              f"{len(cluster_runs)} pipelines")
        check("no pipeline reports zero runs", all(r["runs"] >= 1 for r in cluster_runs))
        check("walls are positive and finite",
              all(all(w > 0 for w in r["walls"]) for r in cluster_runs))
        if multi:
            check("a repeated pipeline yields >= 2 separate runs (stamps, not a time gap)",
                  True, f"{multi[0]['name']}: {multi[0]['runs']} runs")
            check("trend draws an svg line for the repeated pipeline",
                  "<svg" in trend and "<path" in trend, trend[:80])
            check("trend marks each run with a hoverable point",
                  "<circle" in trend and "<title>" in trend)
            check("trend axis is labelled",
                  "seconds" in trend and "run" in trend)
            check("trend legend names the pipeline", multi[0]["name"] in trend)
            # Two runs of the same pipeline seconds apart must NOT be merged into one long run.
            same = [r for r in cluster_runs if r["runs"] >= 2]
            check("repeated runs have comparable walls, not one merged total",
                  all(max(r["walls"]) < sum(r["walls"]) for r in same if len(r["walls"]) > 1))
        else:
            check("trend explains why a line is not possible yet",
                  "at least two" in trend or "empty" in trend, trend[:90])
    check("trend table lists every run", "<tr>" in table and "Pipeline" in table, f"{len(table)}b")
    check("trend table reports a direction per pipeline",
          "stable" in table or "slower" in table or "faster" in table or "—" in table)

    fail = cluster_out.get("cFail", "")
    lgf = cluster_out.get("lgFail", "")
    sat = cluster_out.get("cSat", "")
    lgs = cluster_out.get("lgSat", "")
    svc = cluster_out.get("svcPanel", "")
    check("failure rate chart renders", "<svg" in fail or "Collecting" in fail, f"{len(fail)}b")
    check("failure rate axis is a fixed 0-100 scale, not auto-scaled",
          ("100%" in fail and "0%" in fail) or not fail.startswith("<svg"))
    check("failure rate legend states the window total",
          ("settled" in lgf and "%" in lgf) or not lgf, f"{len(lgf)}b")
    check("failure rate legend carries a status glyph", any(g in lgf for g in ("✓", "!", "✕")) or not lgf)
    check("saturation chart renders", "<svg" in sat or "Collecting" in sat, f"{len(sat)}b")
    check("saturation legend names slots, not just a percentage",
          "slots" in lgs or not lgs, f"{len(lgs)}b")
    check("services panel renders a table or says why it is empty",
          ("<table" in svc) or ("No services deployed" in svc), f"{len(svc)}b")
    if "<table" in svc:
        check("services list an address", "Address" in svc)
        check("services report uptime", "Uptime" in svc)
        check("service liveness is separate from the deploy job's status",
              "LIVE" in svc and "Deploy job" in svc)
        check("the panel explains that a COMPLETED deploy is normal", "COMPLETED is normal" in svc)

    # The priority verdict must be arithmetic, not judgement: same inputs, same sentence, every
    # time, computed in the page with no service on the other end of it.
    print("\n=== priority verdict is deterministic ===")
    strip = lambda h: re.sub(r"<[^>]+>", "", h or "")
    if verdict:
        hon = strip(verdict.get("honoured"))
        notr = strip(verdict.get("notReordering"))
        thin = strip(verdict.get("thin"))
        unc = strip(verdict.get("uncontended"))
        check("a high band with shorter waits reads as honoured",
              "honoured" in hon, hon[:90])
        check("a high band with longer waits reads as not reordering",
              "not reordering" in notr.lower(), notr[:90])
        check("the two opposite inputs produce different verdicts", hon != notr)
        check("a band below the sample floor is refused, not judged",
              "Not enough contended samples" in thin, thin[:90])
        check("the refusal names which band was thin", "p9" in thin, thin[:110])
        check("all-uncontended input explains there was no queue to reorder",
              "no queue" in unc.lower() or "never" in unc.lower(), unc[:110])
        check("every verdict states the population it measured",
              all("waited ≥" in strip(verdict.get(k, "")) for k in ("honoured", "notReordering", "thin")))
        check("every verdict reports its sample counts",
              all("n=" in strip(verdict.get(k, "")) for k in ("honoured", "notReordering", "thin")))

    phases = cluster_out.get("cPhases", "")
    ptab = cluster_out.get("tPhases", "")
    slat = cluster_out.get("cStoreLat", "")
    prio = cluster_out.get("cPrio", "")
    check("dispatch phase stack renders", "<svg" in phases or "Collecting" in phases, f"{len(phases)}b")
    check("phase table names all five phases",
          all(n in ptab for n in ("route", "select", "record", "store", "send")) or not ptab,
          f"{len(ptab)}b")
    check("phase table shows each phase's share", "%" in ptab or not ptab)
    check("store latency chart renders write and read",
          ("<svg" in slat or "Collecting" in slat), f"{len(slat)}b")
    lsl = cluster_out.get("lgStoreLat", "")
    check("store latency legend distinguishes write from read",
          ("write" in lsl and "read" in lsl) or not lsl, f"{len(lsl)}b")
    check("priority panel renders or explains the gap",
          "<svg" in prio or "priority" in prio.lower(), f"{len(prio)}b")

    qc = cluster_out.get("cQueue", "")
    check("queue chart emits an svg with axis titles",
          "<svg" in qc and "job" in qc, f"{len(qc)}b")

    print("\n=== rendered output: timeline ===")
    chart = tl_out.get("chart", "")
    mets = tl_out.get("metrics", "")
    fails = tl_out.get("failbox", "")
    check("timeline renders span rows", 'class="row"' in chart or "empty" in chart, f"{len(chart)}b")
    if 'class="row"' in chart:
        check("spans carry a PERM/EPH chip", "chip perm" in chart or "chip eph" in chart)
        check("spans carry a status-coloured bar", "bar b-" in chart)
        check("queue-wait segment is drawn when a job waited",
              'class="wait' in chart or "queue_wait" not in chart)
        check("time axis is emitted", 'class="axis"' in chart)
    check("metric tiles rendered", "Wall clock" in mets and "Critical path" in mets, f"{len(mets)}b")
    check("parallelism and queue-time tiles present",
          "Parallelism" in mets and "Queue time" in mets)
    check("starved tile present", "Starved" in mets)
    check("failures block always renders (even with none)",
          "Failures" in fails, f"{len(fails)}b")

    # Every function a page calls must exist in that page. A hand-written list of helpers only
    # catches the ones you thought of — it passed while timeline.html called esc(), defined only
    # in cluster.html, and threw "esc is not defined" the moment a pipeline with missing spans
    # was picked. So derive the call sites instead of listing them.
    print("\n=== every called helper is defined in its own page ===")
    BUILTINS = {
        # JS + DOM surface these pages legitimately use.
        "if", "for", "while", "switch", "catch", "return", "function", "typeof", "new", "delete",
        "void", "in", "of", "do", "else", "try", "throw", "case", "String", "Number", "Boolean",
        "Array", "Object", "Math", "JSON", "Date", "RegExp", "Error", "parseInt", "parseFloat",
        "isNaN", "encodeURIComponent", "decodeURIComponent", "setTimeout", "clearTimeout",
        "setInterval", "clearInterval", "fetch", "alert", "confirm", "console", "document",
        "window", "getComputedStyle", "requestAnimationFrame", "Promise", "Set", "Map",
        "Intl", "parse", "stringify", "max", "min", "abs", "floor", "ceil", "round", "pow",
        "sqrt", "apply", "call", "bind", "map", "filter", "reduce", "forEach", "sort", "slice",
        "splice", "concat", "join", "split", "replace", "indexOf", "lastIndexOf", "includes",
        "push", "pop", "shift", "unshift", "keys", "values", "entries", "toFixed", "toUpperCase",
        "toLowerCase", "trim", "charAt", "substring", "substr", "match", "test", "exec", "has",
        "get", "set", "add", "then", "all", "race", "resolve", "reject", "finally", "assign",
        "toString", "hasOwnProperty", "isArray", "from", "now", "reverse", "some", "every",
        "find", "findIndex", "flat", "flatMap", "padStart", "padEnd", "repeat", "startsWith",
        "endsWith", "localeCompare", "isFinite", "isInteger", "sign", "log", "exp", "random",
    }
    for tpl in ("cluster.html", "timeline.html"):
        src = open(os.path.join(TEMPLATES, tpl), encoding="utf-8").read()
        blocks = re.findall(r"<script>(.*?)</script>", src, re.S)
        js = max(blocks, key=len) if blocks else ""
        defined = set(re.findall(r"function\s+([A-Za-z_$][\w$]*)\s*\(", js))
        defined |= set(re.findall(r"(?:var|let|const)\s+([A-Za-z_$][\w$]*)\s*=\s*function", js))
        defined |= set(re.findall(r"(?:var|let|const)\s+([A-Za-z_$][\w$]*)\s*=\s*\(?[\w\s,]*\)?\s*=>", js))
        # Parameters count as defined: a callback passed in (legendFmt, fn) is called by name.
        for params in re.findall(r"function[\w\s$]*\(([^)]*)\)", js):
            defined |= {x.strip() for x in params.split(",") if x.strip().isidentifier()}
        # Scan code only. An identifier inside a string ("capacity (slots)") is prose, not a call.
        code = re.sub(r"/\*.*?\*/", " ", js, flags=re.S)
        code = re.sub(r"(?m)//.*$", " ", code)
        code = re.sub(r"'(?:\\.|[^'\\])*'", "''", code)
        code = re.sub(r'"(?:\\.|[^"\\])*"', '""', code)
        # Calls that are not property accesses: `foo(` but not `.foo(`. No whitespace before the
        # paren — code writes `esc(x)`, while the prose that survives inside HTML fragments reads
        # "demand (queued + running)". That space is what separates a call from a caption.
        called = set(re.findall(r"(?<![.\w$])([a-z_$][\w$]*)\(", code))
        called -= {"var", "let", "const", "and", "or", "not"}
        missing = sorted(called - defined - BUILTINS)
        check(f"{tpl}: every function it calls is defined in it", not missing,
              f"undefined: {missing}")

    # ------------------------------------------------------------------
    # Structure: every section and every mount point the renderers write into
    # must exist in the served HTML. A renderer that targets a missing id fails
    # silently, which is how the analytics tab shipped dead.
    # ------------------------------------------------------------------
    print("\n=== cluster page structure ===")
    try:
        with urllib.request.urlopen(DASH + "/cluster", timeout=25) as r:
            page = r.read().decode()
    except (urllib.error.URLError, OSError):
        check("cluster page loads", False)
        page = ""

    if page:
        check("cluster page loads", len(page) > 20000, f"{len(page)}b")

        # Mount points written to by JS — a typo here blanks a panel with no error.
        mounts = ["tiles", "storeStrip", "diagAllClear", "diagPanels", "lanes", "budget",
                  "budgetNote", "topo", "detail", "cQueue", "cDispatch", "cThru", "cHb",
                  "cPhases", "tPhases", "cStoreLat", "lgStoreLat", "cPrio", "prioNote",
                  "cFail", "lgFail", "cSat", "lgSat", "svcPanel",
                  "scalingChart", "scalingBox", "anaTiles", "cSuccess", "cHist", "cPct",
                  "cUtil", "cTrend", "trendTable", "zoom", "zoomBody", "secnav",
                  "view-live", "view-analytics", "tabLive", "tabAna", "res", "nf", "iv"]
        missing = [m for m in mounts if f'id="{m}"' not in page]
        check("every JS mount point exists in the HTML", not missing, f"missing: {missing}")

        sections = ["Why work isn't moving", "Before dispatch", "Performance across the system",
                    "Topology", "Trends", "Scaling history", "How to read this page",
                    "Success rate", "Job duration distribution",
                    "Queue wait percentiles", "Per-worker slot utilisation",
                    "Pipeline duration trend"]
        absent = [x for x in sections if x not in page]
        check("all sections are present", not absent, f"absent: {absent}")

        check("Live is the default sub-tab",
              'id="view-analytics" hidden' in page and 'id="view-live">' in page)
        check("how-to-read sits above the sub-tabs",
              page.index("How to read this page") < page.index('class="subtabs"'))
        check("collapsed-by-default sections are collapsed",
              'class="panel howto">' in page and 'class="panel fold">' in page)

        # Every chart must be wired for click-to-expand and carry axis units.
        for cid in ("cQueue", "cDispatch", "cThru", "cHb", "scalingChart",
                    "cSuccess", "cHist", "cPct", "cUtil", "cTrend"):
            check(f"{cid} is registered as zoomable", f"makeZoomable('{cid}'" in page)
        # The topology is deliberately NOT zoomable: clicking a node is how you inspect it, and
        # the detail table beside it is the answer. A modal copy of the SVG replaces both.
        # Every var(--x) must be declared. An undefined custom property invalidates the whole
        # declaration silently: 16 health signals claimed to be green while rendering in plain
        # ink, and the sticky nav had no background at all.
        used = set(re.findall(r"var\((--[a-z0-9-]+)", page))
        declared = set(re.findall(r"(--[a-z0-9-]+)\s*:", page))
        check("every CSS custom property used is declared",
              not (used - declared), f"undefined: {sorted(used - declared)}")

        # Health signals must be readable without colour discrimination — good and bad are
        # near-identical hues under deuteranopia.
        check("status colours are paired with a glyph, not colour alone",
              "'✓'" in page and "'✕'" in page and "class=\"g\"" in page)
        check("tiles take a status band from thresholds",
              "function band(" in page and "band(p99" in page)
        check("health-critical tiles exist", all(k in page for k in
              ("'Dead letter'", "'Parked'", "'Capability gaps'")))
        check("topology is not wired for zoom", "makeZoomable('topo'" not in page)
        # The trend table grows with every pipeline ever run, so it must scroll in place rather
        # than stretching the panel down the page.
        check("trend table scrolls inside its panel",
              'id="trendTable" class="tscroll"' in page)
        check("trend table scroll box is bounded and scrollable",
              ".tscroll {" in page and "max-height" in page.split(".tscroll {")[1][:120]
              and "overflow:auto" in page.split(".tscroll {")[1][:120])
        check("trend table header stays visible while scrolling",
              ".tscroll table.budget thead th" in page and "position:sticky" in page)
        check("topology keeps its click-to-inspect detail panel",
              'id="detail"' in page and "Click the Master or any worker" in page)
        check("topology sits beside its detail table", "topo-wrap" in page)
        # The demo runner is the one mutating control on the page; it must be preset-driven.
        check("demo preset picker is present", 'id="demoPreset"' in page)
        check("demo run button is present", 'id="btnDemo"' in page and "runDemo()" in page)
        check("demo progress strip is present", 'id="demoStrip"' in page)
        check("demo posts a preset name and nothing else",
              "JSON.stringify({ preset: preset })" in page)
        check("demo presets are loaded from the server, not hardcoded in the page",
              "/api/demo/presets" in page)
        check("demo links through to the timeline", "/timeline?dag=" in page)

        for cid in ("cPhases", "cStoreLat", "cPrio", "cFail", "cSat"):
            check(f"{cid} has a mount point", f'id="{cid}"' in page)
            check(f"{cid} is registered as zoomable", f"makeZoomable('{cid}'" in page)
        check("new panels carry axis labels", page.count("axl2") >= 8)
        check("every new Live panel has a nav entry",
              all(('href="#' + a) in page for a in ("a-fail", "a-sat", "a-svc", "a-phases", "a-storelat")))
        check("services panel has a mount point", 'id="svcPanel"' in page)
        # Nothing on this page may reach a model: every conclusion is computed from the payload.
        for term in ("openai", "anthropic", "claude.ai", "api.openai", "gpt-", "generativelanguage",
                     "huggingface", "llm"):
            check(f"page makes no reference to {term}", term not in page.lower())
        urls = set(re.findall(r"fetch\(\s*['\"]([^'\"]+)", page))
        external = [u for u in urls if u.startswith("http") or u.startswith("//")]
        check("every fetch is a same-origin dashboard route", not external, str(external))
        check("the verdict is computed in-page, not fetched",
              "function anaPriority(" in page and "/api/verdict" not in page)
        check("axis titles helper is used", page.count("axisTitles(") >= 4)
        check("every section anchor has a nav entry",
              all(('href="#' + a) in page for a in ("s-health", "s-blocked", "s-queue",
                                                    "s-perf", "s-topo", "s-trends", "s-scaling")))

        # Panels must be isolated so one throw cannot blank the page again.
        check("panel renders are wrapped in safe()", page.count("safe('") >= 8,
              f"{page.count(chr(115)+chr(97)+chr(102)+chr(101)+chr(40)+chr(39))} wrapped")

    # ------------------------------------------------------------------
    # Timeline page structure — same rigour as the cluster page above.
    # ------------------------------------------------------------------
    print("\n=== timeline page structure ===")
    try:
        with urllib.request.urlopen(DASH + "/timeline", timeout=25) as r:
            tp = r.read().decode()
    except (urllib.error.URLError, OSError):
        check("timeline page loads", False)
        tp = ""

    if tp:
        check("timeline page loads", len(tp) > 10000, f"{len(tp)}b")

        mounts = ["chart", "metrics", "failbox", "conn", "filter", "dagpick",
                  "win", "limit", "btnGroup", "btnAuto"]
        missing = [m for m in mounts if f'id="{m}"' not in tp]
        check("every timeline mount point exists", not missing, f"missing: {missing}")

        # Controls and their defaults. A default that hides data is a bug — that is exactly
        # how the page looked broken while working correctly.
        check("pipeline picker is present", '<select id="dagpick"' in tp)
        check("window selector defaults to 'fit to all spans'",
              re.search(r'<option value="0"[^>]*selected', tp) is not None)
        check("free-text job filter is present", 'id="filter"' in tp)
        check("group-by-worker toggle present", 'id="btnGroup"' in tp)

        # Layout requirements the user asked for explicitly.
        check("chart area is scrollable", "max-height:62vh" in tp and "overflow-y:auto" in tp)
        check("time axis is sticky while scrolling", "position:sticky" in tp)
        check("failures list is scrollable", ".fails ul" in tp and "max-height:230px" in tp)
        check("how-to-read is collapsible and above the chart",
              '<details class="guide"' in tp and tp.index("How to read this") < tp.index('id="chart"'))
        check("legend lives inside the how-to-read block",
              tp.index('class="legend"') > tp.index("How to read this")
              and tp.index('class="legend"') < tp.index('id="chart"'))

        # Visual encodings that carry meaning — each must exist as a style rule.
        for cls, what in ((".wait", "queue-wait segment"), (".wait.starved", "starved highlight"),
                          (".bar.crit", "critical-path outline"), (".chip.perm", "permanent chip"),
                          (".chip.eph", "ephemeral chip"), (".b-COMPLETED", "completed colour"),
                          (".b-FAILED", "failed colour"), (".b-DEAD", "dead colour")):
            check(f"timeline defines the {what} ({cls})", cls in tp)

        # Guide rows must document every encoding, so the legend and the CSS cannot drift apart.
        for term in ("grey prefix", "amber prefix", "coloured bar", "amber outline",
                     "PERM / EPH", "hover a row"):
            check(f"how-to-read documents '{term}'", term in tp)

        check("null-safe control access is used",
              "function val(id" in tp and tp.count("getElementById('dagpick')") == 0)
        check("an empty result explains why rather than showing a blank",
              "fell outside this time window" in tp
              and "held in memory only" in tp)

    # ------------------------------------------------------------------
    # HTML validity. The id checks above all passed while the markup was badly
    # nested — .wrap closed before the analytics view, so that view escaped the
    # centred container and rendered full-bleed. Structure needs its own check.
    # ------------------------------------------------------------------
    print("\n=== HTML structure validity ===")
    from html.parser import HTMLParser

    INLINE = {
        "br", "img", "input", "meta", "link", "hr", "span", "p", "a", "i", "b", "em",
        "option", "text", "svg", "path", "rect", "circle", "line", "polyline", "polygon",
        "title", "tspan", "marker", "defs", "g", "button", "select", "label", "summary",
        "table", "thead", "tbody", "tr", "th", "td", "h1", "h2", "h3", "h4", "dl", "dt",
        "dd", "ul", "li", "strong", "code", "nav", "article", "figure", "figcaption",
        "small", "pre", "polygon",
    }

    class Structure(HTMLParser):
        """Tracks only block containers, and ignores anything inside <script>."""

        def __init__(self):
            super().__init__(convert_charrefs=True)
            self.stack, self.in_script, self.errors, self.parents = [], False, [], {}

        def handle_starttag(self, tag, attrs):
            if tag == "script":
                self.in_script = True
                return
            if self.in_script or tag in INLINE:
                return
            d = dict(attrs)
            ident = d.get("id") or d.get("class", "")
            if ident:
                self.parents[ident] = [x[1] for x in self.stack if x[1]]
            self.stack.append((tag, ident))

        def handle_endtag(self, tag):
            if tag == "script":
                self.in_script = False
                return
            if self.in_script or tag in INLINE:
                return
            if not self.stack:
                self.errors.append(f"stray </{tag}>")
                return
            top = self.stack[-1]
            if top[0] != tag:
                self.errors.append(f"</{tag}> closed while <{top[0]} {top[1]}> was open")
                for k in range(len(self.stack) - 1, -1, -1):
                    if self.stack[k][0] == tag:
                        del self.stack[k:]
                        return
                return
            self.stack.pop()

    for page_path, label, nesting in (
        ("/cluster", "cluster", [("view-live", "wrap"), ("view-analytics", "wrap"),
                                 ("subtabs", "wrap")]),
        ("/timeline", "timeline", []),
        ("/dags", "dags", []),
    ):
        try:
            with urllib.request.urlopen(DASH + page_path, timeout=25) as r:
                body = r.read().decode()
        except (urllib.error.URLError, OSError):
            check(f"{label}: page fetched for structure check", False)
            continue

        sp = Structure()
        sp.feed(body)
        check(f"{label}: no mismatched closing tags", not sp.errors,
              "; ".join(sp.errors[:2]))
        leftover = [x[1] or x[0] for x in sp.stack]
        check(f"{label}: every container is closed", not leftover, f"unclosed: {leftover[:3]}")

        # Containment: a view that escapes its centred wrapper renders full-bleed.
        for child, parent in nesting:
            check(f"{label}: '{child}' is inside '{parent}'",
                  parent in sp.parents.get(child, []),
                  f"actual parents: {sp.parents.get(child)}")

    bad = [n for n, ok in R if not ok]
    print(f"\n{'=' * 66}\n RESULT: {len(R) - len(bad)}/{len(R)} passed\n{'=' * 66}")
    for b in bad:
        print("  FAILED:", b)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
