from __future__ import annotations

import hashlib, json, math, random, statistics
from pathlib import Path

OUTLIER_POLICY = "No measured pass is removed. Warm-up passes are recorded separately and excluded from summaries."

def percentile(values, q):
    xs = sorted(float(x) for x in values)
    if not xs: return None
    k = (len(xs)-1)*q
    lo, hi = math.floor(k), math.ceil(k)
    return xs[lo] if lo == hi else xs[lo]*(hi-k)+xs[hi]*(k-lo)

def bootstrap_median_ci(values, resamples=10000, seed=20261026):
    xs = [float(x) for x in values]
    rng = random.Random(seed)
    medians = [statistics.median(rng.choices(xs, k=len(xs))) for _ in range(resamples)]
    return [percentile(medians, .025), percentile(medians, .975)]

def summarize(values):
    xs = [float(x) for x in values]
    mean = statistics.mean(xs)
    sd = statistics.pstdev(xs)
    return {"n":len(xs), "median":statistics.median(xs), "min":min(xs), "max":max(xs),
            "mean":mean, "standard_deviation":sd,
            "coefficient_of_variation": sd/mean if mean else 0.0,
            "median_bootstrap_95_ci": bootstrap_median_ci(xs)}

def latency(values):
    return {"p50":percentile(values,.50), "p95":percentile(values,.95), "p99":percentile(values,.99)}

def sha256(path):
    h=hashlib.sha256(); h.update(Path(path).read_bytes()); return h.hexdigest()

def write_json(path, value):
    path=Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2)+"\n", encoding="utf-8")

def load_graph(path):
    raw=json.loads(Path(path).read_text(encoding="utf-8")); wf=raw.get("workflow",raw)
    if "specification" in wf:
        tasks=wf["specification"].get("tasks",[])
        execution={t["id"]:t for t in wf.get("execution",{}).get("tasks",[])}
        by={t["id"]:{**t,**execution.get(t["id"],{})} for t in tasks}
        parents={t["id"]:list(t.get("parents",[])) for t in tasks}
    else:
        tasks=wf.get("tasks",[])
        by={(t.get("id") or t["name"]):t for t in tasks}; parents={n:[] for n in by}
        for t in tasks:
            node=t.get("id") or t["name"]
            if "parents" in t: parents[node]=list(t["parents"])
            for child in t.get("children",[]): parents.setdefault(child,[]).append(node)
    return raw, by, parents
