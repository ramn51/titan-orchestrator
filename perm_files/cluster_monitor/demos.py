"""Fixed, bounded DAG scenarios for control-plane demonstrations."""

import json
from datetime import datetime, timezone
from pathlib import Path
from secrets import token_hex


PRESETS = {
    "basic": ("Normal basic", "Three sequential stages; baseline scheduling."),
    "fan_out": ("Fan out", "One root, eight parallel branches, one join."),
    "deep_dag": ("Deep DAG", "Fourteen dependent stages; release latency."),
    "saturation_burst": ("Saturation burst", "Sixteen short jobs compete for worker slots."),
    "long_saturation": ("Long saturation", "Twelve longer jobs keep slots occupied."),
    "load": ("Load", "Thirty-six short jobs expose queue throughput."),
}


def recover_latest(manifest_path):
    """Restore the most recent demo banner after a dashboard restart."""
    try:
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    newest = None
    for key, runs in manifest.items():
        if not key.startswith("__runs__CLUSTER_") or not isinstance(runs, list):
            continue
        name = key.removeprefix("__runs__")
        preset = next((item for item in PRESETS if name.startswith(f"CLUSTER_{item.upper()}_")), None)
        if not preset:
            continue
        for run in runs:
            if not isinstance(run, dict) or not isinstance(run.get("jobs"), dict):
                continue
            if newest is None or run.get("run_ts", 0) > newest[0]:
                newest = (run.get("run_ts", 0), {"preset": preset, "label": PRESETS[preset][0],
                           "dag_id": f"DAG-{name}", "job_ids": list(run["jobs"])})
    return newest[1] if newest else None


def build(preset):
    if preset not in PRESETS:
        raise ValueError("Unknown demo preset")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + token_hex(2)
    name = f"CLUSTER_{preset.upper()}_{stamp}"
    jobs = []

    def add(index, parents=(), seconds=1.0):
        jobs.append({"id": f"{name}_{index:02d}", "filename": "cluster_demo_stage.py",
                     "args": f"stage_{index:02d} {seconds:g}",
                     "depends_on": [f"{name}_{parent:02d}" for parent in parents],
                     "requirement": "GENERAL", "priority": 1 + index % 9})

    if preset == "basic":
        for index in range(3):
            add(index, [index - 1] if index else (), 1)
    elif preset == "fan_out":
        add(0, seconds=1)
        for index in range(1, 9):
            add(index, [0], 2)
        add(9, range(1, 9), 1)
    elif preset == "deep_dag":
        for index in range(14):
            add(index, [index - 1] if index else (), 0.8)
    elif preset == "saturation_burst":
        for index in range(16):
            add(index, seconds=2)
    elif preset == "long_saturation":
        for index in range(12):
            add(index, seconds=8)
    elif preset == "load":
        for index in range(36):
            add(index, seconds=0.6)
    return name, jobs
