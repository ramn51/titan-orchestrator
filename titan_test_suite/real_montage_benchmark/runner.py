"""Submit the official M101 Montage workflow as real Titan DAG stages."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import shutil
import sys
import tarfile
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
import titan_sdk.titan_sdk as sdk
from titan_sdk.titan_sdk import TitanClient, TitanJob


URL = "https://irsa.ipac.caltech.edu/Montage/docs/m101Example/tutorial-initial.tar.gz"
TERMINAL = {"COMPLETED", "FAILED", "DEAD", "CANCELLED"}


def encode(value: dict) -> str:
    raw = json.dumps(value, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def wait(client: TitanClient, full_ids: list[str], timeout: float) -> dict[str, str]:
    deadline = time.monotonic() + timeout
    statuses: dict[str, str] = {}
    while time.monotonic() < deadline:
        statuses = {item: client.get_job_status("DAG-" + item).strip().upper() for item in full_ids}
        if statuses and all(value in TERMINAL for value in statuses.values()):
            return statuses
        time.sleep(0.5)
    raise TimeoutError(statuses)


def unique_copy(source: Path, name: str, root: Path) -> Path:
    target = root / name
    shutil.copy2(source, target)
    return target


def fetch_chunked(client: TitanClient, remote: str, local: Path, scratch: Path) -> None:
    manifest_path = scratch / f"{remote}.parts.json"
    scratch.mkdir(parents=True, exist_ok=True)
    if not client.fetch_artifact(manifest_path.name, str(manifest_path)):
        raise RuntimeError(f"chunk manifest fetch failed: {remote}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    digest = hashlib.sha256()
    local.parent.mkdir(parents=True, exist_ok=True)
    with local.open("wb") as target:
        for item in manifest["parts"]:
            part = scratch / item["asset"]
            if not client.fetch_artifact(item["asset"], str(part)):
                raise RuntimeError(f"chunk fetch failed: {item['asset']}")
            data = part.read_bytes()
            if hashlib.sha256(data).hexdigest() != item["sha256"]:
                raise RuntimeError(f"chunk checksum mismatch: {item['asset']}")
            target.write(data)
            digest.update(data)
    if local.stat().st_size != manifest["bytes"] or digest.hexdigest() != manifest["sha256"]:
        raise RuntimeError(f"artifact checksum mismatch: {remote}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--worker-master-host", default=None,
                        help="Master address reachable from worker VMs; defaults to --host")
    parser.add_argument("--port", type=int, default=9090)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--stage", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument("--expected-mosaic-sha", required=True)
    parser.add_argument("--expected-area-sha", required=True)
    parser.add_argument("--expected-preview-sha", required=True)
    args = parser.parse_args()
    sdk.TITAN_HOST, sdk.TITAN_PORT = args.host, args.port
    client = TitanClient()
    run_id = "m101_" + uuid.uuid4().hex[:10]
    worker_master = args.worker_master_host or args.host
    workspace = Path(tempfile.mkdtemp(prefix="titan-real-montage-"))
    started = time.time()
    try:
        archive = workspace / "tutorial-initial.tar.gz"
        urllib.request.urlretrieve(URL, archive)
        with tarfile.open(archive, "r:gz") as bundle:
            bundle.extractall(workspace, filter="data")
        source = workspace / "m101"
        upload = workspace / "upload"
        upload.mkdir()

        deployed_stage = unique_copy(args.stage, f"real_montage_stage_{run_id}.py", upload)
        response = client.deploy_script(str(deployed_stage))
        if response != f"SCRIPT_STAGED:{deployed_stage.name}":
            raise RuntimeError(f"stage deployment failed: {response}")

        template_asset = f"{run_id}__template.hdr"
        template_local = unique_copy(source / "template.hdr", template_asset, upload)
        if client.upload_file(str(template_local)) != "UPLOAD_SUCCESS":
            raise RuntimeError("template upload failed")

        raw_files = sorted((source / "rawdir").glob("*.fits"))
        projected: list[dict] = []
        corrected: list[dict] = []
        jobs: list[TitanJob] = []
        project_ids: list[str] = []
        for index, raw in enumerate(raw_files):
            raw_asset = f"{run_id}__raw__{raw.name}"
            local = unique_copy(raw, raw_asset, upload)
            if client.upload_file(str(local)) != "UPLOAD_SUCCESS":
                raise RuntimeError(f"input upload failed: {raw.name}")
            out_name = raw.name
            area_name = raw.stem + "_area.fits"
            out_asset = f"{run_id}__proj__{out_name}"
            area_asset = f"{run_id}__proj__{area_name}"
            job_id = f"realmontage_{run_id}_project_{index:02d}"
            project_ids.append(job_id)
            projected.extend([{"name": out_name, "asset": out_asset}, {"name": area_name, "asset": area_asset}])
            payload = {"mode": "project", "master": worker_master, "port": args.port, "run_id": run_id,
                       "job_id": job_id, "template": template_asset, "input_name": raw.name,
                       "input_asset": raw_asset, "output_name": out_name,
                       "output_asset": out_asset, "area_asset": area_asset}
            jobs.append(TitanJob(job_id, str(deployed_stage), args=encode(payload), requirement="GENERAL"))

        images_asset = f"{run_id}__images.tbl"
        metadata_id = f"realmontage_{run_id}_metadata"
        jobs.append(TitanJob(metadata_id, str(deployed_stage), args=encode({
            "mode": "metadata", "master": worker_master, "port": args.port, "run_id": run_id,
            "job_id": metadata_id, "projected": projected, "table_asset": images_asset,
        }), parents=project_ids, requirement="GENERAL"))

        diffs_asset = f"{run_id}__diffs.tbl"
        overlaps_id = f"realmontage_{run_id}_overlaps"
        jobs.append(TitanJob(overlaps_id, str(deployed_stage), args=encode({
            "mode": "overlaps", "master": worker_master, "port": args.port, "run_id": run_id,
            "job_id": overlaps_id, "images_asset": images_asset, "diffs_asset": diffs_asset,
        }), parents=[metadata_id], requirement="GENERAL"))

        fits_asset = f"{run_id}__fits.tbl"
        diff_id = f"realmontage_{run_id}_difference_fit"
        jobs.append(TitanJob(diff_id, str(deployed_stage), args=encode({
            "mode": "difference_fit", "master": worker_master, "port": args.port, "run_id": run_id,
            "job_id": diff_id, "template": template_asset, "projected": projected,
            "diffs_asset": diffs_asset, "fits_asset": fits_asset,
        }), parents=[overlaps_id], requirement="GENERAL"))

        corrections_asset = f"{run_id}__corrections.tbl"
        model_id = f"realmontage_{run_id}_background_model"
        jobs.append(TitanJob(model_id, str(deployed_stage), args=encode({
            "mode": "background_model", "master": worker_master, "port": args.port, "run_id": run_id,
            "job_id": model_id, "images_asset": images_asset, "fits_asset": fits_asset,
            "corrections_asset": corrections_asset,
        }), parents=[diff_id], requirement="GENERAL"))

        background_ids: list[str] = []
        for index, raw in enumerate(raw_files):
            name = raw.name
            area_name = raw.stem + "_area.fits"
            out_asset = f"{run_id}__corr__{name}"
            area_asset = f"{run_id}__corr__{area_name}"
            corrected.extend([{"name": name, "asset": out_asset}, {"name": area_name, "asset": area_asset}])
            job_id = f"realmontage_{run_id}_background_{index:02d}"
            background_ids.append(job_id)
            jobs.append(TitanJob(job_id, str(deployed_stage), args=encode({
                "mode": "background", "master": worker_master, "port": args.port, "run_id": run_id,
                "job_id": job_id, "input_name": name, "area_name": area_name,
                "input_asset": f"{run_id}__proj__{name}", "area_asset": f"{run_id}__proj__{area_name}",
                "images_asset": images_asset, "corrections_asset": corrections_asset,
                "output_name": name, "output_asset": out_asset, "output_area_asset": area_asset,
            }), parents=[model_id, project_ids[index]], requirement="GENERAL"))

        mosaic_asset = f"{run_id}__m101_mosaic.fits"
        mosaic_area_asset = f"{run_id}__m101_mosaic_area.fits"
        add_id = f"realmontage_{run_id}_add"
        jobs.append(TitanJob(add_id, str(deployed_stage), args=encode({
            "mode": "add", "master": worker_master, "port": args.port, "run_id": run_id,
            "job_id": add_id, "template": template_asset, "corrected": corrected,
            "images_asset": images_asset, "mosaic_asset": mosaic_asset,
            "mosaic_area_asset": mosaic_area_asset,
        }), parents=background_ids, requirement="GENERAL"))

        preview_asset = f"{run_id}__m101_mosaic.png"
        header_asset = f"{run_id}__m101_mosaic.hdr"
        preview_id = f"realmontage_{run_id}_preview"
        jobs.append(TitanJob(preview_id, str(deployed_stage), args=encode({
            "mode": "preview", "master": worker_master, "port": args.port, "run_id": run_id,
            "job_id": preview_id, "mosaic_asset": mosaic_asset,
            "preview_asset": preview_asset, "header_asset": header_asset,
        }), parents=[add_id], requirement="GENERAL"))

        dag = "REAL_MONTAGE_M101_" + run_id
        accepted = client.submit_dag(dag, jobs)
        if accepted != "DAG_ACCEPTED":
            raise RuntimeError(f"DAG rejected: {accepted}")
        statuses = wait(client, [job.id for job in jobs], args.timeout)
        if any(value != "COMPLETED" for value in statuses.values()):
            raise RuntimeError(statuses)

        result_dir = args.output.resolve().parent / run_id
        result_dir.mkdir(parents=True, exist_ok=True)
        mosaic = result_dir / "m101_mosaic.fits"
        area = result_dir / "m101_mosaic_area.fits"
        preview = result_dir / "m101_mosaic.png"
        header = result_dir / "m101_mosaic.hdr"
        for remote, local in [(mosaic_asset, mosaic), (mosaic_area_asset, area)]:
            fetch_chunked(client, remote, local, result_dir / "chunks")
        for remote, local in [(preview_asset, preview), (header_asset, header)]:
            if not client.fetch_artifact(remote, str(local)):
                raise RuntimeError(f"result fetch failed: {remote}")
        evidence = []
        hosts: dict[str, int] = {}
        for job in jobs:
            raw = client.store_get(f"real-montage:{run_id}:{job.id}")
            if raw and raw != "NULL":
                item = json.loads(raw)
                evidence.append(item)
                hosts[item["host"]] = hosts.get(item["host"], 0) + 1
        mosaic_sha = sha(mosaic)
        area_sha = sha(area)
        preview_sha = sha(preview)
        result = {
            "schema": "titan-real-montage-m101-v1",
            "run_id": run_id,
            "dag": dag,
            "tasks": len(jobs),
            "completed": sum(value == "COMPLETED" for value in statuses.values()),
            "elapsed_seconds": time.time() - started,
            "workers": hosts,
            "cross_worker": len(hosts) >= 2,
            "mosaic_sha256": mosaic_sha,
            "expected_mosaic_sha256": args.expected_mosaic_sha,
            "mosaic_matches_control": mosaic_sha == args.expected_mosaic_sha,
            "mosaic_bytes": mosaic.stat().st_size,
            "area_sha256": area_sha,
            "expected_area_sha256": args.expected_area_sha,
            "area_matches_control": area_sha == args.expected_area_sha,
            "preview_sha256": preview_sha,
            "expected_preview_sha256": args.expected_preview_sha,
            "preview_matches_control": preview_sha == args.expected_preview_sha,
            "statuses": statuses,
            "stages": evidence,
            "artifacts": {"mosaic": str(mosaic), "area": str(area), "preview": str(preview), "header": str(header)},
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({key: result[key] for key in ["dag", "tasks", "completed", "elapsed_seconds", "workers", "cross_worker", "mosaic_matches_control", "area_matches_control", "preview_matches_control", "mosaic_sha256"]}, indent=2))
        if (not result["mosaic_matches_control"] or not result["area_matches_control"]
                or not result["preview_matches_control"] or not result["cross_worker"]):
            raise RuntimeError("real Montage verification invariant failed")
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    main()
