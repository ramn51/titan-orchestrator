"""Execute one real Montage module stage and relay files through Titan assets."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import titan_sdk.titan_sdk as sdk
from titan_sdk.titan_sdk import TitanClient


def decode(value: str) -> dict:
    return json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))


def run(command: list[str], cwd: Path) -> dict:
    started = time.time()
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True, timeout=600)
    record = {
        "command": command,
        "returncode": result.returncode,
        "seconds": time.time() - started,
        "stdout": result.stdout[-4000:],
        "stderr": result.stderr[-4000:],
    }
    if result.returncode != 0 or 'stat="ERROR"' in result.stdout:
        raise RuntimeError(json.dumps(record))
    return record


def fetch(client: TitanClient, remote: str, local: Path) -> None:
    local.parent.mkdir(parents=True, exist_ok=True)
    if not client.fetch_artifact(remote, str(local)):
        raise RuntimeError(f"failed to fetch asset {remote}")


def publish(client: TitanClient, local: Path, remote: str, scratch: Path) -> dict:
    staged = scratch / remote
    staged.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(local, staged)
    response = client.upload_file(str(staged))
    if response != "UPLOAD_SUCCESS":
        raise RuntimeError(f"failed to upload {remote}: {response}")
    return {
        "asset": remote,
        "bytes": local.stat().st_size,
        "sha256": hashlib.sha256(local.read_bytes()).hexdigest(),
    }


def publish_chunked(client: TitanClient, local: Path, remote: str, scratch: Path,
                    chunk_bytes: int = 1_000_000) -> dict:
    parts = []
    with local.open("rb") as source:
        index = 0
        while True:
            data = source.read(chunk_bytes)
            if not data:
                break
            name = f"{remote}.part{index:04d}"
            part = scratch / name
            part.parent.mkdir(parents=True, exist_ok=True)
            part.write_bytes(data)
            response = client.upload_file(str(part))
            if response != "UPLOAD_SUCCESS":
                raise RuntimeError(f"failed to upload {name}: {response}")
            parts.append({"asset": name, "bytes": len(data),
                          "sha256": hashlib.sha256(data).hexdigest()})
            index += 1
    manifest = {
        "schema": "titan-chunked-artifact-v1",
        "asset": remote,
        "bytes": local.stat().st_size,
        "sha256": hashlib.sha256(local.read_bytes()).hexdigest(),
        "parts": parts,
    }
    manifest_path = scratch / f"{remote}.parts.json"
    manifest_path.write_text(json.dumps(manifest, separators=(",", ":")), encoding="utf-8")
    published = publish(client, manifest_path, manifest_path.name, scratch / "manifest")
    return {"asset": remote, "bytes": manifest["bytes"], "sha256": manifest["sha256"],
            "parts": len(parts), "manifest": published["asset"]}


def fetch_chunked(client: TitanClient, remote: str, local: Path, scratch: Path) -> None:
    manifest_path = scratch / f"{remote}.parts.json"
    fetch(client, manifest_path.name, manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    local.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    with local.open("wb") as target:
        for item in manifest["parts"]:
            part = scratch / item["asset"]
            fetch(client, item["asset"], part)
            data = part.read_bytes()
            if hashlib.sha256(data).hexdigest() != item["sha256"]:
                raise RuntimeError(f"chunk checksum mismatch: {item['asset']}")
            target.write(data)
            digest.update(data)
    if local.stat().st_size != manifest["bytes"] or digest.hexdigest() != manifest["sha256"]:
        raise RuntimeError(f"artifact checksum mismatch: {remote}")


def background_coefficients(images: Path, corrections: Path, filename: str) -> tuple[str, str, str]:
    """Resolve mBgModel coefficients by image basename for a relocatable stage."""
    image_id = None
    for line in images.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(("|", "\\")):
            continue
        fields = stripped.split()
        if len(fields) >= 2 and Path(fields[-1]).name == filename:
            image_id = fields[0]
            break
    if image_id is None:
        raise RuntimeError(f"image {filename} is absent from {images.name}")
    for line in corrections.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(("|", "\\")):
            continue
        fields = stripped.split()
        if len(fields) >= 4 and fields[0] == image_id:
            return fields[1], fields[2], fields[3]
    raise RuntimeError(f"correction coefficients are absent for image id {image_id}")


def make_image_table_relocatable(table: Path) -> None:
    """Replace generated absolute FITS paths with basenames without changing table widths."""
    rewritten = []
    for line in table.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True):
        stripped = line.strip()
        if stripped and not stripped.startswith(("|", "\\")):
            fields = stripped.split()
            if fields:
                original = fields[-1]
                name = Path(original).name
                if original != name and original in line:
                    line = line.replace(original, " " * (len(original) - len(name)) + name, 1)
        rewritten.append(line)
    table.write_text("".join(rewritten), encoding="utf-8")


def main() -> None:
    payload = decode(sys.argv[1])
    sdk.TITAN_HOST = payload["master"]
    sdk.TITAN_PORT = int(payload.get("port", 9090))
    client = TitanClient()
    mode = payload["mode"]
    run_id = payload["run_id"]
    job_id = payload["job_id"]
    work = Path(tempfile.mkdtemp(prefix=f"montage-{run_id}-{job_id}-"))
    records: list[dict] = []
    outputs: list[dict] = []
    started = time.time()
    try:
        template = work / "template.hdr"
        if payload.get("template"):
            fetch(client, payload["template"], template)

        if mode == "project":
            source = work / "raw" / payload["input_name"]
            target = work / "proj" / payload["output_name"]
            fetch(client, payload["input_asset"], source)
            target.parent.mkdir(parents=True, exist_ok=True)
            records.append(run(["mProject", str(source), str(target), str(template)], work))
            outputs.append(publish(client, target, payload["output_asset"], work / "publish"))
            area = target.with_name(target.stem + "_area.fits")
            outputs.append(publish(client, area, payload["area_asset"], work / "publish"))

        elif mode == "metadata":
            proj = work / "proj"
            for item in payload["projected"]:
                fetch(client, item["asset"], proj / item["name"])
            table = work / "images.tbl"
            records.append(run(["mImgtbl", str(proj), str(table)], work))
            make_image_table_relocatable(table)
            outputs.append(publish(client, table, payload["table_asset"], work / "publish"))

        elif mode == "overlaps":
            images = work / "images.tbl"
            diffs = work / "diffs.tbl"
            fetch(client, payload["images_asset"], images)
            records.append(run(["mOverlaps", str(images), str(diffs)], work))
            outputs.append(publish(client, diffs, payload["diffs_asset"], work / "publish"))

        elif mode == "difference_fit":
            proj = work / "proj"
            diffdir = work / "diff"
            diffdir.mkdir(parents=True, exist_ok=True)
            for item in payload["projected"]:
                fetch(client, item["asset"], proj / item["name"])
            diffs = work / "diffs.tbl"
            fetch(client, payload["diffs_asset"], diffs)
            records.append(run(["mDiffExec", "-p", str(proj), str(diffs), str(template), str(diffdir)], work))
            fits = work / "fits.tbl"
            records.append(run(["mFitExec", str(diffs), str(fits), str(diffdir)], work))
            outputs.append(publish(client, fits, payload["fits_asset"], work / "publish"))

        elif mode == "background_model":
            images = work / "images.tbl"
            fits = work / "fits.tbl"
            corrections = work / "corrections.tbl"
            fetch(client, payload["images_asset"], images)
            fetch(client, payload["fits_asset"], fits)
            records.append(run(["mBgModel", str(images), str(fits), str(corrections)], work))
            outputs.append(publish(client, corrections, payload["corrections_asset"], work / "publish"))

        elif mode == "background":
            proj = work / "proj"
            corr = work / "corr"
            source = proj / payload["input_name"]
            area = proj / payload["area_name"]
            target = corr / payload["output_name"]
            fetch(client, payload["input_asset"], source)
            fetch(client, payload["area_asset"], area)
            images = work / "images.tbl"
            corrections = work / "corrections.tbl"
            fetch(client, payload["images_asset"], images)
            fetch(client, payload["corrections_asset"], corrections)
            target.parent.mkdir(parents=True, exist_ok=True)
            a, b, c = background_coefficients(images, corrections, payload["input_name"])
            records.append(run(["mBackground", str(source), str(target), a, b, c], work))
            outputs.append(publish(client, target, payload["output_asset"], work / "publish"))
            target_area = target.with_name(target.stem + "_area.fits")
            outputs.append(publish(client, target_area, payload["output_area_asset"], work / "publish"))

        elif mode == "add":
            corr = work / "corr"
            for item in payload["corrected"]:
                fetch(client, item["asset"], corr / item["name"])
            images = work / "images.tbl"
            fetch(client, payload["images_asset"], images)
            final = work / "m101_mosaic.fits"
            records.append(run(["mAdd", "-p", str(corr), str(images), str(template), str(final)], work))
            outputs.append(publish_chunked(client, final, payload["mosaic_asset"], work / "publish"))
            final_area = work / "m101_mosaic_area.fits"
            outputs.append(publish_chunked(client, final_area, payload["mosaic_area_asset"], work / "publish"))

        elif mode == "preview":
            final = work / "m101_mosaic.fits"
            fetch_chunked(client, payload["mosaic_asset"], final, work / "chunks")
            preview = work / "m101_mosaic.png"
            header = work / "m101_mosaic.hdr"
            records.append(run(["mViewer", "-gray", str(final), "0s", "max", "gaussian-log", "-out", str(preview)], work))
            records.append(run(["mGetHdr", str(final), str(header)], work))
            outputs.append(publish(client, preview, payload["preview_asset"], work / "publish"))
            outputs.append(publish(client, header, payload["header_asset"], work / "publish"))

        else:
            raise ValueError(f"unknown mode: {mode}")

        evidence = {
            "schema": "titan-real-montage-stage-v1",
            "run_id": run_id,
            "job_id": job_id,
            "mode": mode,
            "host": socket.gethostname(),
            "started": started,
            "finished": time.time(),
            "commands": records,
            "outputs": outputs,
        }
        client.store_put(f"real-montage:{run_id}:{job_id}", json.dumps(evidence, separators=(",", ":")))
        print(json.dumps(evidence))
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
