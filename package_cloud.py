#!/usr/bin/env python3
"""Dependency-free cloud bundle packager for Windows, Linux, macOS, and Unix."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


ROOT = Path(__file__).resolve().parent
ENGINE = ROOT / "target" / "titan-orchestrator-1.0-SNAPSHOT.jar"
STORE = ROOT / "perm_files" / "TitanStore.jar"
CLOUD = ROOT / "deployment" / "cloud"


def require(path: Path) -> None:
    if not path.is_file():
        raise SystemExit(f"Required file not found: {path}")


def maven_command(explicit: str | None) -> str:
    if explicit:
        return explicit
    for name in ("mvn", "mvn.cmd"):
        found = shutil.which(name)
        if found:
            return found
    for wrapper in (ROOT / "mvnw", ROOT / "mvnw.cmd"):
        if wrapper.is_file():
            return str(wrapper)
    raise SystemExit("Maven not found. Install it, pass --maven, or use --skip-build with a current JAR.")


def ensure_current_jar(skip_build: bool, maven: str | None) -> None:
    if not skip_build:
        subprocess.run([maven_command(maven), "-q", "-DskipTests", "package"],
                       cwd=ROOT, check=True)
    require(ENGINE)
    if skip_build:
        jar_time = ENGINE.stat().st_mtime_ns
        sources = list((ROOT / "src" / "main" / "java").rglob("*.java")) + [ROOT / "pom.xml"]
        newer = next((path for path in sources if path.stat().st_mtime_ns > jar_time), None)
        if newer:
            raise SystemExit(f"Target JAR is older than {newer}. Re-run without --skip-build.")


def copy_tree(source: Path, destination: Path) -> None:
    shutil.copytree(source, destination, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"))


def git_value(*args: str) -> str:
    try:
        return subprocess.check_output(["git", "-C", str(ROOT), *args], text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def build_info() -> str:
    digest = hashlib.sha256(ENGINE.read_bytes()).hexdigest().upper()
    return ("Titan cloud bundle\n"
            f"Branch: {git_value('branch', '--show-current')}\n"
            f"Commit: {git_value('rev-parse', 'HEAD')}\n"
            f"Engine SHA-256: {digest}\n"
            f"Packaged UTC: {datetime.now(timezone.utc).isoformat()}\n")


def zip_bundle(source: Path, destination: Path) -> None:
    destination.unlink(missing_ok=True)
    with ZipFile(destination, "w", ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(source.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(source.parent).as_posix()
            info = ZipInfo(relative, datetime.fromtimestamp(path.stat().st_mtime).timetuple()[:6])
            mode = 0o755 if path.suffix == ".sh" else 0o644
            info.external_attr = (0o100000 | mode) << 16
            info.compress_type = ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())


def package(output: Path) -> tuple[Path, Path]:
    output = output.resolve()
    stage = output / "_staging"
    output.mkdir(parents=True, exist_ok=True)
    if stage.exists():
        if output not in stage.resolve().parents:
            raise SystemExit(f"Refusing to clear staging outside {output}")
        shutil.rmtree(stage)

    master = stage / "titan-master-bundle"
    master_perm = master / "perm_files"
    worker = stage / "titan-worker-bundle"
    master_perm.mkdir(parents=True)
    worker.mkdir(parents=True)
    for directory in (master / "uploads", master / "titan_workspace", master / "telemetry"):
        directory.mkdir(parents=True)
        (directory / ".gitkeep").write_text("", encoding="ascii")

    shutil.copy2(ENGINE, master_perm / ENGINE.name)
    shutil.copy2(ENGINE, master_perm / "Worker.jar")
    shutil.copy2(STORE, master_perm / STORE.name)
    shutil.copy2(ROOT / "perm_files" / "server_dashboard.py", master_perm)
    for directory in ("dag_viewer", "cluster_monitor", "templates", "static", "dag_samples"):
        copy_tree(ROOT / "perm_files" / directory, master_perm / directory)
    copy_tree(ROOT / "titan_sdk", master / "titan_sdk")
    shutil.copy2(ROOT / "setup.py", master)
    for launcher in ("start_master.sh", "start_master.ps1"):
        shutil.copy2(CLOUD / launcher, master)
    shutil.copy2(CLOUD / "config.master.properties", master / "config.properties")

    shutil.copy2(ENGINE, worker / "Worker.jar")
    copy_tree(ROOT / "titan_sdk", worker / "titan_sdk")
    shutil.copy2(ROOT / "setup.py", worker)
    for launcher in ("start_worker.sh", "start_worker.ps1"):
        shutil.copy2(CLOUD / launcher, worker)
    shutil.copy2(CLOUD / "config.worker.properties", worker / "config.properties")

    info = build_info()
    (master / "BUILD_INFO.txt").write_text(info, encoding="utf-8")
    (worker / "BUILD_INFO.txt").write_text(info, encoding="utf-8")

    master_zip = output / "titan-master-bundle.zip"
    worker_zip = output / "titan-worker-bundle.zip"
    zip_bundle(master, master_zip)
    zip_bundle(worker, worker_zip)
    shutil.rmtree(stage)
    return master_zip, worker_zip


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true", help="package the existing verified target JAR")
    parser.add_argument("--output", default="dist/cloud", help="bundle output directory")
    parser.add_argument("--maven", help="Maven executable path")
    args = parser.parse_args()

    ensure_current_jar(args.skip_build, args.maven)
    require(STORE)
    for filename in ("start_master.sh", "start_master.ps1", "start_worker.sh", "start_worker.ps1",
                     "config.master.properties", "config.worker.properties"):
        require(CLOUD / filename)
    master, worker = package(ROOT / args.output)
    for path in (master, worker):
        digest = hashlib.sha256(path.read_bytes()).hexdigest().upper()
        print(f"{path}  {path.stat().st_size} bytes  SHA256={digest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
