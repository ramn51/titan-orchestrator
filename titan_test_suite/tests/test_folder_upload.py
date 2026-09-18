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
Project-folder upload: zip a directory, ship it, confirm it arrived intact.

Previously this used `project_path` before defining it, so it raised NameError on line 22 and
could never have passed. It now builds the fixture, asserts the outcome, and returns an exit code.

    python3 titan_test_suite/tests/test_folder_upload.py
"""

import os
import shutil
import sys
import tempfile
import zipfile

from titan_sdk import TitanClient

R = []


def check(name, ok, detail=""):
    R.append((name, ok))
    print(f"  {'[PASS]' if ok else '[FAIL]'} {name}" + (f" - {detail}" if detail else ""))


def main():
    client = TitanClient()
    tmp = tempfile.mkdtemp(prefix="titan_folder_")
    project_name = "my_titan_project"
    project_path = os.path.join(tmp, project_name)
    try:
        # Nested file included on purpose: the SDK stores paths relative to the folder root, so a
        # flat-only fixture would not exercise that.
        os.makedirs(os.path.join(project_path, "lib"), exist_ok=True)
        files = {
            "main.py": "print('Main running')\n",
            "helper.py": "print('Helper loaded')\n",
            os.path.join("lib", "util.py"): "VALUE = 42\n",
        }
        for rel, body in files.items():
            with open(os.path.join(project_path, rel), "w") as f:
                f.write(body)
        # Must be excluded by the SDK's own filter.
        os.makedirs(os.path.join(project_path, "__pycache__"), exist_ok=True)
        with open(os.path.join(project_path, "__pycache__", "junk.pyc"), "w") as f:
            f.write("x")

        print(f"\n=== uploading folder '{project_name}' ({len(files)} files) ===")
        resp = (client.upload_project_folder(project_path) or "").strip()
        print(f"  server: {resp}")
        check("upload reports success", "UPLOAD_SUCCESS" in resp, resp[:80])

        leftover = os.path.exists(f"{project_name}.zip")
        check("the local temporary zip was cleaned up", not leftover,
              f"{project_name}.zip left behind in {os.getcwd()}" if leftover else "")

        landed = os.path.join("uploads", f"{project_name}.zip")
        if os.path.isdir("uploads"):
            check("the archive landed in the Master's uploads directory",
                  os.path.exists(landed), landed)
            if os.path.exists(landed):
                with zipfile.ZipFile(landed) as z:
                    names = set(z.namelist())
                    check("every source file is inside the archive",
                          all(rel.replace(os.sep, "/") in names for rel in files),
                          str(sorted(names)))
                    check("nested paths are preserved, not flattened",
                          "lib/util.py" in names, str(sorted(names)))
                    check("__pycache__ was excluded",
                          not any("__pycache__" in n for n in names), str(sorted(names)))
                    if "main.py" in names:
                        check("file content survived the round trip",
                              z.read("main.py").decode() == files["main.py"])
        else:
            check("uploads/ not visible from here — archive check skipped, not failed", True,
                  "Master is remote or uses another working directory")

        missing = client.upload_project_folder(os.path.join(tmp, "nope")) or ""
        check("a missing folder is refused with a clear error", "ERROR" in missing, missing[:60])
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        if os.path.exists(f"{project_name}.zip"):
            os.remove(f"{project_name}.zip")

    bad = [n for n, ok in R if not ok]
    print(f"\n{'=' * 60}\n RESULT: {len(R) - len(bad)}/{len(R)} passed\n{'=' * 60}")
    for b in bad:
        print("  FAILED:", b)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
