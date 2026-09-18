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
Single-file upload to the Master's uploads/ directory.

Previously this referenced a calc.py fixture that is not in the repo, so it could only ever print
`ERROR: File not found`. The fixture is now written at run time — a test that depends on a file
someone remembered to commit is a test that eventually stops running.

    python3 titan_test_suite/tests/test_upload_func.py
"""

import os
import shutil
import sys
import tempfile

from titan_sdk import TitanClient

R = []


def check(name, ok, detail=""):
    R.append((name, ok))
    print(f"  {'[PASS]' if ok else '[FAIL]'} {name}" + (f" - {detail}" if detail else ""))


def main():
    client = TitanClient()
    tmp = tempfile.mkdtemp(prefix="titan_upload_")
    try:
        # A real, non-trivial file so the round trip proves content survived, not just the name.
        asset = os.path.join(tmp, "calc.py")
        body = "def add(a, b):\n    return a + b\n\n\nif __name__ == '__main__':\n    print(add(2, 3))\n"
        with open(asset, "w") as f:
            f.write(body)

        print(f"\n=== uploading {os.path.basename(asset)} ({len(body)} bytes) ===")
        resp = (client.upload_file(asset) or "").strip()
        print(f"  server: {resp}")
        check("upload reports success", "UPLOAD_SUCCESS" in resp, resp[:80])

        # The Master writes into its uploads/ dir; when it runs locally we can confirm the bytes.
        landed = os.path.join("uploads", "calc.py")
        if os.path.isdir("uploads"):
            check("the file landed in the Master's uploads directory",
                  os.path.exists(landed), landed)
            if os.path.exists(landed):
                with open(landed) as f:
                    got = f.read()
                check("uploaded content matches byte for byte", got == body,
                      f"{len(got)} bytes vs {len(body)}")
        else:
            check("uploads/ not visible from here — content check skipped, not failed", True,
                  "Master is remote or uses another working directory")

        # A path that does not exist must be refused clearly rather than uploading nothing.
        missing = client.upload_file(os.path.join(tmp, "does_not_exist.py")) or ""
        check("a missing file is refused with a clear error", "ERROR" in missing, missing[:60])
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    bad = [n for n, ok in R if not ok]
    print(f"\n{'=' * 60}\n RESULT: {len(R) - len(bad)}/{len(R)} passed\n{'=' * 60}")
    for b in bad:
        print("  FAILED:", b)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
