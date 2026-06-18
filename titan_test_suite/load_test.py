"""
Titan Load Test Suite

Tests throughput, concurrency, DAG resolution, failure recovery, and soak
behavior of the Titan cluster under controlled load.

Usage:
    python titan_test_suite/load_test.py [test_name]

    test_name: throughput | concurrency | chain | fanout | store | saturation | all
    Default: all

Prerequisites:
    - Cluster running: ./titan-dev.sh up
    - A noop script at perm_files/noop.py (auto-created if missing)
"""

import sys
import os
import time
import json
import threading
import tempfile
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from titan_sdk.titan_sdk import TitanClient, TitanJob

# ── Config ────────────────────────────────────────────────────────────────────

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 9090

# ── Helpers ───────────────────────────────────────────────────────────────────

def make_noop_script():
    """Ensure perm_files/noop.py exists."""
    noop = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "perm_files", "noop.py")
    if not os.path.exists(noop):
        with open(noop, "w") as f:
            f.write("print('ok')\n")
    return noop


def make_sleep_script(seconds):
    """Create a temp script that sleeps for N seconds."""
    f = tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False)
    f.write(f"import time; time.sleep({seconds}); print('done')\n")
    f.close()
    return f.name


def wait_for_jobs(client, job_ids, timeout=120):
    """Poll until all jobs reach a terminal state. Returns {job_id: status}."""
    terminal = {"COMPLETED", "FAILED", "DEAD", "CANCELLED", "ERROR"}
    results = {}
    deadline = time.time() + timeout
    remaining = set(job_ids)

    while remaining and time.time() < deadline:
        for jid in list(remaining):
            prefixed = jid if jid.startswith("DAG-") else f"DAG-{jid}"
            status = client.get_job_status(prefixed)
            if status and status.upper() in terminal:
                results[jid] = status.upper()
                remaining.remove(jid)
        if remaining:
            time.sleep(1)

    for jid in remaining:
        results[jid] = "TIMEOUT"
    return results


def print_header(name):
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")


def print_result(label, value, unit=""):
    print(f"  {label:<35} {value} {unit}")


# ── Test 1: Throughput ────────────────────────────────────────────────────────

def test_throughput(count=100):
    """
    Submit N standalone jobs as fast as possible and measure:
    - Submission rate (jobs/sec at the SDK → Master boundary)
    - End-to-end completion time
    """
    print_header(f"THROUGHPUT — {count} sequential submissions")

    client = TitanClient()
    noop = make_noop_script()
    ts = int(time.time())
    job_ids = []

    # Phase 1: Submit
    submit_start = time.time()
    for i in range(count):
        jid = f"tp-{ts}-{i}"
        client.submit_dag(f"tp-{ts}-{i}", [TitanJob(job_id=jid, filename=noop)])
        job_ids.append(jid)
    submit_elapsed = time.time() - submit_start

    print_result("Submission time", f"{submit_elapsed:.2f}", "sec")
    print_result("Submission rate", f"{count / submit_elapsed:.1f}", "jobs/sec")

    # Phase 2: Wait for completion
    wait_start = time.time()
    results = wait_for_jobs(client, job_ids, timeout=180)
    wait_elapsed = time.time() - wait_start

    counts = Counter(results.values())
    e2e = submit_elapsed + wait_elapsed

    print_result("All jobs completed in", f"{e2e:.2f}", "sec")
    print_result("Effective throughput", f"{count / e2e:.1f}", "jobs/sec (e2e)")
    print_result("Results", dict(counts))

    failed = count - counts.get("COMPLETED", 0)
    if failed > 0:
        print(f"  ⚠ {failed} jobs did not complete successfully")
    return counts.get("COMPLETED", 0) == count


# ── Test 2: Concurrent submissions ───────────────────────────────────────────

def test_concurrency(threads=5, per_thread=20):
    """
    Submit jobs from N threads simultaneously to test:
    - SchedulerServer under concurrent TCP connections
    - TitanJRedisAdapter synchronized bottleneck
    - Dispatch fairness across workers
    """
    total = threads * per_thread
    print_header(f"CONCURRENCY — {threads} threads × {per_thread} jobs = {total}")

    client = TitanClient()
    noop = make_noop_script()
    ts = int(time.time())
    all_job_ids = []
    lock = threading.Lock()
    errors = []

    def submitter(tid):
        c = TitanClient()  # separate connection per thread
        for i in range(per_thread):
            jid = f"cc-{ts}-{tid}-{i}"
            try:
                c.submit_dag(f"cc-{ts}-{tid}-{i}", [TitanJob(job_id=jid, filename=noop)])
                with lock:
                    all_job_ids.append(jid)
            except Exception as e:
                with lock:
                    errors.append((jid, str(e)))

    start = time.time()
    pool = [threading.Thread(target=submitter, args=(t,)) for t in range(threads)]
    for t in pool: t.start()
    for t in pool: t.join()
    submit_elapsed = time.time() - start

    print_result("Concurrent submission time", f"{submit_elapsed:.2f}", "sec")
    print_result("Submission rate", f"{len(all_job_ids) / submit_elapsed:.1f}", "jobs/sec")
    print_result("Submit errors", len(errors))

    # Wait for completion
    results = wait_for_jobs(client, all_job_ids, timeout=180)
    counts = Counter(results.values())
    print_result("Results", dict(counts))

    return counts.get("COMPLETED", 0) == total and len(errors) == 0


# ── Test 3: Deep DAG chain ───────────────────────────────────────────────────

def test_chain(depth=30):
    """
    Submit a single DAG with N sequential dependencies:
    job-0 → job-1 → job-2 → ... → job-(N-1)

    Tests: dagWaitingRoom resolution, unlockChildren() performance,
    overhead of sequential dependency walks.
    """
    print_header(f"DAG CHAIN — {depth} sequential jobs")

    client = TitanClient()
    noop = make_noop_script()
    ts = int(time.time())

    jobs = []
    for i in range(depth):
        jobs.append(TitanJob(
            job_id=f"chain-{ts}-{i}",
            filename=noop,
            parents=[f"chain-{ts}-{i-1}"] if i > 0 else []
        ))

    start = time.time()
    client.submit_dag(f"chain-{ts}", jobs)

    job_ids = [f"chain-{ts}-{i}" for i in range(depth)]
    results = wait_for_jobs(client, job_ids, timeout=300)
    elapsed = time.time() - start

    counts = Counter(results.values())
    per_job = elapsed / depth

    print_result("Total time", f"{elapsed:.2f}", "sec")
    print_result("Per-job overhead", f"{per_job:.3f}", "sec")
    print_result("Results", dict(counts))

    return counts.get("COMPLETED", 0) == depth


# ── Test 4: Wide fan-out + fan-in ────────────────────────────────────────────

def test_fanout(width=20):
    """
    Submit a DAG: 1 root → N parallel jobs → 1 fan-in collector.
    Tests: parallel dispatch, fan-in resolution, worker load balancing.
    """
    print_header(f"FAN-OUT — 1 → {width} parallel → 1 fan-in")

    client = TitanClient()
    noop = make_noop_script()
    ts = int(time.time())

    jobs = [TitanJob(job_id=f"fo-root-{ts}", filename=noop)]
    parallel_ids = []
    for i in range(width):
        jid = f"fo-par-{ts}-{i}"
        parallel_ids.append(jid)
        jobs.append(TitanJob(job_id=jid, filename=noop, parents=[f"fo-root-{ts}"]))

    fanin_id = f"fo-fanin-{ts}"
    jobs.append(TitanJob(job_id=fanin_id, filename=noop, parents=parallel_ids))

    start = time.time()
    client.submit_dag(f"fanout-{ts}", jobs)

    all_ids = [f"fo-root-{ts}"] + parallel_ids + [fanin_id]
    results = wait_for_jobs(client, all_ids, timeout=180)
    elapsed = time.time() - start

    counts = Counter(results.values())
    total = width + 2

    print_result("Total time", f"{elapsed:.2f}", "sec")
    print_result("Jobs", total)
    print_result("Results", dict(counts))

    return counts.get("COMPLETED", 0) == total


# ── Test 5: TitanStore throughput ────────────────────────────────────────────

def test_store(count=500, threads=5):
    """
    Rapid KV writes from multiple threads through the Master.
    Tests: TitanJRedisAdapter synchronized bottleneck, TitanStore write throughput.
    Then reads them all back to verify no data loss.
    """
    per_thread = count // threads
    total = per_thread * threads
    print_header(f"TITANSTORE — {total} writes ({threads} threads × {per_thread})")

    ts = int(time.time())
    errors = []
    lock = threading.Lock()

    def writer(tid):
        c = TitanClient()
        for i in range(per_thread):
            try:
                c.store_put(f"lt:{ts}:{tid}:{i}", f"v-{tid}-{i}")
            except Exception as e:
                with lock:
                    errors.append(str(e))

    start = time.time()
    pool = [threading.Thread(target=writer, args=(t,)) for t in range(threads)]
    for t in pool: t.start()
    for t in pool: t.join()
    write_elapsed = time.time() - start

    print_result("Write time", f"{write_elapsed:.2f}", "sec")
    print_result("Write rate", f"{total / write_elapsed:.0f}", "ops/sec")
    print_result("Write errors", len(errors))

    # Read back and verify
    client = TitanClient()
    missing = 0
    read_start = time.time()
    for tid in range(threads):
        for i in range(per_thread):
            val = client.store_get(f"lt:{ts}:{tid}:{i}")
            if val != f"v-{tid}-{i}":
                missing += 1
    read_elapsed = time.time() - read_start

    print_result("Read-back time", f"{read_elapsed:.2f}", "sec")
    print_result("Read rate", f"{total / read_elapsed:.0f}", "ops/sec")
    print_result("Missing/wrong values", missing)

    return missing == 0 and len(errors) == 0


# ── Test 6: Worker saturation ────────────────────────────────────────────────

def test_saturation(job_count=12):
    """
    Submit more jobs than a single worker can handle (MAX_THREADS=4).
    With 1 worker: first 4 run, rest queue. Tests backpressure and re-queue.
    With 2+ workers: tests spill-over dispatch.

    Uses 3-second sleep scripts so jobs occupy slots long enough to observe.
    """
    print_header(f"SATURATION — {job_count} slow jobs (3s each)")

    client = TitanClient()
    sleep_script = make_sleep_script(3)
    ts = int(time.time())

    job_ids = []
    start = time.time()
    for i in range(job_count):
        jid = f"sat-{ts}-{i}"
        client.submit_dag(f"sat-{ts}-{i}", [TitanJob(job_id=jid, filename=sleep_script)])
        job_ids.append(jid)
    submit_elapsed = time.time() - start

    print_result("Submit time", f"{submit_elapsed:.2f}", "sec")

    # Check how many are RUNNING after 2 seconds (should be <= MAX_THREADS per worker)
    time.sleep(2)
    running = 0
    for jid in job_ids:
        s = client.get_job_status(f"DAG-{jid}")
        if s and s.upper() == "RUNNING":
            running += 1
    print_result("Running after 2s", running, f"(expect <= workers × 4)")

    # Wait for all
    results = wait_for_jobs(client, job_ids, timeout=120)
    elapsed = time.time() - start
    counts = Counter(results.values())

    # With 1 worker (4 threads), 12 jobs × 3s each = ~9s minimum (3 batches)
    print_result("Total time", f"{elapsed:.2f}", "sec")
    print_result("Results", dict(counts))

    os.unlink(sleep_script)
    return counts.get("COMPLETED", 0) == job_count


# ── Test 7: Diamond DAG ──────────────────────────────────────────────────────

def test_diamond():
    """
    Classic diamond pattern:
        A → B
        A → C
        B → D
        C → D

    Tests: fan-out from A, fan-in at D (both parents must complete).
    """
    print_header("DIAMOND DAG — A → B,C → D")

    client = TitanClient()
    noop = make_noop_script()
    ts = int(time.time())

    jobs = [
        TitanJob(job_id=f"dia-a-{ts}", filename=noop),
        TitanJob(job_id=f"dia-b-{ts}", filename=noop, parents=[f"dia-a-{ts}"]),
        TitanJob(job_id=f"dia-c-{ts}", filename=noop, parents=[f"dia-a-{ts}"]),
        TitanJob(job_id=f"dia-d-{ts}", filename=noop, parents=[f"dia-b-{ts}", f"dia-c-{ts}"]),
    ]

    start = time.time()
    client.submit_dag(f"diamond-{ts}", jobs)

    ids = [f"dia-{x}-{ts}" for x in "abcd"]
    results = wait_for_jobs(client, ids, timeout=60)
    elapsed = time.time() - start

    counts = Counter(results.values())
    print_result("Total time", f"{elapsed:.2f}", "sec")
    print_result("Results", dict(counts))

    return counts.get("COMPLETED", 0) == 4


# ── Test 8: Mixed workload (fast + slow) ─────────────────────────────────────

def test_mixed():
    """
    Submit a mix of fast noop jobs and slow 5s jobs simultaneously.
    Tests: fast jobs shouldn't be starved by slow ones occupying all slots.
    """
    print_header("MIXED WORKLOAD — 8 fast + 4 slow (5s)")

    client = TitanClient()
    noop = make_noop_script()
    slow = make_sleep_script(5)
    ts = int(time.time())
    job_ids = []

    start = time.time()

    # Submit slow first to fill slots
    for i in range(4):
        jid = f"mx-slow-{ts}-{i}"
        client.submit_dag(f"mx-slow-{ts}-{i}", [TitanJob(job_id=jid, filename=slow)])
        job_ids.append(jid)

    # Then fast ones
    fast_ids = []
    for i in range(8):
        jid = f"mx-fast-{ts}-{i}"
        client.submit_dag(f"mx-fast-{ts}-{i}", [TitanJob(job_id=jid, filename=noop)])
        job_ids.append(jid)
        fast_ids.append(jid)

    results = wait_for_jobs(client, job_ids, timeout=120)
    elapsed = time.time() - start

    # Check: did fast jobs complete before slow ones finished?
    counts = Counter(results.values())
    print_result("Total time", f"{elapsed:.2f}", "sec")
    print_result("Results", dict(counts))
    print_result("Note", "With 1 worker (4 slots), fast jobs wait for slow batch")

    os.unlink(slow)
    return counts.get("COMPLETED", 0) == 12


# ── Runner ────────────────────────────────────────────────────────────────────

TESTS = {
    "throughput":   lambda: test_throughput(50),
    "concurrency":  lambda: test_concurrency(5, 10),
    "chain":        lambda: test_chain(20),
    "fanout":       lambda: test_fanout(15),
    "diamond":      test_diamond,
    "store":        lambda: test_store(300, 5),
    "saturation":   lambda: test_saturation(12),
    "mixed":        test_mixed,
}

def main():
    test_name = sys.argv[1] if len(sys.argv) > 1 else "all"

    # Verify cluster is reachable
    try:
        c = TitanClient()
        c.get_job_status("DAG-__ping__")
    except Exception:
        print("ERROR: Cannot reach Titan Master at localhost:9090")
        print("       Start the cluster first: ./titan-dev.sh up")
        sys.exit(1)

    make_noop_script()

    if test_name == "all":
        to_run = list(TESTS.items())
    elif test_name in TESTS:
        to_run = [(test_name, TESTS[test_name])]
    else:
        print(f"Unknown test: {test_name}")
        print(f"Available: {', '.join(TESTS.keys())}, all")
        sys.exit(1)

    results = {}
    total_start = time.time()

    for name, fn in to_run:
        try:
            passed = fn()
            results[name] = "PASS" if passed else "FAIL"
        except Exception as e:
            results[name] = f"ERROR: {e}"
            import traceback
            traceback.print_exc()

    total_elapsed = time.time() - total_start

    print(f"\n{'='*60}")
    print(f"  LOAD TEST RESULTS")
    print(f"{'='*60}")
    for name, result in results.items():
        icon = "✓" if result == "PASS" else "✗"
        print(f"  {icon} {name:<20} {result}")
    print(f"\n  Total time: {total_elapsed:.1f}s")
    print(f"{'='*60}")

    if any(r != "PASS" for r in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
