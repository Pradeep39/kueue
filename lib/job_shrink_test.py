#!/usr/bin/env python3
"""Job shrink test: an elastic batch Job's replacement slice shrinks while it waits for quota.

The Job runs 2 Pods of 100m in the 400m ClusterQueue, scales to 6 (the replacement needs 600m and
waits), then to 1 while the replacement is pending. If the pending replacement follows the Job, it
is admitted with 1 Pod, fewer than the 2 of the slice it replaces. The larger slice then covers the
smaller one: the test fails if ClusterQueue.status ever reports more than 400m or more than one
reserving workload, or if no replacement was admitted smaller than the slice it replaces.
"""
import argparse, json, os, sys, time

import job_test
from job_test import CQ, MANIFESTS, NS, QUOTA_M, Recorder, kubectl


def wait(cond, timeout=60):
    for _ in range(timeout * 2):
        if cond():
            return True
        time.sleep(0.5)
    return False


def trial(n, outdir):
    kubectl("-n", NS, "delete", "job", "elastic", "--ignore-not-found", "--wait=true")
    wait(lambda: not kubectl("-n", NS, "get", "workloads", "-o", "name").strip())
    with open(os.path.join(outdir, f"shrink-trial-{n}.log"), "w") as log:
        rec = Recorder(log)
        rec.start()
        time.sleep(3)
        kubectl("apply", "-f", os.path.join(MANIFESTS, "job.yaml"))
        wait(lambda: rec.admitted_at)
        for p, settle in ((2, 8), (6, 0)):
            rec.out(f"scale to {p}")
            kubectl("-n", NS, "patch", "job", "elastic", "--type=merge", "-p", json.dumps({"spec": {"parallelism": p}}))
            time.sleep(settle)
        # Wait for the replacement for 6 Pods to be created and left pending, then scale down.
        wait(lambda: any(name not in rec.admitted_at for name in rec.replaces), 30)
        time.sleep(2)
        rec.out("scale to 1")
        kubectl("-n", NS, "patch", "job", "elastic", "--type=merge", "-p", json.dumps({"spec": {"parallelism": 1}}))
        time.sleep(15)
        rec.stop()
    shrinks = [(new, old, rec.granted[old], rec.granted[new]) for new, old in rec.replaces.items()
               if new in rec.granted and old in rec.granted and rec.granted[new] < rec.granted[old]]
    over = sum(cpu > QUOTA_M or r > 1 for cpu, r in rec.cq_samples)
    return shrinks, over, rec.cq_samples


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    kubectl("apply", "-f", os.path.join(MANIFESTS, "job-setup.yaml"))
    failed = 0
    print(f"{'trial':<6} {'shrinking replacement':>34} {'status writes':>14} {'max cpu':>8} {'max reserving':>14}  result")
    for n in range(1, args.trials + 1):
        shrinks, over, samples = trial(n, args.out)
        ok = bool(shrinks) and over == 0
        failed += not ok
        s = ", ".join(f"{g_old} -> {g_new} Pods" for _, _, g_old, g_new in shrinks) or "none"
        print(f"{n:<6} {s:>34} {len(samples):>14} {str(max((c for c, _ in samples), default=0)) + 'm':>8} "
              f"{max((r for _, r in samples), default=0):>14}  {'PASS' if ok else 'FAIL'}")
    kubectl("-n", NS, "delete", "job", "elastic", "--ignore-not-found", "--wait=true")
    print()
    print(f"job-shrink test: {'PASS' if not failed else f'FAIL ({failed} of {args.trials} trials)'}  (logs: {args.out})")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
