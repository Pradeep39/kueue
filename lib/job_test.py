#!/usr/bin/env python3
"""Job test: scales one elastic batch Job 1 -> 2 -> 3 -> 4 Pods (100m each) in a 400m
ClusterQueue, while watching the Workloads and the ClusterQueue status.

The 2 -> 3 and 3 -> 4 replacements are charged 500m and 700m if a slice and its replacement are
both charged. The test fails if ClusterQueue.status ever reports more than the quota, or more
than one workload holding quota.

ClusterQueue status writes are batched, so a Kueue with the bug fails this test only sometimes.
The edge test is the deterministic one.
"""
import argparse, datetime, json, os, subprocess, sys, threading, time

NS, CQ, QUOTA_M = "wl-slice-quota-job", "wl-slice-quota-job", 400
HERE = os.path.dirname(os.path.abspath(__file__))
MANIFESTS = os.path.join(HERE, "..", "manifests")


def ts():
    return datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]


def cpu_m(q):
    return int(q[:-1]) if q.endswith("m") else int(float(q) * 1000)


def kubectl(*args):
    return subprocess.run(["kubectl", *args], check=True, capture_output=True, text=True).stdout


def stream(args, procs):
    p = subprocess.Popen(["kubectl", *args], stdout=subprocess.PIPE, text=True)
    procs.append(p)
    dec, buf = json.JSONDecoder(), ""
    for line in p.stdout:
        buf += line
        while buf.strip():
            try:
                obj, end = dec.raw_decode(buf.lstrip())
            except json.JSONDecodeError:
                break
            buf = buf.lstrip()[end:]
            yield time.time(), obj


def cond(wl, t):
    return any(c["type"] == t and c["status"] == "True" for c in wl.get("status", {}).get("conditions", []))


class Recorder:
    def __init__(self, log):
        self.log, self.lock = log, threading.Lock()
        self.cq_samples, self.admitted_at, self.finished_at, self.replaces = [], {}, {}, {}
        self.held, self.api_over = {}, 0
        self.procs, self.threads = [], []

    def start(self):
        for target in (self.workloads, self.clusterqueue):
            t = threading.Thread(target=target, daemon=True)
            t.start()
            self.threads.append(t)

    def stop(self):
        for p in self.procs:
            p.terminate()
        for t in self.threads:
            t.join(timeout=10)

    def out(self, line):
        with self.lock:
            self.log.write(f"{ts()} {line}\n")
            self.log.flush()

    def workloads(self):
        for t, ev in stream(["get", "workloads", "-n", NS, "-w", "--output-watch-events", "-o", "json"], self.procs):
            wl, name = ev["object"], ev["object"]["metadata"]["name"]
            rep = wl["metadata"].get("annotations", {}).get("kueue.x-k8s.io/workload-slice-replacement-for", "")
            usage = sum(cpu_m(p.get("resourceUsage", {}).get("cpu", "0"))
                        for p in (wl.get("status", {}).get("admission") or {}).get("podSetAssignments", []))
            qr, adm, fin = cond(wl, "QuotaReserved"), cond(wl, "Admitted"), cond(wl, "Finished")
            with self.lock:
                if rep:
                    self.replaces[name] = rep.split("/", 1)[1]
                if adm:
                    self.admitted_at.setdefault(name, t)
                if fin:
                    self.finished_at.setdefault(name, t)
                if ev["type"] != "DELETED" and qr and not fin:
                    self.held[name] = usage
                else:
                    self.held.pop(name, None)
                total = sum(self.held.values())
                self.api_over += total > QUOTA_M
            self.out(f"WL {ev['type']:<8} {name} admitted={adm} finished={fin}"
                     f"{' replaces=' + rep if rep else ''} | slices with QuotaReserved in the API: {total}m")

    def clusterqueue(self):
        for _, cq in stream(["get", "clusterqueue", CQ, "-w", "-o", "json"], self.procs):
            st = cq.get("status", {})
            cpu = sum(cpu_m(r["total"]) for f in st.get("flavorsReservation", []) for r in f["resources"] if r["name"] == "cpu")
            reserving = st.get("reservingWorkloads", 0)
            with self.lock:
                self.cq_samples.append((cpu, reserving))
            self.out(f"CQ reserved cpu={cpu}m/{QUOTA_M}m reservingWorkloads={reserving}{'  OVER' if cpu > QUOTA_M or reserving > 1 else ''}")


def trial(n, outdir):
    kubectl("-n", NS, "delete", "job", "elastic", "--ignore-not-found", "--wait=true")
    for _ in range(60):
        if not kubectl("-n", NS, "get", "workloads", "-o", "name").strip():
            break
        time.sleep(1)
    with open(os.path.join(outdir, f"trial-{n}.log"), "w") as log:
        rec = Recorder(log)
        rec.start()
        time.sleep(3)
        kubectl("apply", "-f", os.path.join(MANIFESTS, "job.yaml"))
        time.sleep(10)
        for p in (2, 3, 4):
            rec.out(f"scale to {p}")
            kubectl("-n", NS, "patch", "job", "elastic", "--type=merge", "-p", json.dumps({"spec": {"parallelism": p}}))
            time.sleep(10)
        time.sleep(2)
        rec.stop()
    windows = [rec.finished_at[old] - rec.admitted_at[new] for new, old in rec.replaces.items()
               if new in rec.admitted_at and old in rec.finished_at]
    worst = max(rec.cq_samples, default=(0, 0))
    over = sum(cpu > QUOTA_M or r > 1 for cpu, r in rec.cq_samples)
    return {"replacements": len(windows), "windows": windows, "status_writes": len(rec.cq_samples),
            "status_over": over, "max_cpu": max((c for c, _ in rec.cq_samples), default=0),
            "max_reserving": max((r for _, r in rec.cq_samples), default=0), "api_over": rec.api_over}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    kubectl("apply", "-f", os.path.join(MANIFESTS, "job-setup.yaml"))
    failed = 0
    print(f"{'trial':<6} {'replacements':>12} {'window (s)':>18} {'status writes':>14} {'max cpu':>8} {'max reserving':>14}  result")
    for n in range(1, args.trials + 1):
        r = trial(n, args.out)
        w = r["windows"]
        span = f"{min(w):.3f}-{max(w):.3f}" if w else "-"
        ok = r["status_over"] == 0
        failed += not ok
        print(f"{n:<6} {r['replacements']:>12} {span:>18} {r['status_writes']:>14} {str(r['max_cpu']) + 'm':>8} {r['max_reserving']:>14}  {'PASS' if ok else 'FAIL'}")
    kubectl("-n", NS, "delete", "job", "elastic", "--ignore-not-found", "--wait=true")
    print()
    print("window = time from the replacement showing Admitted to the replaced slice showing Finished, in the API.")
    print("Both slices hold QuotaReserved in the API during the window by design; only the ClusterQueue status,")
    print("which Kueue writes from its cache, must stay within the quota.")
    print(f"job test: {'PASS' if not failed else f'FAIL ({failed} of {args.trials} trials)'}  (logs: {args.out})")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
