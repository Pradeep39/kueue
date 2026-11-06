#!/usr/bin/env python3
"""Edge test: drives Kueue's cache through workload slice replacement cases with hand-made
Workloads, and checks what the ClusterQueue is charged after each step.

Each Workload is owned (as controller) by a ConfigMap standing in for its job, names a LocalQueue
that does not exist so the scheduler never admits it, and gets its quota reservation through the
status subresource. ClusterQueue.status is written from Kueue's cache, so after each step it shows
what the cache charges.
"""
import argparse, json, os, subprocess, sys, time, datetime

CQ = "wl-slice-quota-edge"
HERE = os.path.dirname(os.path.abspath(__file__))


def kubectl(*args, stdin=None):
    r = subprocess.run(["kubectl", *args], input=stdin, capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(f"kubectl {' '.join(args)}: {r.stderr.strip()}")
    return r.stdout


def uid(kind, ns, name):
    return kubectl("-n", ns, "get", kind, name, "-o", "jsonpath={.metadata.uid}")


def now():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make(ns, name, cpu, owner="job", replaces=None):
    """Creates the Workload and reserves cpu for it in CQ."""
    # Every slice of a chain carries the chain's slice name, as Kueue sets it.
    ann = {"kueue.x-k8s.io/workload-slice-name": "a"}
    if replaces:
        ann["kueue.x-k8s.io/workload-slice-replacement-for"] = replaces
    meta = {"name": name, "namespace": ns, "annotations": ann}
    if owner:
        meta["ownerReferences"] = [{"apiVersion": "v1", "kind": "ConfigMap", "name": owner,
                                    "uid": uid("configmap", ns, owner), "controller": True}]
    wl = {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "Workload", "metadata": meta,
          "spec": {"queueName": "parked", "podSets": [{"name": "main", "count": 1, "template": {"spec": {
              "restartPolicy": "Never",
              "containers": [{"name": "c", "image": "busybox", "resources": {"requests": {"cpu": cpu}}}]}}}]}}
    kubectl("apply", "-f", "-", stdin=json.dumps(wl))
    t = now()
    status = {"status": {
        "admission": {"clusterQueue": CQ, "podSetAssignments": [
            {"name": "main", "count": 1, "flavors": {"cpu": "wl-slice-quota"}, "resourceUsage": {"cpu": cpu}}]},
        "conditions": [
            {"type": "QuotaReserved", "status": "True", "reason": "QuotaReserved", "message": "edge-case test", "lastTransitionTime": t},
            {"type": "Admitted", "status": "True", "reason": "Admitted", "message": "edge-case test", "lastTransitionTime": t}]}}
    kubectl("-n", ns, "patch", "workload", name, "--subresource=status", "--type=merge", "-p", json.dumps(status))


def finish(ns, name):
    st = json.loads(kubectl("-n", ns, "get", "workload", name, "-o", "json"))["status"]
    st["conditions"].append({"type": "Finished", "status": "True", "reason": "Succeeded", "message": "edge-case test", "lastTransitionTime": now()})
    kubectl("-n", ns, "patch", "workload", name, "--subresource=status", "--type=merge", "-p", json.dumps({"status": {"conditions": st["conditions"]}}))


def delete(ns, name):
    kubectl("-n", ns, "delete", "workload", name, "--wait=false")
    for _ in range(60):
        if subprocess.run(["kubectl", "-n", ns, "get", "workload", name], capture_output=True).returncode:
            return
        time.sleep(0.5)
    kubectl("-n", ns, "patch", "workload", name, "--type=merge", "-p", '{"metadata":{"finalizers":null}}')


def touch(ns, name):
    """Updates the Workload without changing its reservation, so the cache processes it again."""
    kubectl("-n", ns, "label", "workload", name, f"edge-touch={int(time.time())}", "--overwrite")


def charged():
    """Waits for ClusterQueue.status to settle and returns (reserved cpu, reserving workloads)."""
    # A status patch made by hand does not prompt a ClusterQueue status write, so touch the
    # ClusterQueue to have it recomputed from the cache.
    kubectl("annotate", "clusterqueue", CQ, f"edge-poke={time.time()}", "--overwrite")
    last, stable = None, 0
    for _ in range(40):
        time.sleep(0.5)
        st = json.loads(kubectl("get", "clusterqueue", CQ, "-o", "json")).get("status", {})
        cpu = next((r["total"] for f in st.get("flavorsReservation", []) for r in f["resources"] if r["name"] == "cpu"), "0")
        cur = (cpu, st.get("reservingWorkloads", 0))
        stable = stable + 1 if cur == last else 0
        last = cur
        if stable >= 4:
            break
    return last


def clean():
    for ns in ("wl-slice-quota-edge", "wl-slice-quota-edge-2"):
        for name in kubectl("-n", ns, "get", "workloads", "-o", "name").split():
            delete(ns, name.split("/", 1)[1])
    charged()


SCENARIOS = [
    ("basic replacement", [
        ("A (2 cpu)", lambda: make("wl-slice-quota-edge", "a", "2"), ("2", 1)),
        ("B (3 cpu) replaces A", lambda: make("wl-slice-quota-edge", "b", "3", replaces="wl-slice-quota-edge/a"), ("3", 1)),
        ("A finishes", lambda: finish("wl-slice-quota-edge", "a"), ("3", 1)),
    ]),
    ("replacement leaves first", [
        ("A (2 cpu)", lambda: make("wl-slice-quota-edge", "a", "2"), ("2", 1)),
        ("B (3 cpu) replaces A", lambda: make("wl-slice-quota-edge", "b", "3", replaces="wl-slice-quota-edge/a"), ("3", 1)),
        ("B deleted", lambda: delete("wl-slice-quota-edge", "b"), ("2", 1)),
    ]),
    ("chain of three", [
        ("A (2 cpu)", lambda: make("wl-slice-quota-edge", "a", "2"), ("2", 1)),
        ("B (3 cpu) replaces A", lambda: make("wl-slice-quota-edge", "b", "3", replaces="wl-slice-quota-edge/a"), ("3", 1)),
        ("C (4 cpu) replaces B", lambda: make("wl-slice-quota-edge", "c", "4", replaces="wl-slice-quota-edge/b"), ("4", 1)),
        ("C deleted", lambda: delete("wl-slice-quota-edge", "c"), ("3", 1)),
    ]),
    ("chain, middle slice finishes first", [
        ("A (2 cpu)", lambda: make("wl-slice-quota-edge", "a", "2"), ("2", 1)),
        ("B (3 cpu) replaces A", lambda: make("wl-slice-quota-edge", "b", "3", replaces="wl-slice-quota-edge/a"), ("3", 1)),
        ("C (4 cpu) replaces B", lambda: make("wl-slice-quota-edge", "c", "4", replaces="wl-slice-quota-edge/b"), ("4", 1)),
        ("B finishes", lambda: finish("wl-slice-quota-edge", "b"), ("4", 1)),
        ("C updated", lambda: touch("wl-slice-quota-edge", "c"), ("4", 1)),
        ("C deleted", lambda: delete("wl-slice-quota-edge", "c"), ("2", 1)),
    ]),
    ("replacement that shrinks", [
        ("A (2 cpu)", lambda: make("wl-slice-quota-edge", "a", "2"), ("2", 1)),
        ("B (1 cpu) replaces A", lambda: make("wl-slice-quota-edge", "b", "1", replaces="wl-slice-quota-edge/a"), ("2", 1)),
        ("A finishes", lambda: finish("wl-slice-quota-edge", "a"), ("1", 1)),
    ]),
    ("self-reference", [
        ("S (1 cpu) names itself", lambda: make("wl-slice-quota-edge", "s", "1", replaces="wl-slice-quota-edge/s"), ("1", 1)),
    ]),
    ("another namespace", [
        ("X (1 cpu) in wl-slice-quota-edge-2", lambda: make("wl-slice-quota-edge-2", "x", "1"), ("1", 1)),
        ("B (3 cpu) names wl-slice-quota-edge-2/x", lambda: make("wl-slice-quota-edge", "b", "3", replaces="wl-slice-quota-edge-2/x"), ("4", 2)),
    ]),
    ("another job at the replaced slice's key", [
        ("A (2 cpu)", lambda: make("wl-slice-quota-edge", "a", "2"), ("2", 1)),
        ("B (3 cpu) replaces A", lambda: make("wl-slice-quota-edge", "b", "3", replaces="wl-slice-quota-edge/a"), ("3", 1)),
        ("A deleted", lambda: delete("wl-slice-quota-edge", "a"), ("3", 1)),
        ("A' (1 cpu) of other-job at A's key", lambda: make("wl-slice-quota-edge", "a", "1", owner="other-job"), ("4", 2)),
        ("B updated", lambda: touch("wl-slice-quota-edge", "b"), ("4", 2)),
    ]),
    ("Workloads without a controller", [
        ("A (2 cpu), no controller", lambda: make("wl-slice-quota-edge", "a", "2", owner=None), ("2", 1)),
        ("B (3 cpu), no controller, replaces A", lambda: make("wl-slice-quota-edge", "b", "3", owner=None, replaces="wl-slice-quota-edge/a"), ("5", 2)),
    ]),
]

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    kubectl("apply", "-f", os.path.join(HERE, "..", "manifests", "edge-setup.yaml"))
    fails = 0
    with open(os.path.join(args.out, "edge.log"), "w") as log:
        def say(line):
            print(line)
            log.write(line + "\n")
        for name, steps in SCENARIOS:
            clean()
            say(f"== {name}")
            for label, action, want in steps:
                action()
                got = charged()
                ok = got == want
                fails += not ok
                say(f"   {'ok  ' if ok else 'FAIL'} {label:<44} charged {got[0]:>2} cpu, {got[1]} reserving   (want {want[0]} cpu, {want[1]})")
        clean()
        say("")
        say(f"edge test: {'PASS' if not fails else f'FAIL ({fails} steps)'}  (log: {args.out}/edge.log)")
    sys.exit(1 if fails else 0)
