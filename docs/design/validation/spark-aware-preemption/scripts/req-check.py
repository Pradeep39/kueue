#!/usr/bin/env python3
"""Per-requirement verdicts for a capture.sh directory (design doc §4, R-1..R-13, SC-1/SC-2).

  req-check.py <capture-dir> [--cap N] [--partial] [--preemption] [--slots 12]

Reads the watch streams and samples directly. Each line is PASS / FAIL / INFO with the evidence.
Requirements a scenario does not exercise are reported as n/a rather than passed.
"""
import argparse, collections, json, re, subprocess, sys
from datetime import datetime, timezone

ap = argparse.ArgumentParser()
ap.add_argument("dir")
ap.add_argument("--cap", type=int)
ap.add_argument("--partial", action="store_true")
ap.add_argument("--preemption", action="store_true")
ap.add_argument("--slots", type=float, default=12.0)
a = ap.parse_args()
d = a.dir
here = __import__("os").path.dirname(__import__("os").path.abspath(__file__))


def ep(t): return datetime.fromisoformat(t.replace("Z", "+00:00")).timestamp()
def hms(t): return datetime.fromtimestamp(t, timezone.utc).strftime("%H:%M:%S")
def jl(p):
    try: return [json.loads(l) for l in open(p) if l.strip()]
    except FileNotFoundError: return []
def mib(q):
    m = re.match(r"^([0-9.]+)([A-Za-z]*)$", str(q or ""))
    if not m: return 0.0
    return float(m.group(1)) * {"": 1 / 2**20, "Ki": 1 / 1024, "Mi": 1, "Gi": 1024}.get(m.group(2), 0)

pods = [r for r in jl(f"{d}/watch/pods.jsonl")]
wls_raw = jl(f"{d}/watch/workloads.jsonl")
wls = [r for r in wls_raw if r["type"] != "RESYNC"]
sas = [r for r in jl(f"{d}/watch/sparkapps.jsonl") if r["type"] != "RESYNC"]
samples = jl(f"{d}/samples.jsonl")
results = []
def verdict(req, ok, evidence):
    results.append((req, "PASS" if ok is True else "FAIL" if ok is False else ok, evidence))

# ---------------------------------------------------------------- ledger vs reality (R-1, R-4, R-13)
# Replay pods to know live (ungated, non-terminal) Pods at each sample; compare with the CQ ledger.
stream = sorted([(ep(r["t"]), "p", r) for r in pods] + [(ep(s["t"]), "s", s) for s in samples], key=lambda x: x[0])
P, over, under, mem_mismatch, last_used = {}, [], [], [], None
for t, kind, r in stream:
    if kind == "p":
        if r["type"] == "RESYNC": P.clear(); continue
        if r["type"] == "DELETED": P.pop(r["name"], None)
        else: P[r["name"]] = r
        continue
    live = [p for p in P.values() if not p["gated"] and p["phase"] not in ("Succeeded", "Failed")]
    cpu = [float(v) for k, v in r.get("usage", {}).items() if k.endswith("cpu")]
    mem = [mib(v) for k, v in r.get("usage", {}).items() if k.endswith("memory")]
    used_cpu, used_mem = (cpu[0] if cpu else 0.0), (mem[0] if mem else 0.0)
    real_mem = sum(mib(p.get("mem")) for p in live)
    over.append((t, used_cpu - len(live)))
    under.append((t, len(live) - used_cpu))
    if live and used_cpu and abs(used_mem / used_cpu - real_mem / len(live)) > 1:
        mem_mismatch.append((t, used_mem / used_cpu, real_mem / len(live)))
    last_used = used_cpu

def longest(series, thresh=0.5):
    best, start, prev = (0, None), None, None
    for t, g in series:
        if g > thresh:
            start = start if start is not None else t
        else:
            if start is not None and (prev - start) > best[0]: best = (prev - start, start)
            start = None
        prev = t
    if start is not None and prev - start > best[0]: best = (prev - start, start)
    return best

o_len, o_at = longest(over)
u_len, u_at = longest(under)
peak_over = max((g for _, g in over), default=0)
# A scale-up admits quota a moment before its Pods start, and a scale-down releases it a moment after;
# a sustained gap is what R-4 rules out.
verdict("R-1 accounting tracks what runs", o_len <= 15 and u_len <= 5,
        f"ledger above live Pods: longest {o_len:.0f}s (peak +{peak_over:g}); live Pods above ledger: longest {u_len:.0f}s")
# The ledger must be empty at the end only if every application finished inside the capture.
final_state = {}
for r in sorted(sas, key=lambda r: ep(r["t"])):
    final_state[r["name"]] = r["state"]
all_done = bool(final_state) and all(v in ("COMPLETED", "FAILED") for v in final_state.values())
end_ok = (last_used or 0) == 0 if all_done else True
verdict("R-4 capacity returned on scale-down", o_len <= 15 and end_ok,
        f"longest over-reservation {o_len:.0f}s{' at ' + hms(o_at) if o_at else ''}; ledger at end {last_used or 0:g} cpu"
        + ("" if all_done else f" (apps not all finished: {final_state})"))
verdict("R-13 per-worker charge matches the Pod", not mem_mismatch,
        f"{len(mem_mismatch)} samples where ledger memory/cpu differs from live Pods' request" +
        (f", e.g. {mem_mismatch[0][1]:.0f}Mi vs {mem_mismatch[0][2]:.0f}Mi" if mem_mismatch else " (charge == Pod request)"))

# ---------------------------------------------------------------- SC-1 / SC-1b / SC-2 via edge-analyze
ea = subprocess.run([sys.executable, "-I", f"{here}/edge-analyze.py", d, str(a.slots)], capture_output=True, text=True).stdout
def line(prefix): return next((l for l in ea.splitlines() if l.startswith(prefix)), "")
verdict("SC-1 quota is a ceiling", "HELD" in line("SC-1 "), line("SC-1 "))
verdict("SC-1b live Pods within quota", "HELD" in line("SC-1b"), line("SC-1b"))
verdict("SC-2 every running worker has a grant (R-3, R-6)", "HELD" in line("SC-2"), line("SC-2"))
# capture.sh writes one RESYNC marker per watch (re)start: 1 is clean, more means a restart (whose
# gap the analyzers resync over), 0 is a capture from before the markers existed.
n_pod = sum(1 for r in pods if r["type"] == "RESYNC")
n_wl = sum(1 for r in wls_raw if r["type"] == "RESYNC")
if n_pod == 0 and n_wl == 0:
    verdict("capture integrity", "INFO", "capture predates restart markers; restarts cannot be ruled out")
else:
    verdict("capture integrity", n_pod == 1 and n_wl == 1, f"watch starts: pods={n_pod} workloads={n_wl} (1 = no restart)")

# ---------------------------------------------------------------- release latency after eviction (R-4)
# For each evicted, quota-holding slice: seconds from its application having no active Pod
# (ungated, not being deleted, not terminal: what IsActive counts) to its reservation dropping.
ev_rows = sorted((r for r in wls), key=lambda r: ep(r["t"]))
evicted_at, released_at = {}, {}
for r in ev_rows:
    c = r["cond"]
    if c.get("Evicted", {}).get("s") == "True":
        evicted_at.setdefault(r["name"], (ep(r["t"]), r["owner"]))
        if c.get("QuotaReserved", {}).get("s") != "True" and r["name"] not in released_at:
            released_at[r["name"]] = ep(r["t"])
pod_rows = sorted((r for r in pods if r["type"] != "RESYNC"), key=lambda r: ep(r["t"]))
def last_active_gone(app, after, before):
    """Time at which app last went from >=1 to 0 active Pods inside (after, before]."""
    P2, gone = {}, after
    for r in pod_rows:
        t = ep(r["t"])
        if t > before: break
        if r["app"] != app: continue
        if r["type"] == "DELETED": P2.pop(r["name"], None)
        else: P2[r["name"]] = r
        n = sum(1 for p in P2.values() if not p["gated"] and not p["deleting"] and p["phase"] not in ("Succeeded", "Failed"))
        if t > after and n == 0 and gone == after: gone = t
        if n > 0: gone = after
    return gone
lat = []
for name, (t_ev, app) in evicted_at.items():
    if name in released_at:
        # Measured from the later of the eviction and the last active Pod stopping: an
        # application evicted with no active Pod (e.g. just readmitted, no driver yet) owes
        # the release from the eviction itself.
        stopped = max(t_ev, last_active_gone(app, t_ev - 30, released_at[name]))
        lat.append(max(0.0, released_at[name] - stopped))
if a.preemption:
    lat.sort()
    verdict("R-4 evicted quota released promptly once Pods stop", (lat[-1] <= 3) if lat else "n/a",
            f"{len(lat)} evictions; release after last active Pod: median {lat[len(lat)//2]:.0f}s, max {lat[-1]:.0f}s" if lat else "no evictions")

# ---------------------------------------------------------------- R-2: resize without requeue/restart
states = collections.defaultdict(list)
for r in sorted((r for r in sas if r["type"] != "RESYNC"), key=lambda r: ep(r["t"])):
    s = states[r["name"]]
    if not s or s[-1] != r["state"]: s.append(r["state"])
reruns = {app: max(0, s.count("PENDING_RERUN") - 1) for app, s in states.items()}
slices = collections.Counter(r["owner"] for r in wls if r["type"] == "ADDED" and r.get("replaces"))
if not a.preemption:
    verdict("R-2 resize while admitted, no restart", all(v == 0 for v in reruns.values()) and sum(slices.values()) > 0,
            f"scale-up slices per app {dict(slices)}; restarts beyond the initial start {reruns}")
else:
    verdict("R-2 resize while admitted, no restart", "INFO", f"preemption run; restarts {reruns} are evictions, not resizes")

# ---------------------------------------------------------------- R-5: Kueue never rewrites the CR beyond suspend
gen_bumps = {}
for app in states:
    rows = [r for r in sorted(sas, key=lambda r: ep(r["t"])) if r.get("name") == app and r["type"] != "RESYNC"]
    gens, suspends = len({r["gen"] for r in rows}) - 1, sum(1 for x, y in zip(rows, rows[1:]) if x["suspend"] != y["suspend"])
    gen_bumps[app] = (gens, suspends)
ok5 = all(g <= s for g, s in gen_bumps.values())
verdict("R-5 the job's resource is not modified to account for it", ok5,
        "spec generation bumps vs suspend flips per app: " + ", ".join(f"{k} {g}/{s}" for k, (g, s) in sorted(gen_bumps.items())))

# ---------------------------------------------------------------- R-8: start below max, not rejected for max > quota
first = {r["owner"] for r in wls if not r.get("replaces") and r["cond"].get("QuotaReserved", {}).get("s") == "True"}
apps = set(states)
verdict("R-8 starts below its maximum (maxExecutors 30 > 12 slots)", apps <= first if apps else "n/a",
        f"admitted {sorted(first & apps)} of {sorted(apps)}")

# ---------------------------------------------------------------- R-11 / R-12 via the other tools
summ = open(f"{d}/summary.txt").read() if __import__("os").path.exists(f"{d}/summary.txt") else ""
peaks = {m.group(1): int(m.group(2)) for m in re.finditer(r"^\s+(\S+): peak (\d+) at", summ, re.M)}
if a.cap:
    verdict("R-11 workers awaiting quota bounded", all(v <= a.cap for v in peaks.values()) if peaks else "n/a",
            f"peak gated Pods per app {peaks} against maxPendingPods {a.cap}")
else:
    verdict("R-11 workers awaiting quota bounded", "INFO", f"no cap in this run; peak gated per app {peaks}")
ic = subprocess.run([sys.executable, "-I", f"{here}/idle-capacity.py", d, str(a.slots)], capture_output=True, text=True).stdout
m = re.search(r"= (\d+)s in (\d+) window", ic)
idle = int(m.group(1)) if m else -1
partials = re.search(r"partial admissions: (\d+)", ic)
if a.partial and not a.preemption:
    # one sample interval (2s) of slack: admission lands between samples.
    verdict("R-12 no free capacity idle behind a pending scale-up", idle <= 2,
            f"{idle}s idle; partial admissions {partials.group(1) if partials else '?'}")
else:
    verdict("R-12 no free capacity idle behind a pending scale-up", "INFO", f"{idle}s idle ({'preemption run' if a.preemption else 'partial off'})")

w = max(len(r[0]) for r in results)
for req, v, ev in results:
    print(f"{v:4}  {req:<{w}}  {ev}")
sys.exit(1 if any(v == "FAIL" for _, v, _ in results) else 0)
