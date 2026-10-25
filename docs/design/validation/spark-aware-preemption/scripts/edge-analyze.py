#!/usr/bin/env python3
"""Correctness + preemption report for a capture.sh directory.

  edge-analyze.py <capture-dir> [cohort-slots=12]

SC-1  quota ceiling: ClusterQueue cpu usage <= cohort slots (nominal + borrowing), every sample.
SC-2  every ungated, non-terminal executor Pod has a grant: per app, ungated executors <= the
      executor count granted to its quota-holding slice. Watches are separate streams, so a
      breach must persist for > GRACE seconds to be reported.
Preemption: per-victim eviction count, per-app suspends/reruns, preemptor scale-up admissions,
      and how many victims were evicted per preemption event.
"""
import collections, json, sys
from datetime import datetime, timezone

d = sys.argv[1]
SLOTS = float(sys.argv[2]) if len(sys.argv) > 2 else 12.0
GRACE = 3.0

def ep(t): return datetime.fromisoformat(t.replace("Z", "+00:00")).timestamp()
def jl(p):
    try: return [json.loads(l) for l in open(p) if l.strip()]
    except FileNotFoundError: return []
def hms(t): return datetime.fromtimestamp(t, timezone.utc).strftime("%H:%M:%S")

pods, wls, sas = jl(f"{d}/watch/pods.jsonl"), jl(f"{d}/watch/workloads.jsonl"), jl(f"{d}/watch/sparkapps.jsonl")
resyncs = {"pods": sum(r["type"] == "RESYNC" for r in pods), "workloads": sum(r["type"] == "RESYNC" for r in wls)}
print(f"watch (re)starts: pods={resyncs['pods']} workloads={resyncs['workloads']} (1 = no restart)")
events, samples = jl(f"{d}/watch/events.jsonl"), jl(f"{d}/samples.jsonl")

# ---------------------------------------------------------------- SC-1
over = [(s["t"][11:19], v) for s in samples for k, v in s.get("usage", {}).items()
        if k.endswith("cpu") and float(v) > SLOTS]
print(f"SC-1 quota ceiling ({SLOTS:g} cpu): {'HELD' if not over else 'VIOLATED'}"
      + (f" — {len(over)} samples, first {over[0]}" if over else f" on {len(samples)} samples"))

# ---------------------------------------------------------------- SC-2
stream = sorted([("pod", ep(r["t"]), r) for r in pods] + [("wl", ep(r["t"]), r) for r in wls], key=lambda x: x[1])
P, W = {}, {}
breach_since, breaches, checked = {}, [], 0
def granted(app):
    g = [w["granted"].get("executor", 0) for w in W.values()
         if w["owner"] == app and w["cond"].get("QuotaReserved", {}).get("s") == "True"
         and w["cond"].get("Finished", {}).get("s") != "True"]
    # An evicted slice keeps its reservation (and the cluster keeps charging it) until the
    # job reconciler stops the job, so it still covers its Pods until QuotaReserved drops.
    return max(g) if g else 0
quiet_until = 0.0
for kind, t, r in stream:
    store = P if kind == "pod" else W
    if r["type"] == "RESYNC":
        # Rebuild this stream from the replay; don't judge while it refills.
        store.clear(); quiet_until = t + GRACE
        breach_since.clear()
        continue
    if r["type"] == "DELETED": store.pop(r["name"], None)
    else: store[r["name"]] = r
    if t < quiet_until: continue
    apps = {p["app"] for p in P.values() if p["role"] == "executor"}
    for app in apps:
        ungated = sum(1 for p in P.values() if p["app"] == app and p["role"] == "executor" and not p["gated"]
                      and p["phase"] not in ("Succeeded", "Failed"))
        g = granted(app); checked += 1
        if ungated > g:
            breach_since.setdefault(app, (t, ungated, g))
        elif app in breach_since:
            t0, u, gg = breach_since.pop(app)
            if t - t0 > GRACE: breaches.append((hms(t0), app, u, gg, round(t - t0, 1)))
for app, (t0, u, g) in breach_since.items():
    if stream and stream[-1][1] - t0 > GRACE: breaches.append((hms(t0), app, u, g, "until end"))
real = []
P2 = {}
for kind, t, r in stream:
    if kind != "pod": continue
    if r["type"] == "RESYNC": P2.clear(); continue
    if r["type"] == "DELETED": P2.pop(r["name"], None)
    else: P2[r["name"]] = r
    n = sum(1 for p in P2.values() if not p["gated"] and p["phase"] not in ("Succeeded", "Failed"))
    if n > SLOTS: real.append((t, n))
spans, cur = [], None
for t, n in real:
    if cur and t - cur[1] <= 1.0: cur = (cur[0], t, max(cur[2], n))
    else:
        if cur: spans.append(cur)
        cur = (t, t, n)
if cur: spans.append(cur)
print(f"SC-1b real capacity (ungated live pods x 1cpu <= {SLOTS:g}): " + ("HELD" if not spans else
      f"EXCEEDED in {len(spans)} span(s): " + ", ".join(f"{hms(a)}+{b-a:.0f}s peak={m}" for a, b, m in spans[:6])))
print(f"SC-2 every running executor has a grant: {'HELD' if not breaches else 'VIOLATED'} ({checked} checks, grace {GRACE:g}s)")
for b in breaches[:10]: print(f"   {b[0]} {b[1]}: {b[2]} ungated executors vs {b[3]} granted for {b[4]}s")

# ---------------------------------------------------------------- preemption
evicted = {}
for r in wls:
    if r["type"] == "RESYNC": continue
    c = r["cond"].get("Evicted", {})
    if c.get("s") == "True" and r["name"] not in evicted:
        evicted[r["name"]] = (r["owner"], c.get("r"), c.get("lt"))
by_app = collections.Counter((o, rs) for o, rs, _ in evicted.values())
print(f"evictions: {len(evicted)} slices  " + ", ".join(f"{o}:{rs}={n}" for (o, rs), n in sorted(by_app.items())))
batches = collections.Counter(lt for _, rs, lt in evicted.values() if rs == "Preempted")
if batches:
    print(f"   preemption events: {len(batches)}  victims per event: {sorted(batches.values())}")
pre_msgs = [e for e in events if e.get("reason") == "Preempted"]
if pre_msgs:
    print(f"   e.g. {pre_msgs[0]['obj']}: {pre_msgs[0]['msg'][:160]}")

states = collections.defaultdict(list)
for r in sorted((r for r in sas if r["type"] != "RESYNC"), key=lambda r: ep(r["t"])):
    s = states[r["name"]]
    if not s or s[-1] != r["state"]: s.append(r["state"])
for app, s in sorted(states.items()):
    print(f"   {app}: suspends={s.count('SUSPENDED') + s.count('SUSPENDING')} reruns={s.count('PENDING_RERUN')} path={'>'.join(x[:4] for x in s)[:150]}")

# ---------------------------------------------------------------- per-app timeline
first_qr = {}
for r in wls:
    if r["type"] == "RESYNC": continue
    c = r["cond"].get("QuotaReserved", {})
    if c.get("s") == "True" and not r.get("replaces"):
        first_qr.setdefault(r["owner"], c.get("lt"))
for app in sorted({r["owner"] for r in wls if r["type"] != "RESYNC"}):
    per_slice = {r["name"]: ep(r["cond"]["QuotaReserved"]["lt"]) - ep(r["created"]) for r in wls
                 if r["type"] not in ("DELETED", "RESYNC") and r["owner"] == app and r.get("replaces")
                 and r["cond"].get("QuotaReserved", {}).get("s") == "True"}
    lats = sorted(per_slice.values())
    print(f"   {app}: first admitted {str(first_qr.get(app, '-'))[11:19]}  scale-up admissions={len(lats)}"
          + (f" latency median={lats[len(lats)//2]:.0f}s max={lats[-1]:.0f}s" if lats else ""))
