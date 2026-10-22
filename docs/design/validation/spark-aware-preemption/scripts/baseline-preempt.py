#!/usr/bin/env python3
"""Per-eviction preemption baseline for capture.sh dirs.  preempt.py <dir> [slots=12]"""
import collections, json, re, sys, statistics as st
from datetime import datetime, timezone

d = sys.argv[1]; SLOTS = float(sys.argv[2]) if len(sys.argv) > 2 else 12.0
def ep(t):
    if not t: return None
    t = t.strip().replace("Z", "+00:00")
    m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?(.*)", t)
    return datetime.fromisoformat(m.group(1) + (m.group(3) or "+00:00")).timestamp() + (float(m.group(2)) if m.group(2) else 0)
def hms(t): return datetime.fromtimestamp(t, timezone.utc).strftime("%H:%M:%S") if t else "-"
def jl(p):
    out = []
    try:
        for i, l in enumerate(open(p)):
            if l.strip():
                try: out.append(json.loads(l))
                except ValueError: pass
    except FileNotFoundError: pass
    return out
def tot(x): return sum(x.values()) if x else 0
def qr(w): return w["cond"].get("QuotaReserved", {}).get("s") == "True"
def fin(w): return w["cond"].get("Finished", {}).get("s") == "True"

wls = jl(f"{d}/watch/workloads.jsonl"); pods = jl(f"{d}/watch/pods.jsonl")
sas = jl(f"{d}/watch/sparkapps.jsonl"); evs = jl(f"{d}/watch/events.jsonl")
resync = {k: sum(r.get("type") == "RESYNC" for r in v) for k, v in (("wl", wls), ("pods", pods), ("sa", sas), ("ev", evs))}
t_end = float(open(f"{d}/end.epoch").read())
t_start = float(open(f"{d}/start.epoch").read())

# preemptor mapping from events
pre_of = collections.defaultdict(list)
for e in evs:
    if e.get("reason") == "PreemptedWorkload":
        m = re.search(r"Preempted workload [^/]+/(\S+)", e.get("msg", ""))
        if m: pre_of[m.group(1)].append((ep(e["t"]), e["obj"]))
def preemptor(v, t):
    c = pre_of.get(v, [])
    return min(c, key=lambda x: abs(x[0] - t))[1] if c else None

# ---------------- workload replay
W = {}; cur_ev = {}; evictions = []; seen_ev = set(); first_qr = {}; first_seen = {}; release = {}; spec_at_qr = {}
app_size = collections.defaultdict(list)   # app -> [(t, slots)]
def appslots(app):
    g = [tot(w["granted"]) for w in W.values() if w["owner"] == app and qr(w) and not fin(w)]
    return max(g) if g else 0
def used():
    apps = {w["owner"] for w in W.values()}
    return sum(appslots(a) for a in apps)
for r in wls:
    if r["type"] == "RESYNC": W.clear(); continue
    t = ep(r["t"]); n = r["name"]
    before_used = used()
    before = dict(W)
    if r["type"] == "DELETED": W.pop(n, None)
    else: W[n] = r
    first_seen.setdefault(n, r)
    if qr(r) and n not in first_qr:
        first_qr[n] = ep(r["cond"]["QuotaReserved"]["lt"]); spec_at_qr[n] = (tot(r["spec"]), tot(r["granted"]), r["granted"].get("executor", 0))
    c = r["cond"].get("Evicted", {})
    if c.get("s") == "True" and (n, c.get("lt")) not in seen_ev:
        seen_ev.add((n, c.get("lt"))); cur_ev[n] = c.get("lt")
        evictions.append(dict(lt=c.get("lt"), t=ep(c["lt"]), seen=t, victim=n, app=r["owner"], vsize=tot(r["granted"]),
                              vexec=r["granted"].get("executor", 0), reason=c.get("r"), used_before=before_used,
                              snapshot={k: dict(v) for k, v in before.items()}))
    if n in cur_ev and not qr(r) and (n, cur_ev[n]) not in release: release[(n, cur_ev[n])] = t
    for a in {w["owner"] for w in W.values()} | {r["owner"]}:
        s = appslots(a)
        if not app_size[a] or app_size[a][-1][1] != s: app_size[a].append((t, s))

# ---------------- pods: attempts and executor run intervals
P = {}  # key (name, created) -> dict
for r in pods:
    if r["type"] == "RESYNC": continue
    t = ep(r["t"]); k = (r["name"], r["created"])
    p = P.setdefault(k, dict(app=r["app"], role=r["role"], created=ep(r["created"]), run=None, end=None))
    if p["run"] is None and r["phase"] == "Running" and not r["gated"] and not r["deleting"]: p["run"] = t
    if p["end"] is None and (r["type"] == "DELETED" or r["deleting"] or r["phase"] in ("Succeeded", "Failed")):
        p["end"] = t
drivers = collections.defaultdict(list)
for p in P.values():
    if p["role"] == "driver": drivers[p["app"]].append(p)
for a in drivers: drivers[a].sort(key=lambda p: p["created"])
def attempt(app, t):
    ds = [p for p in drivers.get(app, []) if p["created"] <= t]
    return ds[-1] if ds else None
def exec_seconds(app, t0, t1):
    s = 0.0; live = 0
    for p in P.values():
        if p["app"] != app or p["role"] != "executor" or p["run"] is None or p["created"] < t0 - 1: continue
        a, b = max(p["run"], t0), min(p["end"] or t_end, t1)
        if b > a: s += b - a
        if p["run"] <= t1 - 2 and (p["end"] is None or p["end"] > t1 - 2): live += 1
    return s, live

# ---------------- app completion
sa_first, sa_done, sa_state = {}, {}, {}
for r in sas:
    if r["type"] == "RESYNC": continue
    t = ep(r["t"]); sa_first.setdefault(r["name"], t)
    if r["state"] in ("COMPLETED", "FAILED") and r["name"] not in sa_done: sa_done[r["name"]] = (t, r["state"])
    sa_state[r["name"]] = r["state"]
apps = sorted(sa_first)
hi = [a for a in apps if "hi" in a]

def size_at(app, t):
    s = 0
    for tt, v in app_size.get(app, []):
        if tt <= t: s = v
        else: break
    return s

rows = []
for e in evictions:
    v = e["victim"]; pre = preemptor(v, e["t"])
    pw = W.get(pre) or first_seen.get(pre)
    # preemptor spec at eviction time from snapshot (fallback: first record)
    snap = e["snapshot"]
    pr = snap.get(pre) or first_seen.get(pre)
    old = pr.get("replaces", None) and pr["replaces"].split("/")[-1]
    old_slots = tot(snap[old]["granted"]) if old and old in snap and qr(snap[old]) else 0
    old_exec = snap[old]["granted"].get("executor", 0) if old and old in snap and qr(snap[old]) else 0
    req = tot(pr["spec"]); delta = req - old_slots; dexec = pr["spec"].get("executor", 0) - old_exec
    free_raw = SLOTS - e["used_before"]; free = max(0.0, free_raw)
    need = max(0, delta - free)
    adm = first_qr.get(pre); created = ep(pr["created"])
    adm_spec = spec_at_qr.get(pre, (None, None, None))
    drv = attempt(e["app"], e["t"])
    if drv and drv["run"] and drv["run"] <= e["t"] and (drv["end"] is None or drv["end"] >= e["t"] - 1):
        age = e["t"] - drv["run"]; xs, live = exec_seconds(e["app"], drv["created"], e["t"])
    else:
        age, xs, live = 0.0, 0.0, 0
    papp = pr["owner"]
    # preemptor trajectory after the eviction
    later = [(tt, s) for tt, s in app_size[papp] if tt > e["t"]]
    pk = max([s for _, s in later] + [size_at(papp, e["t"]) ])
    nxt_down = next((tt for tt, s in later if s < (adm_spec[1] or req)), None)
    done = sa_done.get(papp, (None,))[0]
    bend = next((tt for tt, s_ in later if s_ <= old_slots), done or t_end)
    bpeak = max([s_ for tt, s_ in later if tt < bend] + [size_at(papp, e["t"])])
    rows.append(dict(free_raw=free_raw, surv2=e["vsize"] - need >= 2 and SLOTS - bpeak >= 2, surv4=e["vsize"] - need >= 4 and SLOTS - bpeak >= 4, bpeak=bpeak, blen=bend - e["t"], t=e["t"], victim_app=e["app"], victim=v[-5:], vsize=e["vsize"], vexec=e["vexec"], preemptor=papp,
                     pslice=(pre or "?")[-5:], initial=not old, req=req, delta=delta, dexec=dexec, free=free, need=need,
                     ratio=(e["vsize"] / need) if need else float("inf"), created=created, adm=adm,
                     adm_granted=adm_spec[1], wait=(adm - created) if adm else None,
                     pre_ev=e["t"] - created, teardown=(adm - e["t"]) if adm else None,
                     release=(release.get((v, e["lt"])) - e["t"]) if release.get((v, e["lt"])) else None,
                     age=age, xsec=xs, live=live, fit2=e["vsize"] - need >= 2, fit4=e["vsize"] - need >= 4,
                     ppeak=pk, pdown=(nxt_down - e["t"]) if nxt_down else None, pdone=(done - e["t"]) if done else None))

# ---------------- counterfactuals over the preemptor's actual size trace H(t)
# B: strict priority + ideal partial reclaim. Low apps shrink toward floor F slots (driver + F-1
#    executors) before any is evicted; an app is evicted only when SLOTS - H < n_alive * F; an evicted
#    app restarts (from zero) as soon as F slots are free again. Lost work = (F-1) executors x time
#    since that app's (re)start -- a lower bound on what it would have had.
# C: floor-protected: never take a low app below F; the preemptor is capped at SLOTS - n_low * F
#    (partial admission). 0 evictions; cost = preemptor slot-seconds denied.
def simulate(F):
    if not hi: return None
    h = hi[0]; trace = app_size[h]
    t_arr = next((tt for tt, s in trace if s > 0), None)
    if t_arr is None: return None
    hdone = sa_done.get(h, (t_end,))[0]
    lows = [a for a in apps if a != h]
    alive = {a: (attempt(a, t_arr)["run"] if attempt(a, t_arr) and attempt(a, t_arr)["run"] else t_arr) for a in lows
             if not (a in sa_done and sa_done[a][0] <= t_arr)}
    pend = []; ev = 0; lost = 0.0
    nlow = len(alive); capH = SLOTS - nlow * F; denied = 0.0
    pts = [(tt, s) for tt, s in trace if tt <= hdone] + [(hdone, 0)]
    for (tt, s), (tn, _) in zip(pts, pts[1:]):
        if s > capH: denied += (s - capH) * (tn - tt)
        cap = SLOTS - s
        while len(alive) * F > cap and alive:
            a = max(alive, key=lambda x: alive[x])          # evict the youngest (least lost)
            lost += (F - 1) * (tt - alive.pop(a)); ev += 1; pend.append(a)
        while pend and (len(alive) + 1) * F <= cap:
            alive[pend.pop(0)] = tt
    return dict(evictions=ev, lost_xsec=round(lost), hi_cap=capH, hi_denied_slot_sec=round(denied))
sim = {F: simulate(F) for F in (2, 4)}

# ---------------- restarts (driver attempts) per app
restarts = {a: max(0, len(drivers.get(a, [])) - 1) for a in apps}
ev_by_app = collections.Counter(r["victim_app"] for r in rows)
xs_by_app = collections.defaultdict(float); age_by_app = collections.defaultdict(float)
for r in rows: xs_by_app[r["victim_app"]] += r["xsec"]; age_by_app[r["victim_app"]] += r["age"]

out = dict(dir=d.rstrip("/").split("/")[-1], resync=resync, rows=rows, sim=sim, restarts=restarts, ev_by_app=dict(ev_by_app),
           xs_by_app=dict(xs_by_app), age_by_app=dict(age_by_app),
           completion={a: ((sa_done[a][0] - sa_first[a]) if a in sa_done else None, sa_done.get(a, (None, sa_state.get(a)))[1]) for a in apps},
           window=t_end - t_start, hi_peak=max((s for _, s in app_size[hi[0]]), default=0) if hi else None,
           hi_time_at_ge={k: sum((b[0] - a[0]) for a, b in zip(app_size[hi[0]], app_size[hi[0]][1:] + [(sa_done.get(hi[0], (t_end,))[0], 0)]) if a[1] >= k) for k in (8, 9, 10, 11, 12)} if hi else {})
print(json.dumps(out, default=str))
