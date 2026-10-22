#!/usr/bin/env bash
# Turn capture.sh output into a time-bucketed timeline plus a summary that answers:
#   - how far did gate-blocked Pods climb, against how many quota slots?
#   - did a pending slice's requested executor count climb while its grant stayed flat?
#   - how long did replacement slices wait, and what ended each long wait?
#   - how much conflict churn was there, on which objects, and does it track the gated count?
#
#   ./analyze.sh gated-churn-YYYYmmdd-HHMMSS [bucket-seconds, default 10]
#
# Writes <dir>/timeline.csv and <dir>/summary.txt (summary is also printed).
set -uo pipefail
OUT="${1:?usage: analyze.sh <capture-dir> [bucket-seconds]}"
BUCKET="${2:-10}"
[ -d "$OUT/watch" ] || { echo "$OUT does not look like a capture.sh directory" >&2; exit 1; }
exec python3 -I - "$OUT" "$BUCKET" <<'PY'
import collections, csv, glob, json, math, os, re, sys
from datetime import datetime, timezone

out, B = sys.argv[1], int(sys.argv[2])

def ep(ts):
    """RFC3339 (with or without fractional seconds) -> epoch float."""
    if not ts:
        return None
    ts = ts.strip().replace("Z", "+00:00")
    m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?(.*)", ts)
    if not m:
        return None
    base = datetime.fromisoformat(m.group(1) + (m.group(3) or "+00:00"))
    return base.timestamp() + (float(m.group(2)) if m.group(2) else 0.0)

def jl(path):
    if not os.path.exists(path):
        return []
    rows = []
    for line in open(path):
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
    return rows

def read_epoch(name, default):
    p = os.path.join(out, name)
    return float(open(p).read().strip()) if os.path.exists(p) else default

def mib(q):
    if q is None:
        return None
    m = re.match(r"^([0-9.]+)([A-Za-z]*)$", str(q))
    if not m:
        return None
    v, u = float(m.group(1)), m.group(2)
    f = {"": 1 / 2**20, "Ki": 1 / 1024, "Mi": 1, "Gi": 1024, "Ti": 1024**2,
         "k": 1e3 / 2**20, "K": 1e3 / 2**20, "M": 1e6 / 2**20, "G": 1e9 / 2**20, "m": 1e-3 / 2**20}.get(u)
    return v * f if f is not None else None

pods_ev = jl(f"{out}/watch/pods.jsonl")
wl_ev   = jl(f"{out}/watch/workloads.jsonl")
sa_ev   = jl(f"{out}/watch/sparkapps.jsonl")
evts    = jl(f"{out}/watch/events.jsonl")
samples = jl(f"{out}/samples.jsonl")

t0 = read_epoch("start.epoch", min([ep(r["t"]) for r in pods_ev + wl_ev] or [0]))
t1 = read_epoch("end.epoch",   max([ep(r["t"]) for r in pods_ev + wl_ev] or [t0]))
nb = max(1, math.ceil((t1 - t0) / B))
def bi(t):
    return None if t is None or t < t0 or t >= t0 + nb * B else int((t - t0) // B)
def blabel(i):
    return datetime.fromtimestamp(t0 + i * B, timezone.utc).strftime("%H:%M:%S")

def zeros():
    return [0] * nb
col = collections.defaultdict(zeros)
gated_by_app_at = [dict() for _ in range(nb)]

# ------------------------------------------------------------------ pods
# Replay the watch; at each bucket boundary snapshot the live state.
pod_state, seen_created = {}, set()
gated_deleted_at = []          # epochs of gated executor Pods deleted while still gated
ungated_at = []                # epochs of gate removals (Kueue granting quota)
pod_mem = []
evs = sorted(pods_ev, key=lambda r: ep(r["t"]) or 0)
k = 0
def snapshot(i):
    by_app = collections.Counter()
    for p in pod_state.values():
        if p["role"] != "executor" or p["deleting"]:
            continue
        if p["gated"]:
            by_app[p["app"]] += 1
            col["gated_total"][i] += 1
        elif p["phase"] == "Running":
            col["exec_running"][i] += 1
        elif p["phase"] == "Pending":
            col["exec_pending_ungated"][i] += 1
    gated_by_app_at[i] = dict(by_app)
for i in range(nb):
    edge = t0 + (i + 1) * B
    while k < len(evs) and (ep(evs[k]["t"]) or 0) < edge:
        r = evs[k]; k += 1
        if r["type"] == "RESYNC":
            pod_state.clear()
            continue
        t = ep(r["t"]); b = bi(t)
        prev = pod_state.get(r["name"])
        if r.get("mem"):
            pod_mem.append(mib(r["mem"]))
        if r["type"] == "DELETED":
            if prev and prev["role"] == "executor" and prev["gated"]:
                gated_deleted_at.append(t)
                if b is not None: col["gated_deleted"][b] += 1
            pod_state.pop(r["name"], None)
            continue
        if r["role"] == "executor" and r["name"] not in seen_created:
            seen_created.add(r["name"])
            c = bi(ep(r.get("created")))
            if c is not None: col["exec_created"][c] += 1
        if prev and prev["gated"] and not r["gated"] and r["role"] == "executor":
            ungated_at.append(t)
            if b is not None: col["ungated"][b] += 1
        pod_state[r["name"]] = r
    snapshot(i)

# ------------------------------------------------------------------ workloads
def finished(w):  return w["cond"].get("Finished", {}).get("s") == "True"
def reserved(w):  return w["cond"].get("QuotaReserved", {}).get("s") == "True"
wl_state, writers_wl = {}, collections.Counter()
peak_gap = {}                  # app -> (requested, granted, label)
evs = sorted(wl_ev, key=lambda r: ep(r["t"]) or 0)
k = 0
for i in range(nb):
    edge = t0 + (i + 1) * B
    while k < len(evs) and (ep(evs[k]["t"]) or 0) < edge:
        r = evs[k]; k += 1
        if r["type"] == "RESYNC":
            wl_state.clear()
            continue
        b = bi(ep(r["t"]))
        if r["type"] == "MODIFIED" and b is not None:
            col["wl_writes"][b] += 1
            writers_wl[r.get("writer", "-")] += 1
        if r["type"] == "DELETED":
            wl_state.pop(r["name"], None)
        else:
            wl_state[r["name"]] = r
    edge_t = t0 + (i + 1) * B
    granted_by_app = collections.Counter()
    for w in wl_state.values():
        if finished(w):
            continue
        if reserved(w):
            granted_by_app[w["owner"]] += w["granted"].get("executor", 0)
    for w in wl_state.values():
        if finished(w) or reserved(w):
            continue
        col["pending_slices"][i] += 1
        req = w["spec"].get("executor", 0)
        col["max_requested_pending"][i] = max(col["max_requested_pending"][i], req)
        age = edge_t - (ep(w["created"]) or edge_t)
        col["oldest_pending_s"][i] = max(col["oldest_pending_s"][i], int(age))
        g = granted_by_app[w["owner"]]
        best = peak_gap.get(w["owner"])
        if best is None or req - g > best[0] - best[1]:
            peak_gap[w["owner"]] = (req, g, blabel(i))
    col["granted_total"][i] = sum(granted_by_app.values())

# Admission latency per slice, from the final state each slice was seen in.
final_wl = {}
for r in wl_ev:
    if r["type"] not in ("DELETED", "RESYNC"):
        final_wl[r["name"]] = r
latencies = []
for n, w in final_wl.items():
    qr = w["cond"].get("QuotaReserved", {})
    if qr.get("s") == "True" and ep(qr.get("lt")) and ep(w["created"]):
        latencies.append((ep(qr["lt"]) - ep(w["created"]), n, w["owner"], bool(w.get("replaces")),
                          ep(qr["lt"]), w["spec"].get("executor", 0)))
latencies.sort(reverse=True)

# ------------------------------------------------------------------ sparkapps / events
writers_sa = collections.Counter()
for r in sa_ev:
    if r["type"] == "RESYNC":
        continue
    b = bi(ep(r["t"]))
    if r["type"] == "MODIFIED" and b is not None:
        col["sa_writes"][b] += 1
        writers_sa[r.get("writer", "-")] += 1
event_reasons = collections.Counter()
for r in evts:
    if r.get("type") == "RESYNC":
        continue
    event_reasons[(r.get("kind"), r.get("reason"))] += 1

# ------------------------------------------------------------------ logs
TS = re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d))")
CONFLICT = re.compile(r"object has been modified")
RES = re.compile(r'on ([a-z0-9.\-]+) \\*"([^"\\]+)\\*"')
CTRL = re.compile(r'"(?:controller|logger|controllerKind)"\s*[:=]\s*"([^"]+)"|controller="([^"]+)"')

def scan_conflicts(path, colname):
    by_res, by_ctrl, by_obj, errors = collections.Counter(), collections.Counter(), collections.Counter(), 0
    if not os.path.exists(path):
        return by_res, by_ctrl, by_obj, errors, False
    for line in open(path, errors="replace"):
        m = TS.search(line[:120])
        b = bi(ep(m.group(1))) if m else None
        if "Reconciler error" in line:
            errors += 1
            if b is not None: col[colname + "_reconcile_errors"][b] += 1
        if not CONFLICT.search(line):
            continue
        if b is not None: col[colname + "_conflicts"][b] += 1
        rm = RES.search(line)
        res = rm.group(1).split(".")[0] if rm else "?"
        by_res[res] += 1
        if rm: by_obj[f"{res}/{rm.group(2)}"] += 1
        cm = CTRL.search(line)
        by_ctrl[(cm.group(1) or cm.group(2)) if cm else "?"] += 1
    return by_res, by_ctrl, by_obj, errors, True

k_res, k_ctrl, k_obj, k_err, k_ok = scan_conflicts(f"{out}/logs/kueue.log", "kueue")
o_res, o_ctrl, o_obj, o_err, o_ok = scan_conflicts(f"{out}/logs/spark-operator.log", "operator")

DA_REQ  = re.compile(r"Requesting (\d+) new executors?")
K8S_REQ = re.compile(r"Going to request (\d+) executors")
IDLE    = re.compile(r"idle timeout|been idle|due to idle", re.I)
# ExecutorPodsAllocator cancelling Pending pods when its target drops. Gated pods never register
# with the driver, so they can never be idle — this, not the idle timeout, is how Spark removes them.
EXCESS  = re.compile(r"Deleting (\d+) excess pod requests")
idle_at, excess_at = [], []
for path in glob.glob(f"{out}/logs/driver-*.log"):
    for line in open(path, errors="replace"):
        m = TS.match(line)
        if not m:
            continue
        t = ep(m.group(1)); b = bi(t)
        if b is None:
            continue
        if (d := DA_REQ.search(line)):
            col["da_requested"][b] += int(d.group(1))
        if (d := K8S_REQ.search(line)):
            col["k8s_pod_requests"][b] += int(d.group(1))
        if IDLE.search(line):
            col["da_idle_removals"][b] += 1
            idle_at.append(t)
        if (d := EXCESS.search(line)):
            col["k8s_excess_deleted"][b] += int(d.group(1))
            excess_at.append(t)

# ------------------------------------------------------------------ prometheus
prom_totals = {}
for path in glob.glob(f"{out}/prom/*.json"):
    name = os.path.basename(path)[:-5]
    try:
        data = json.load(open(path))["data"]["result"]
    except Exception:
        continue
    for s in data:
        lbl = ",".join(f"{k}={v}" for k, v in sorted(s["metric"].items()))
        per_b = collections.defaultdict(list)
        for ts, v in s["values"]:
            b = bi(float(ts))
            if b is not None and v not in ("NaN", "+Inf", "-Inf"):
                per_b[b].append(float(v))
        if name in ("apiserver_409", "reconcile_errors", "workqueue_retries", "reconcile_by_result", "apiserver_writes"):
            # rates -> approximate count per bucket
            series = {b: sum(vs) / len(vs) * B for b, vs in per_b.items()}
            prom_totals[(name, lbl)] = sum(series.values())
        else:
            series = {b: max(vs) for b, vs in per_b.items()}
            prom_totals[(name, lbl)] = max(series.values(), default=0)
        if name == "apiserver_409":
            for b, v in series.items():
                col["api_409"][b] += v
        if name == "admission_wait_p95":
            for b, v in series.items():
                col["prom_admission_wait_p95_s"][b] = round(v, 1)

for s in samples:
    b = bi(ep(s["t"]))
    if b is None:
        continue
    mem = [v for kk, v in s.get("usage", {}).items() if kk.endswith("/memory")]
    if mem:
        col["cq_mem_usage_mi"][b] = int(mib(mem[0]) or 0)

# ------------------------------------------------------------------ timeline.csv
cols = ["gated_total", "exec_running", "exec_pending_ungated", "exec_created", "ungated", "gated_deleted",
        "pending_slices", "max_requested_pending", "granted_total", "oldest_pending_s",
        "wl_writes", "sa_writes", "kueue_conflicts", "kueue_reconcile_errors",
        "operator_conflicts", "api_409", "da_requested", "k8s_pod_requests", "k8s_excess_deleted", "da_idle_removals",
        "cq_mem_usage_mi", "prom_admission_wait_p95_s"]
with open(f"{out}/timeline.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["time"] + cols + ["gated_by_app"])
    for i in range(nb):
        w.writerow([blabel(i)] + [round(col[c][i], 1) if isinstance(col[c][i], float) else col[c][i] for c in cols]
                   + [" ".join(f"{a}={n}" for a, n in sorted(gated_by_app_at[i].items()))])

# ------------------------------------------------------------------ summary
def pearson(x, y):
    n = len(x)
    if n < 3: return None
    mx, my = sum(x) / n, sum(y) / n
    sx = math.sqrt(sum((a - mx) ** 2 for a in x)); sy = math.sqrt(sum((b - my) ** 2 for b in y))
    return None if sx == 0 or sy == 0 else sum((a - mx) * (b - my) for a, b in zip(x, y)) / (sx * sy)

L = []
p = L.append
p(f"window {blabel(0)} .. {blabel(nb - 1)} UTC, {nb} buckets of {B}s")
p("")
p("== Gate-blocked executor Pods")
pk = max(range(nb), key=lambda i: col["gated_total"][i])
p(f"peak gated total: {col['gated_total'][pk]} at {blabel(pk)}   ({gated_by_app_at[pk]})")
apps = sorted({a for d in gated_by_app_at for a in d})
for a in apps:
    j = max(range(nb), key=lambda i: gated_by_app_at[i].get(a, 0))
    p(f"  {a}: peak {gated_by_app_at[j].get(a, 0)} at {blabel(j)}")
nominal = next((v for s in samples for kk, v in s.get("nominal", {}).items() if kk.endswith("/memory")), None)
pm = [m for m in pod_mem if m]
if nominal and pm:
    per = max(set(pm), key=pm.count)
    p(f"quota: nominal memory {nominal}; executor pod request {per:.0f}Mi -> ~{int(mib(nominal) // per)} pod slots"
      " (Kueue's charge may differ from the pod request; see §8.5)")
elif nominal:
    p(f"quota: nominal memory {nominal}")
p(f"gate removals (Kueue ungating): {len(ungated_at)};  gated Pods deleted while still gated: {len(gated_deleted_at)}")
p("")
p("== Requested vs granted executors (pending replacement slice vs the app's admitted grant)")
for a, (req, g, at) in sorted(peak_gap.items()):
    p(f"  {a}: requested {req} vs granted {g} at {at}  (gap {req - g})")
mr = max(col["max_requested_pending"])
p(f"max requested on any pending slice: {mr}")
p("")
p("== Admission wait")
p(f"longest still-pending age seen: {max(col['oldest_pending_s'])}s")
for lat, n, owner, repl, at, req in latencies[:8]:
    p(f"  {lat:6.1f}s  {owner:10} {'replacement' if repl else 'initial    '} executors={req:<3} {n}")
# What ended each long wait? The claim under test: Kueue does not bound it; Spark reaping
# gated executors (idle timeout) shrinks the request until it fits.
long_waits = [x for x in latencies if x[0] >= 10 and x[3]]
if long_waits:
    p("  what preceded each replacement admitted after >=10s (15s look-back):")
    for lat, n, owner, repl, at, req in long_waits[:8]:
        gd = sum(1 for t in gated_deleted_at if at - 15 <= t <= at)
        il = sum(1 for t in idle_at if at - 15 <= t <= at)
        ex = sum(1 for t in excess_at if at - 15 <= t <= at)
        p(f"    {n}: gated Pods deleted={gd}, driver excess-request deletions={ex}, idle-removal lines={il}")
drv = glob.glob(f"{out}/logs/driver-*.log")
if not drv or not any(os.path.getsize(f) for f in drv):
    p("  WARNING: no driver log content captured — the excess/idle counts above are not evidence of absence")
p("")
p("== Conflict churn")
if k_ok:
    p(f"kueue: {sum(k_res.values())} conflicts, {k_err} 'Reconciler error' lines")
    p(f"  by resource:   {dict(k_res.most_common())}")
    p(f"  by controller: {dict(k_ctrl.most_common())}")
    p(f"  top objects:   {k_obj.most_common(5)}")
else:
    p("kueue: no log captured")
if o_ok:
    p(f"spark-operator: {sum(o_res.values())} conflicts  by resource {dict(o_res.most_common())}")
api = {lbl: v for (n, lbl), v in prom_totals.items() if n == "apiserver_409"}
if api:
    p("apiserver 409s (all clients, approx count): " + ", ".join(f"{l}: {v:.0f}" for l, v in sorted(api.items()) if v >= 0.5))
else:
    p("apiserver 409s: no Prometheus data")
p(f"writes seen on the watch: workloads {sum(col['wl_writes'])}, sparkapplications {sum(col['sa_writes'])}")
p(f"  workload writers:   {dict(writers_wl.most_common(6))}")
p(f"  sparkapp writers:   {dict(writers_sa.most_common(6))}")
p("")
p("== Does churn track the gated count?")
conf = [col["kueue_conflicts"][i] + col["operator_conflicts"][i] for i in range(nb)]
for name, series in (("gated_total", col["gated_total"]), ("pending_slices", col["pending_slices"]),
                     ("wl_writes", col["wl_writes"])):
    r = pearson(series, conf)
    p(f"  pearson(conflicts, {name}) = {'n/a' if r is None else f'{r:+.2f}'}")
on  = [conf[i] for i in range(nb) if col["gated_total"][i] > 0]
off = [conf[i] for i in range(nb) if col["gated_total"][i] == 0]
if on:
    p(f"  conflicts per minute: gated>0 {sum(on) / len(on) * 60 / B:.1f}  vs  gated=0 "
      f"{(sum(off) / len(off) * 60 / B) if off else float('nan'):.1f}   ({len(on)} vs {len(off)} buckets)")
p("")
p("Correlation is not mechanism: confirm against the top conflicted objects and the timeline.")
p("")
p("== Events (count by kind/reason)")
for (kind, reason), n in event_reasons.most_common(12):
    p(f"  {n:5}  {kind}/{reason}")

text = "\n".join(L) + "\n"
open(f"{out}/summary.txt", "w").write(text)
print(text)
print(f"timeline: {out}/timeline.csv")
PY
