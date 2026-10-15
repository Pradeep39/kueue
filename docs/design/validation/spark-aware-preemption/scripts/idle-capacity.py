"""For a capture dir: seconds during which the cohort had free slots while a scale-up slice was
pending (the R-12 waste), and every partial admission (granted < requested at admission)."""
import json, sys
from datetime import datetime
d = sys.argv[1]; SLOTS = float(sys.argv[2]) if len(sys.argv) > 2 else 12
def ep(t): return datetime.fromisoformat(t.replace("Z", "+00:00")).timestamp()
S = [json.loads(l) for l in open(f"{d}/samples.jsonl")]
ev = sorted((json.loads(l) for l in open(f"{d}/watch/workloads.jsonl")), key=lambda r: ep(r["t"]))
resync = [r for r in ev if r["type"] == "RESYNC"]
W, k, idle, prev_t, windows, cur = {}, 0, 0.0, None, [], None
partial, seen = [], set()
for r in ev:
    if r["type"] == "RESYNC": continue
    g, s = r["granted"].get("executor"), r["spec"].get("executor")
    if r.get("replaces") and r["cond"].get("QuotaReserved", {}).get("s") == "True" and g is not None and g < s and r["name"] not in seen:
        seen.add(r["name"]); partial.append((r["t"][11:19], r["owner"], s, g))
for smp in S:
    t = ep(smp["t"])
    while k < len(ev) and ep(ev[k]["t"]) <= t:
        r = ev[k]; k += 1
        if r["type"] == "RESYNC": W.clear(); continue
        if r["type"] == "DELETED": W.pop(r["name"], None)
        else: W[r["name"]] = r
    cpu = [float(v) for kk, v in smp["usage"].items() if kk.endswith("cpu")]
    used = cpu[0] if cpu else 0.0
    pend = [w for w in W.values() if w.get("replaces") and w["cond"].get("Finished", {}).get("s") != "True"
            and w["cond"].get("QuotaReserved", {}).get("s") != "True"]
    waste = used < SLOTS and pend
    if prev_t is not None and waste:
        idle += t - prev_t
    if waste and cur is None: cur = [smp["t"][11:19], None, SLOTS - used, sorted((w["owner"], w["spec"].get("executor")) for w in pend)]
    if not waste and cur is not None: cur[1] = smp["t"][11:19]; windows.append(cur); cur = None
    prev_t = t
if cur: cur[1] = "end"; windows.append(cur)
print(f"{d.split('/')[-1]}: free-capacity-while-scale-up-pending = {idle:.0f}s in {len(windows)} window(s)")
for w in windows: print(f"   {w[0]}..{w[1]} free={w[2]:g} slots pending={w[3]}")
print(f"   partial admissions: {len(partial)}")
for p in partial: print(f"   {p[0]} {p[1]} requested={p[2]} granted={p[3]}")
