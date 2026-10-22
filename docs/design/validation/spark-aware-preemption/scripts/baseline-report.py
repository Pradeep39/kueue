import json, sys, statistics as st, subprocess
from datetime import datetime, timezone
runs = sys.argv[1:]
hms = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%H:%M:%S")
f = lambda v, n=0: "-" if v is None else (f"{v:.{n}f}" if isinstance(v, float) else str(v))
L = []; agg = []
for d in runs:
    o = json.loads(subprocess.run([sys.executable, "-I", "/tmp/preempt-analysis/scripts/preempt.py", d], capture_output=True, text=True, check=True).stdout)
    rows = o["rows"]; agg.append(o)
    L.append(f"\n### {o['dir']}  (window {o['window']:.0f}s, watch starts {o['resync']})\n")
    L.append("| # | evicted at | victim | victim slots (drv+exec) / live exec | preemptor slice | initial? | preemptor req slots | delta slots (exec) | free | need | reclaim ratio | slice wait s | of which after eviction | victim quota release s | victim attempt age s | exec-s discarded | burst peak / len s | partial fits F=2 / F=4 | survives burst F=2 / F=4 |")
    L.append("|"+"---|"*19)
    for i, r in enumerate(rows, 1):
        L.append(f"| {i} | {hms(r['t'])} | {r['victim_app']} | {r['vsize']} (1+{r['vexec']}) / {r['live']} | {r['preemptor']}/{r['pslice']} | {'Y' if r['initial'] else ''} | {r['req']} | {r['delta']} ({r['dexec']}) | {f(r['free'])}{'*' if r['free_raw']<0 else ''} | {f(r['need'])} | {('inf' if r['ratio']==float('inf') else f(r['ratio'],1))} | {f(r['wait'])} | {f(r['teardown'])} | {f(r['release'])} | {f(r['age'])} | {f(r['xsec'])} | {r['bpeak']} / {f(r['blen'])} | {'Y' if r['fit2'] else 'n'} / {'Y' if r['fit4'] else 'n'} | {'Y' if r['surv2'] else 'n'} / {'Y' if r['surv4'] else 'n'} |")
    L.append("")
    L.append("| app | evictions | driver restarts | progress lost s (sum of attempt ages) | exec-s discarded | completion s (create->COMPLETED) |")
    L.append("|---|---|---|---|---|---|")
    for a, (c, stt) in sorted(o["completion"].items()):
        L.append(f"| {a} | {o['ev_by_app'].get(a,0)} | {o['restarts'].get(a,0)} | {f(o['age_by_app'].get(a,0.0))} | {f(o['xs_by_app'].get(a,0.0))} | {f(c) if c else 'not finished in window ('+str(stt)+')'} |")
    L.append("")
    L.append(f"preemptor peak {o['hi_peak']} slots; time at >=8/9/10/11/12 slots: {o['hi_time_at_ge']}  ")
    L.append(f"counterfactual B (strict priority, ideal partial reclaim to floor): F=2 {o['sim']['2']}, F=4 {o['sim']['4']}  ")
for o in agg:
    rows = o["rows"]
    o["_s"] = dict(n=len(rows), restarts=sum(v for a, v in o["restarts"].items() if "hi" not in a),
        xsec=sum(r["xsec"] for r in rows), age=sum(r["age"] for r in rows),
        ratio=st.median([r["ratio"] for r in rows if r["need"]]) if rows else None,
        need1=sum(1 for r in rows if r["need"] <= 1), wait=st.median([r["wait"] for r in rows if r["wait"] is not None]),
        waitmax=max([r["wait"] for r in rows if r["wait"] is not None]), td=st.median([r["teardown"] for r in rows if r["teardown"] is not None]),
        fit2=sum(r["fit2"] for r in rows), fit4=sum(r["fit4"] for r in rows), surv2=sum(r["surv2"] for r in rows), surv4=sum(r["surv4"] for r in rows),
        av_x=sum(r['xsec'] for r in rows if r['surv2']), av_age=sum(r['age'] for r in rows if r['surv2']), blen=st.median([r["blen"] for r in rows]), zero=sum(1 for r in rows if r["age"] == 0))
H = ["\n## Aggregate per run\n", "| run | evictions | low-app restarts | evictions with need<=1 slot | median reclaim ratio | median / max preemptor slice wait s | median wait after eviction s | victim progress lost s | exec-s discarded | victims evicted before running | partial fits F=2/F=4 | survives burst F=2/F=4 | median burst len s | exec-s on burst-survivable evictions (F=2) | B evictions F=2/F=4 | C hi denied slot-s F=2/F=4 | completion hi/lo1/lo2 s |", "|"+"---|"*17]
for o in agg:
    s = o["_s"]; c = o["completion"]
    comp = "/".join(f(c[a][0]) if c[a][0] else "DNF" for a in sorted(c, key=lambda a: ("hi" not in a, a)))
    H.append(f"| {o['dir']} | {s['n']} | {s['restarts']} | {s['need1']} | {f(s['ratio'],1)} | {f(s['wait'])} / {f(s['waitmax'])} | {f(s['td'])} | {f(s['age'])} | {f(s['xsec'])} | {s['zero']} | {s['fit2']}/{s['fit4']} | {s['surv2']}/{s['surv4']} | {f(s['blen'])} | {f(s['av_x'])} | {o['sim']['2']['evictions']}/{o['sim']['4']['evictions']} | {o['sim']['2']['hi_denied_slot_sec']}/{o['sim']['4']['hi_denied_slot_sec']} | {comp} |")
print("\n".join(H + L))
T = {k: sum(o['_s'][k] for o in agg) for k in ('n','restarts','xsec','age','need1','fit2','fit4','surv2','surv4','av_x','zero')}
T['B2'] = sum(o['sim']['2']['evictions'] for o in agg); T['B4'] = sum(o['sim']['4']['evictions'] for o in agg); T['C2'] = sum(o['sim']['2']['hi_denied_slot_sec'] for o in agg)
print('\nTOTALS', json.dumps(T))
