#!/usr/bin/env python3
"""Generate SVG sequence diagrams for Kueue's inference of Spark Dynamic Allocation scaling."""

import html
import subprocess
from pathlib import Path

LANE_W = 268
MARGIN = 34
HEAD_TOP = 96
HEAD_H = 68
STEP = 50
FIRST_Y = HEAD_TOP + HEAD_H + 52

INK = "#1f2933"
MUTED = "#6b7785"
LIFELINE = "#c3cbd5"
CALL = "#25506e"
EVENT = "#9c5a1c"
SELF = "#3f6d52"
ACCENT_BG = "#fdf6e3"
ACCENT_BD = "#d9bc7a"
HEAD_BG = "#eef2f6"
HEAD_BD = "#b9c3cf"


def wrap(text, width):
    words, lines, cur = [], [], ""
    for w in text.split():
        while len(w) > width:  # hard-split tokens with no spaces (file paths)
            words.append(w[:width])
            w = w[width:]
        words.append(w)
    for w in words:
        cand = f"{cur} {w}".strip()
        if len(cand) > width and cur:
            lines.append(cur)
            cur = w
        else:
            cur = cand
    if cur:
        lines.append(cur)
    return lines


SELF_WRAP = 46


def self_extent(text, ref):
    lines = wrap(text, SELF_WRAP)
    longest = max([len(l) for l in lines] + [len(ref or "")])
    return lines, 6.3 * longest


def esc(s):
    return html.escape(s, quote=False)


def build(title, subtitle, lanes, msgs, out):
    n = len(lanes)
    width = MARGIN * 2 + LANE_W * n
    centers = [MARGIN + LANE_W // 2 + i * LANE_W for i in range(n)]

    rows, y = [], FIRST_Y
    for m in msgs:
        rows.append((m, y))
        extra = 0
        if m["kind"] == "note":
            extra = 22 + 15 * (len(wrap(m["text"], 116)) - 1)
        elif m["kind"] == "self":
            extra = 20 + 14 * (len(wrap(m["text"], SELF_WRAP)) - 1) + (12 if m.get("ref") else 0)
        else:
            extra = 15 * (len(wrap(m["text"], 42)) - 1)
        y += STEP + extra
    height = y + 40

    p = []
    p.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" font-family="Helvetica Neue, Helvetica, Arial, sans-serif">'
    )
    p.append(
        '<defs>'
        f'<marker id="ac" viewBox="0 0 10 8" refX="9" refY="4" markerWidth="9" markerHeight="7" orient="auto">'
        f'<path d="M0 0 L10 4 L0 8 z" fill="{CALL}"/></marker>'
        f'<marker id="ae" viewBox="0 0 10 8" refX="9" refY="4" markerWidth="9" markerHeight="7" orient="auto">'
        f'<path d="M0 0 L10 4 L0 8 z" fill="{EVENT}"/></marker>'
        f'<marker id="as" viewBox="0 0 10 8" refX="9" refY="4" markerWidth="8" markerHeight="6" orient="auto">'
        f'<path d="M0 0 L10 4 L0 8 z" fill="{SELF}"/></marker>'
        '</defs>'
    )
    p.append(f'<rect width="{width}" height="{height}" fill="#ffffff"/>')

    p.append(
        f'<text x="{MARGIN}" y="38" font-size="21" font-weight="600" fill="{INK}">{esc(title)}</text>'
    )
    p.append(f'<text x="{MARGIN}" y="60" font-size="12.5" fill="{MUTED}">{esc(subtitle)}</text>')

    lx = width - MARGIN - 356
    p.append(f'<g font-size="10.5" fill="{MUTED}">')
    for i, (col, mk, lab, dash) in enumerate(
        [(CALL, "ac", "call / API write", ""),
         (EVENT, "ae", "watch event", ' stroke-dasharray="5 3"'),
         (SELF, "as", "internal computation", "")]
    ):
        yy = 30 + i * 15
        p.append(
            f'<line x1="{lx}" y1="{yy}" x2="{lx + 26}" y2="{yy}" stroke="{col}" stroke-width="1.6"'
            f' marker-end="url(#{mk})"{dash}/>'
            f'<text x="{lx + 33}" y="{yy + 3.5}">{esc(lab)}</text>'
        )
    p.append('</g>')

    for i, (name, file) in enumerate(lanes):
        cx = centers[i]
        x = cx - LANE_W // 2 + 8
        w = LANE_W - 16
        p.append(
            f'<rect x="{x}" y="{HEAD_TOP}" width="{w}" height="{HEAD_H}" rx="5" '
            f'fill="{HEAD_BG}" stroke="{HEAD_BD}"/>'
        )
        nl = wrap(name, 26)
        ty = HEAD_TOP + 18 if len(nl) > 1 else HEAD_TOP + 22
        for ln in nl:
            p.append(
                f'<text x="{cx}" y="{ty}" font-size="12.5" font-weight="600" '
                f'text-anchor="middle" fill="{INK}">{esc(ln)}</text>'
            )
            ty += 14
        ty += 2
        for ln in wrap(file, 32):
            p.append(
                f'<text x="{cx}" y="{ty}" font-size="9.5" font-family="Menlo, monospace" '
                f'text-anchor="middle" fill="{MUTED}">{esc(ln)}</text>'
            )
            ty += 11
        p.append(
            f'<line x1="{cx}" y1="{HEAD_TOP + HEAD_H}" x2="{cx}" y2="{height - 24}" '
            f'stroke="{LIFELINE}" stroke-width="1.2" stroke-dasharray="4 5"/>'
        )

    for m, y in rows:
        kind = m["kind"]
        if kind == "note":
            lines = wrap(m["text"], 116)
            h = 20 + 15 * len(lines)
            x0 = MARGIN + 10
            p.append(
                f'<rect x="{x0}" y="{y - 14}" width="{width - 2 * MARGIN - 20}" height="{h}" rx="4" '
                f'fill="{ACCENT_BG}" stroke="{ACCENT_BD}"/>'
            )
            ty = y + 3
            for ln in lines:
                p.append(
                    f'<text x="{x0 + 12}" y="{ty}" font-size="11" fill="#7a5a12">{esc(ln)}</text>'
                )
                ty += 15
            continue

        a = centers[m["frm"]]
        num = m["n"]
        if kind == "self":
            lines, est = self_extent(m["text"], m.get("ref"))
            mirror = a + 66 + 18 + est > width - MARGIN
            sd = -1 if mirror else 1
            b = a + sd * 66
            p.append(
                f'<path d="M{a} {y} L{b} {y} L{b} {y + 22} L{a + sd * 5} {y + 22}" fill="none" '
                f'stroke="{SELF}" stroke-width="1.5" marker-end="url(#as)"/>'
            )
            tx = b + sd * 18
            anchor = "end" if mirror else "start"
            nrows = len(lines) + (1 if m.get("ref") else 0)
            ty = y + 15 - 7 * (nrows - 1)
            first = True
            for ln in lines:
                pre = (f'<tspan fill="{MUTED}" font-weight="600">{num}. </tspan>' if first else "")
                p.append(
                    f'<text x="{tx}" y="{ty}" font-size="11" text-anchor="{anchor}" '
                    f'fill="{INK}">{pre}{esc(ln)}</text>'
                )
                ty += 14
                first = False
            if m.get("ref"):
                p.append(
                    f'<text x="{tx}" y="{ty}" font-size="9.5" font-family="Menlo, monospace" '
                    f'text-anchor="{anchor}" fill="{SELF}">{esc(m["ref"])}</text>'
                )
            continue

        b = centers[m["to"]]
        col = CALL if kind == "call" else EVENT
        mk = "ac" if kind == "call" else "ae"
        dash = "" if kind == "call" else ' stroke-dasharray="6 4"'
        sgn = 1 if b > a else -1
        p.append(
            f'<line x1="{a + sgn * 3}" y1="{y}" x2="{b - sgn * 4}" y2="{y}" stroke="{col}" '
            f'stroke-width="1.7" marker-end="url(#{mk})"{dash}/>'
        )
        mid = (a + b) // 2
        lines = wrap(m["text"], 42)
        ty = y - 9 - 15 * (len(lines) - 1) - (12 if m.get("ref") else 0)
        for ln in lines:
            p.append(
                f'<text x="{mid}" y="{ty}" font-size="11" text-anchor="middle" fill="{INK}">'
                f'{esc(ln)}</text>'
            )
            ty += 15
        if m.get("ref"):
            p.append(
                f'<text x="{mid}" y="{ty}" font-size="9.5" font-family="Menlo, monospace" '
                f'text-anchor="middle" fill="{col}">{esc(m["ref"])}</text>'
            )
        p.append(
            f'<circle cx="{a + sgn * 13}" cy="{y - 11}" r="8.5" fill="#ffffff" stroke="{col}" '
            f'stroke-width="1.1"/>'
            f'<text x="{a + sgn * 13}" y="{y - 7.5}" font-size="9.5" font-weight="600" '
            f'text-anchor="middle" fill="{col}">{num}</text>'
        )

    p.append('</svg>')
    out.write_text("\n".join(p))


def seq(items):
    n, out = 1, []
    for it in items:
        if it["kind"] == "note":
            out.append(it)
        else:
            it["n"] = n
            n += 1
            out.append(it)
    return out


def call(f, t, text, ref=None):
    return {"kind": "call", "frm": f, "to": t, "text": text, "ref": ref}


def event(f, t, text, ref=None):
    return {"kind": "event", "frm": f, "to": t, "text": text, "ref": ref}


def self_(f, text, ref=None):
    return {"kind": "self", "frm": f, "text": text, "ref": ref}


def note(text):
    return {"kind": "note", "text": text}


UP_LANES = [
    ("Spark driver (DA)", "ExecutorAllocationManager"),
    ("kube-apiserver", "—"),
    ("executorPodHandler", "sparkapplication_executor_pod_handler.go"),
    ("JobReconciler", "jobframework/reconciler.go:962"),
    ("PodSets / count", "sparkapplication_podset.go"),
    ("workloadslicing", "workloadslicing/workloadslicing.go"),
    ("Scheduler + flavorassigner", "scheduler/scheduler.go"),
    ("scheduler cache", "cache/scheduler/clusterqueue.go"),
    ("elasticJobUngater", "controller/elasticjobs/elastic_job_ungater.go"),
]

UP = seq([
    note("Precondition: the executor pod template already carries kueue.x-k8s.io/elastic-job as a "
         "scheduling gate, injected once at CR create by sparkapplication_webhook.go Default(). "
         "Every Pod the driver creates from it is born gated."),
    call(0, 1, "create executor Pods (born gated)"),
    event(1, 2, "Pod CREATE event"),
    self_(2, "isTrackedExecutorPod() — label match on "
             "sparkoperator.k8s.io/app-name + spark-role", "pod_handler.go:70"),
    self_(2, "schedule() — trailing-edge debounce 5s, maxWait 30s", "pod_handler.go:135"),
    call(2, 3, "enqueue reconcile.Request for the SparkApplication"),
    call(3, 4, "PodSets(ctx, client)", "ensureOneWorkload -> controller.go:157"),
    call(4, 1, "List Pods by app-name + spark-role label"),
    self_(4, "isVerifiedLiveExecutor() — non-terminal Pods count, "
             "INCLUDING still-gated ones (defect 3)", "podset.go:119"),
    note("This is the whole inference. There is no DA event and no call from Spark into Kueue: the "
         "desired executor count is re-derived from live Pod objects on every debounced reconcile, "
         "and cached for the pass (podset.go:137)."),
    call(4, 3, "executor PodSet Count = N_live"),
    call(3, 5, "EnsureWorkloadSlices(podSets, ...)", "workloadslicing.go:173"),
    self_(5, "ScaledUp() -> a NEW slice, never an in-place grow", "workloadslicing.go:163"),
    call(5, 1, "create Workload slice + replacement-for annotation; "
               "name from GetWorkloadNameExtraPart (sequence number)", "controller.go:149"),
    event(1, 6, "pending Workload observed"),
    self_(6, "ReplacedWorkloadSlice / FindReplacedSliceTarget — "
             "predecessor becomes the preemption target", "scheduler.go:794, :469"),
    self_(6, "Assignment.append — charges the snapshot only the "
             "DELTA vs the replaced slice", "flavorassigner.go:967"),
    call(6, 1, "Assignment.ToAPI — FULL count written to status.admission",
         "flavorassigner.go:208"),
    call(6, 7, "AddOrUpdateWorkload", "cache.go:794"),
    self_(7, "sliceChainKey / reconcileSliceGroup — only the chain "
             "tip is charged (ns + slice name + job UID)", "clusterqueue.go"),
    call(6, 1, "replaceOldWorkloadSlice — Finish the predecessor", "scheduler.go:585"),
    event(1, 8, "Workload update event"),
    self_(8, "podsToUngate — room = granted - alreadyUngated", "ungater.go:185"),
    call(8, 1, "remove the scheduling gate from exactly `room` Pods; "
               "kube-scheduler then places them"),
    call(7, 1, "Cache.Usage -> ClusterQueue.status.flavorsUsage",
         "clusterqueue_controller.go:534"),
])

DOWN_LANES = [
    ("Spark driver (DA)", "ExecutorAllocationManager"),
    ("kube-apiserver", "—"),
    ("executorPodHandler", "sparkapplication_executor_pod_handler.go"),
    ("JobReconciler", "jobframework/reconciler.go:962"),
    ("PodSets / count", "sparkapplication_podset.go"),
    ("workloadslicing", "workloadslicing/workloadslicing.go"),
    ("Workload webhook", "webhooks/workload_webhook.go"),
    ("scheduler cache", "cache/scheduler + workload/workload.go"),
]

DOWN = seq([
    call(0, 1, "delete executor Pods (executorIdleTimeout elapsed)"),
    event(1, 2, "Pod DELETE / UPDATE event"),
    call(2, 3, "debounced enqueue — same handler as scale-up"),
    call(3, 4, "PodSets(ctx, client)"),
    self_(4, "isVerifiedLiveExecutor — a Pod with DeletionTimestamp "
             "still counts until Succeeded/Failed", "podset.go:119"),
    call(4, 3, "executor PodSet Count = N_live (lower)"),
    call(3, 5, "EnsureWorkloadSlices(podSets, ...)", "workloadslicing.go:173"),
    self_(5, "ScaledDown() -> in-place patch. No new slice, and the "
             "scheduler is never involved", "workloadslicing.go:156"),
    call(5, 1, "updatePodSetCountsWithRetry — lower spec.podSets[].count",
         "workloadslicing.go:325"),
    call(5, 1, "scaleDownAdmission — lower the granted count, rescale "
               "ResourceUsage, truncate TopologyAssignment", "workloadslicing.go:282"),
    call(1, 6, "admission mutation must pass validation"),
    self_(6, "validateAdmissionUpdate — decrease-only, elastic-only "
             "exception; batch/v1 Job admission stays immutable", "workload_webhook.go:376"),
    event(1, 7, "Workload update"),
    self_(7, "totalRequestsFromAdmission — charges min(spec.count, granted)",
          "workload.go:674"),
    call(7, 1, "flavorsUsage drops"),
    note("Asymmetry worth remembering: scale-up creates a Workload and traverses the full "
         "scheduler + preemption path; scale-down is two in-place patches and never reaches the "
         "scheduler. Both are driven by the same debounced Pod watch."),
])

d = Path.home() / "kueue-diagrams"
d.mkdir(exist_ok=True)

for name, title, sub, lanes, msgs in [
    ("kueue-da-upscale", "Kueue: how a Spark Dynamic Allocation scale-UP is inferred",
     "Elastic SparkApplication + ElasticJobsViaWorkloadSlices — Pradeep39/kueue", UP_LANES, UP),
    ("kueue-da-downscale", "Kueue: how a Spark Dynamic Allocation scale-DOWN is inferred",
     "Elastic SparkApplication + ElasticJobsViaWorkloadSlices — Pradeep39/kueue", DOWN_LANES, DOWN),
]:
    svg = d / f"{name}.svg"
    build(title, sub, lanes, msgs, svg)
    subprocess.run(["rsvg-convert", "-w", "2400", "-o", str(d / f"{name}.png"), str(svg)],
                   check=True)
    print(f"{svg}  ->  {d / (name + '.png')}")
