#!/usr/bin/env bash
# Check the cluster for everything demo-short.sh needs, and set up whatever is missing.
#
#   ensure-demo-env.sh kubeflow|apache [--check]
#     --check   report only, change nothing
#     YES=1     do not ask before restarting Kueue or the Apache Spark Operator
#
# What it checks, and what it does about a gap (🔧 = fixed here, ❌ = left for you):
#   Kueue feature gates      🔧 adds the missing ones to --feature-gates (Kueue rolls out)
#   Kueue configuration      🔧 adds the Spark framework and labelKeysToCopy [preemption-pool,
#                               preemption-role] to kueue-manager-config, then restarts Kueue
#   PreemptionConfig CRD     🔧 applies it from the Kueue checkout (KUEUE_REPO, default ~/kueue-preempt)
#   Queues and priorities    🔧 creates the missing objects from queues-setup.yaml, queues-quatro.yaml
#                               and queues-cinco.yaml (and queues-apache.yaml for the Apache kit);
#                               puts cluster-queue-tres back to 36 slots; restores cluster-queue-quatro's
#                               preemption settings
#   Apache operator          🔧 adds batch-ns-dos to spark.kubernetes.operator.watchedNamespaces and
#                               restarts the operator Pod (its Deployment cannot be patched while Kueue's
#                               priority-class webhook rejects updates to Kueue-managed Deployments)
#   Namespaces, service accounts, the Kubeflow operator's --namespaces, Spark CRDs   ❌ check only
#
# Restarting Kueue does not evict admitted workloads, but its webhooks are unavailable for a few
# seconds, so it asks first (YES=1 skips the question; without a terminal it needs YES=1).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# setup_file NAME: the setup YAML NAME, in this script's directory or the nearest parent that has it
# (directly, or in its setup/ directory), so it is found in the ~/kueue-gated-repro and the repo layouts.
setup_file() { local d=$HERE n c; for n in 0 1 2 3 4; do for c in "$d/$1" "$d/setup/$1"; do [ -f "$c" ] && { echo "$c"; return; }; done; d=$(dirname "$d"); done; echo "$HERE/$1"; }

KIT=${1:-}; MODE=fix
[ "${2:-}" = --check ] && MODE=check
case "$KIT" in kubeflow|apache) ;; *) sed -n 2,23p "$0" | sed 's/^# \{0,1\}//'; exit 2 ;; esac

KNS=kueue-system; KDEP=kueue-controller-manager; KCM=kueue-manager-config
GATES="ElasticJobsViaWorkloadSlicesWithPartialReplicaScaleUp SparkPartialPreemption ConfigurablePreemptions"
LABEL_KEYS="preemption-pool preemption-role"
if [ "$KIT" = kubeflow ]; then
  GATES="SparkApplicationIntegration $GATES"; FRAMEWORK=sparkoperator.k8s.io/sparkapplication
  NS=batch-ns-uno; SETUP="queues-setup.yaml queues-quatro.yaml queues-cinco.yaml"
else
  GATES="ApacheSparkApplicationIntegration $GATES"; FRAMEWORK=spark.apache.org/sparkapplication
  NS=batch-ns-dos; SETUP="queues-setup.yaml queues-quatro.yaml queues-cinco.yaml queues-apache.yaml"
fi

ok=1; fixed=0
pass()  { echo "  ✅ $*"; }
warn()  { echo "  ⚠️  $*"; }
fail()  { echo "  ❌ $*"; ok=0; }
fixd()  { echo "  🔧 $*"; fixed=1; }
gap()   { if [ $MODE = check ]; then fail "$*"; else echo "  … $*"; fi; }
confirm() { # confirm WHAT
  [ "${YES:-0}" = 1 ] && return 0
  if [ ! -t 0 ]; then fail "$1: needs confirmation, rerun with YES=1"; return 1; fi
  printf '  ❓ %s? [y/N] ' "$1"; local a; read -r a; [ "$a" = y ] || [ "$a" = Y ] || { fail "$1: declined"; return 1; }
}

# preemptionconfig_crd: the CRD manifest from the Kueue checkout this script sits in (repo layout), or
# from KUEUE_REPO (default ~/kueue-preempt).
preemptionconfig_crd() {
  local d=$HERE rel=config/components/crd/alpha/bases/kueue.x-k8s.io_preemptionconfigs.yaml
  for _ in 1 2 3 4 5 6; do [ -f "$d/$rel" ] && { echo "$d/$rel"; return; }; d=$(dirname "$d"); done
  [ -f "${KUEUE_REPO:-$HOME/kueue-preempt}/$rel" ] && echo "${KUEUE_REPO:-$HOME/kueue-preempt}/$rel"
}

# ---------------------------------------------------------------- Kueue
echo "  Kueue image: $(kubectl -n $KNS get deploy $KDEP -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || echo 'not found')"
kubectl -n $KNS get deploy $KDEP >/dev/null 2>&1 || { fail "Kueue is not installed ($KNS/$KDEP)"; exit 1; }

# PreemptionConfig CRD first: Kueue watches it once ConfigurablePreemptions is on.
if kubectl get crd preemptionconfigs.kueue.x-k8s.io >/dev/null 2>&1; then pass "PreemptionConfig CRD installed"
else
  gap "PreemptionConfig CRD is not installed"
  if [ $MODE = fix ]; then
    crd=$(preemptionconfig_crd)
    if [ -z "$crd" ]; then fail "PreemptionConfig CRD: no Kueue checkout found (set KUEUE_REPO)"
    elif kubectl apply --server-side -f "$crd" >/dev/null; then fixd "PreemptionConfig CRD applied from the Kueue checkout (make sure it matches the deployed build)"
    else fail "PreemptionConfig CRD: apply failed"; fi
  fi
fi

# Feature gates: the plan is a JSON patch of the container args, or empty when nothing is missing.
args_json=$(kubectl -n $KNS get deploy $KDEP -o jsonpath='{.spec.template.spec.containers[0].args}')
gate_plan=$(python3 -I -c '
import json, sys
args, want = json.loads(sys.argv[1] or "[]"), sys.argv[2].split()
i = next((n for n, a in enumerate(args) if a.startswith("--feature-gates=")), None)
gates = dict(g.split("=", 1) for g in args[i].split("=", 1)[1].split(",") if "=" in g) if i is not None else {}
missing = [g for g in want if gates.get(g, "").lower() != "true"]
print(" ".join(missing))
if missing:
    for g in missing:
        gates[g] = "true"
    new = "--feature-gates=" + ",".join(f"{k}={v}" for k, v in gates.items())
    new_args = list(args)
    if i is None: new_args.append(new)
    else: new_args[i] = new
    path = "/spec/template/spec/containers/0/args"
    print(json.dumps([{"op": "test", "path": path, "value": args}, {"op": "replace", "path": path, "value": new_args}]))
' "$args_json" "$GATES")
missing_gates=$(head -1 <<<"$gate_plan"); gate_patch=$(sed -n 2p <<<"$gate_plan")
if [ -z "$missing_gates" ]; then pass "feature gates on: $GATES"; else gap "feature gates off: $missing_gates"; fi

# Configuration: the new file, or empty when nothing is missing.
cfg=$(kubectl -n $KNS get cm $KCM -o jsonpath='{.data.controller_manager_config\.yaml}')
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
printf '%s\n' "$cfg" > "$tmp/cfg.yaml"
cfg_missing=$(python3 -I - "$tmp/cfg.yaml" "$tmp/new.yaml" "$FRAMEWORK" "$LABEL_KEYS" <<'PY'
import re, sys, yaml
src, dst, framework, keys = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4].split()
text = open(src).read()
integ = (yaml.safe_load(text) or {}).get("integrations") or {}
missing, lines = [], text.split("\n")
# Edit the text rather than re-serialising, so the file's comments and layout survive.
def insert_after(pattern, new_lines):
    for n, l in enumerate(lines):
        if re.match(pattern, l):
            lines[n + 1:n + 1] = new_lines
            return True
    return False
if framework not in (integ.get("frameworks") or []):
    missing.append(f"integrations.frameworks {framework}")
    if not insert_after(r"^  frameworks:\s*$", [f'  - "{framework}"']):
        insert_after(r"^integrations:\s*$", ["  frameworks:", f'  - "{framework}"'])
lack = [k for k in keys if k not in (integ.get("labelKeysToCopy") or [])]
if lack:
    missing.append("integrations.labelKeysToCopy " + " ".join(lack))
    if not insert_after(r"^  labelKeysToCopy:\s*$", [f"  - {k}" for k in lack]):
        if not insert_after(r"^integrations:\s*$", ["  labelKeysToCopy:"] + [f"  - {k}" for k in lack]):
            lines += ["integrations:", "  labelKeysToCopy:"] + [f"  - {k}" for k in lack]
new = "\n".join(lines)
yaml.safe_load(new)  # still valid YAML
open(dst, "w").write(new)
print("; ".join(missing))
PY
) || { fail "could not read or edit $KNS/$KCM"; cfg_missing=""; }
if [ -z "$cfg_missing" ]; then pass "Kueue configuration: $FRAMEWORK integration, labelKeysToCopy [$LABEL_KEYS]"; else gap "Kueue configuration lacks $cfg_missing"; fi

if [ $MODE = fix ] && { [ -n "$missing_gates" ] || [ -n "$cfg_missing" ]; }; then
  active=$(kubectl get workloads -A --no-headers 2>/dev/null | wc -l | tr -d ' ')
  echo "     ($active Workloads exist in the cluster; a restart does not evict them)"
  if confirm "Update Kueue and restart $KNS/$KDEP"; then
    old_pod=$(kubectl -n $KNS get pods -l control-plane=controller-manager -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)
    if [ -n "$cfg_missing" ]; then
      kubectl -n $KNS get cm $KCM -o json > "$tmp/cm.orig.json"
      cp "$tmp/cm.orig.json" "${HERE}/runs/kueue-manager-config.before-preflight-$(date +%m%d-%H%M).json" 2>/dev/null
      kubectl -n $KNS create configmap $KCM --from-file=controller_manager_config.yaml="$tmp/new.yaml" --dry-run=client -o yaml |
        kubectl apply -f - >/dev/null && fixd "Kueue configuration: added $cfg_missing" || fail "could not update $KNS/$KCM"
    fi
    if [ -n "$missing_gates" ]; then
      kubectl -n $KNS patch deploy $KDEP --type=json -p "$gate_patch" >/dev/null && fixd "feature gates: turned on $missing_gates" ||
        fail "could not patch $KNS/$KDEP (the args changed meanwhile? rerun)"
    else
      kubectl -n $KNS rollout restart deploy $KDEP >/dev/null   # the ConfigMap is only read at start
    fi
    kubectl -n $KNS rollout status deploy $KDEP --timeout=300s >/dev/null || fail "Kueue did not roll out within 5 minutes"
    # Ready once the old Pod is gone and the webhooks answer.
    for _ in $(seq 1 60); do
      pods=$(kubectl -n $KNS get pods -l control-plane=controller-manager -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)
      [ "$pods" != "$old_pod" ] && [[ " $pods " != *" $old_pod "* ]] &&
        kubectl apply --dry-run=server -f "$(setup_file queues-setup.yaml)" >/dev/null 2>&1 && break
      sleep 3
    done
    fixd "Kueue restarted: $(kubectl -n $KNS get pods -l control-plane=controller-manager --no-headers 2>/dev/null | awk '{print $1" "$3}')"
  fi
fi
kubectl -n $KNS get pods --no-headers 2>/dev/null | awk '$3!="Running"{print "  ⚠️  "$1" is "$3}'

# ---------------------------------------------------------------- namespaces and queues
for ns in batch-ns-uno batch-ns-dos; do
  kubectl get ns $ns >/dev/null 2>&1 || fail "namespace $ns is missing (with its Spark service account and RBAC)"
done
for y in $SETUP; do
  f=$(setup_file $y)
  [ -f "$f" ] || { fail "setup file $y not found (looked beside the scripts and in ../setup)"; continue; }
  python3 -I - "$f" "$MODE" <<'PY' || ok=0
import json, subprocess, sys, yaml
path, mode = sys.argv[1], sys.argv[2]
missing, bad = [], False
for d in yaml.safe_load_all(open(path)):
    if not d:
        continue
    group = d["apiVersion"].split("/")[0] if "/" in d["apiVersion"] else ""
    kind = d["kind"].lower() + ("." + group if group else "")
    md = d["metadata"]
    ns = ["-n", md["namespace"]] if md.get("namespace") else []
    if subprocess.run(["kubectl", "get", kind, md["name"], *ns], capture_output=True).returncode:
        missing.append((d, f'{d["kind"]} {md.get("namespace", "") + "/" if md.get("namespace") else ""}{md["name"]}'))
name = path.rsplit("/", 1)[-1]
if not missing:
    print(f"  ✅ {name}: every object exists")
for d, what in missing:
    if mode == "check":
        print(f"  ❌ {what} missing ({name})")
        bad = True
        continue
    r = subprocess.run(["kubectl", "create", "--save-config", "-f", "-"], input=json.dumps(d), text=True, capture_output=True)
    print(f"  🔧 created {what} ({name})" if r.returncode == 0 else f"  ❌ could not create {what}: {r.stderr.strip()}")
    bad = bad or r.returncode != 0
sys.exit(1 if bad else 0)
PY
done

# Drift the runners leave behind if they are interrupted.
locks=$(ls -d "$HERE"/runs/.*.lock "$HERE"/../scripts/runs/.*.lock "$HERE"/demo/apache/runs/.*.lock "$HERE"/../demo/apache/runs/.*.lock 2>/dev/null | head -1)
q=$(kubectl get clusterqueue cluster-queue-tres -o jsonpath='{.spec.resourceGroups[0].flavors[0].resources[0].nominalQuota}' 2>/dev/null)
if [ -n "$q" ] && [ "$q" != 36 ]; then
  if [ $MODE = check ] || [ -n "$locks" ]; then warn "cluster-queue-tres is at $q slots, not 36${locks:+ (a run holds $locks)}"
  else
    kubectl patch clusterqueue cluster-queue-tres --type=json -p '[
      {"op":"replace","path":"/spec/resourceGroups/0/flavors/0/resources/0/nominalQuota","value":"36"},
      {"op":"replace","path":"/spec/resourceGroups/0/flavors/0/resources/1/nominalQuota","value":"32256Mi"}]' >/dev/null &&
      fixd "cluster-queue-tres back to 36 slots (was $q)"
  fi
fi
quatro=$(kubectl get clusterqueue cluster-queue-quatro -o jsonpath='{.spec.preemption.withinClusterQueue}/{.spec.preemption.reclaimWithinCohort}/{.metadata.annotations.kueue\.x-k8s\.io/preemption-config-name}' 2>/dev/null)
if [ -n "$quatro" ]; then
  if [ "$quatro" = "Never/Never/spark-batch-pool" ]; then pass "cluster-queue-quatro: classic preemption off, PreemptionConfig spark-batch-pool"
  elif [ $MODE = check ]; then fail "cluster-queue-quatro is not set up for part 5 (withinClusterQueue/reclaimWithinCohort/config = $quatro)"
  else kubectl apply -f "$(setup_file queues-quatro.yaml)" 2>&1 >/dev/null | grep -v '^Warning:' >&2; [ "${PIPESTATUS[0]}" = 0 ] && fixd "cluster-queue-quatro: restored classic preemption Never and PreemptionConfig spark-batch-pool"; fi
fi

# ---------------------------------------------------------------- Spark operator
if [ "$KIT" = kubeflow ]; then
  kubectl get crd sparkapplications.sparkoperator.k8s.io >/dev/null 2>&1 && pass "Kubeflow SparkApplication CRD installed" ||
    fail "the sparkoperator.k8s.io SparkApplication CRD is not installed"
  nss=$(kubectl get deploy -A -o json 2>/dev/null | python3 -I -c '
import json, sys
for d in json.load(sys.stdin)["items"]:
    for c in d["spec"]["template"]["spec"]["containers"]:
        for a in c.get("args") or []:
            if a.startswith("--namespaces=") and "spark-operator" in d["metadata"]["name"]:
                print(a.split("=", 1)[1])')
  if [[ ",$nss," == *",$NS,"* ]]; then pass "Kubeflow Spark Operator watches $NS"
  else fail "the Kubeflow Spark Operator does not watch $NS (--namespaces=$nss): add it to the Helm value spark.jobNamespaces"; fi
  kubectl -n $NS get sa spark-operator-spark >/dev/null 2>&1 && pass "service account $NS/spark-operator-spark" ||
    fail "service account $NS/spark-operator-spark missing (the drivers run as it; the operator's Helm chart creates it per job namespace)"
else
  kubectl get crd sparkapplications.spark.apache.org >/dev/null 2>&1 && pass "Apache SparkApplication CRD installed" ||
    fail "the spark.apache.org SparkApplication CRD is not installed"
  kubectl -n $NS get sa spark >/dev/null 2>&1 && pass "service account $NS/spark" ||
    fail "service account $NS/spark missing (the apps run their driver as it, and it needs RBAC to create Pods)"
  ONS=default; ODEP=spark-kubernetes-operator; OCM=spark-kubernetes-operator-configuration; KEY=spark-operator.properties
  if ! kubectl -n $ONS get deploy $ODEP >/dev/null 2>&1; then fail "the Apache Spark Operator ($ONS/$ODEP) is not installed"
  else
    props=$(kubectl -n $ONS get cm $OCM -o json 2>/dev/null | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["data"].get(sys.argv[1], ""))' "$KEY")
    watched=$(sed -n 's/^spark\.kubernetes\.operator\.watchedNamespaces=//p' <<<"$props" | tail -1 | tr -d ' ')
    if [ -z "$watched" ]; then warn "spark.kubernetes.operator.watchedNamespaces is not set: assuming the operator watches every namespace"
    elif [[ ",$watched," == *",$NS,"* ]]; then pass "Apache Spark Operator watches $watched"
    else
      gap "Apache Spark Operator watches only $watched, not $NS"
      if [ $MODE = fix ] && confirm "Add $NS to the Apache Spark Operator's watchedNamespaces and restart it"; then
        kubectl -n $ONS get cm $OCM -o json > "$tmp/ocm.json"
        cp "$tmp/ocm.json" "$HERE/runs/spark-kubernetes-operator-configuration.before-preflight-$(date +%m%d-%H%M).json" 2>/dev/null
        python3 -I - "$tmp/ocm.json" "$KEY" "$NS" > "$tmp/ocm.new.json" <<'PY'
import json, re, sys
path, key, ns = sys.argv[1:4]
cm = json.load(open(path))
cm["data"][key] = re.sub(r"(?m)^(spark\.kubernetes\.operator\.watchedNamespaces=)(.*)$",
                         lambda m: m.group(1) + m.group(2).strip() + "," + ns, cm["data"][key])
for k in ("resourceVersion", "uid", "creationTimestamp", "managedFields"):
    cm["metadata"].pop(k, None)
print(json.dumps(cm))
PY
        kubectl apply -f "$tmp/ocm.new.json" >/dev/null || fail "could not update $ONS/$OCM"
        sel=$(kubectl -n $ONS get deploy $ODEP -o json | python3 -I -c 'import json,sys; print(",".join(f"{k}={v}" for k,v in json.load(sys.stdin)["spec"]["selector"]["matchLabels"].items()))')
        kubectl -n $ONS delete pod -l "$sel" --wait=true >/dev/null
        kubectl -n $ONS rollout status deploy $ODEP --timeout=180s >/dev/null
        pod=$(kubectl -n $ONS get pods -l "$sel" --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
        for _ in $(seq 1 30); do kubectl -n $ONS logs "$pod" 2>/dev/null | grep -q "watched namespaces \[.*$NS" && break; sleep 3; done
        if kubectl -n $ONS logs "$pod" 2>/dev/null | grep -q "watched namespaces \[.*$NS"; then
          fixd "Apache Spark Operator restarted ($pod), now watching $watched,$NS"
        else fail "the Apache Spark Operator restarted ($pod) but its log does not show it watching $NS yet"; fi
      fi
    fi
  fi
fi

echo
if [ $ok = 1 ]; then echo "  Ready.$([ $fixed = 1 ] && echo ' (🔧 = set up by this preflight)')"
elif [ $MODE = check ]; then echo "  Not ready. ./demo-short.sh preflight (without --check) sets up everything marked 🔧 in its --help."
else echo "  Fix the ❌ items before starting."; fi
[ $ok = 1 ]
