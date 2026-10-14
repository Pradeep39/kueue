#!/usr/bin/env python3
"""Generate SparkApplication manifests for the edge-case matrix.

  gen-apps.py NAME:PRIORITY[,NAME:PRIORITY...] [--partial] [--cap N] [--idle 5s] [--backlog 2s]

PRIORITY is a WorkloadPriorityClass name (low/default/high) or "-" for none. Same SparkALS
workload as apps.yaml, so runs stay comparable.
"""
import argparse
import json

TEMPLATE = '''apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: {name}
  namespace: batch-ns-uno
  annotations:
    kueue.x-k8s.io/elastic-job: "true"{partial}
  labels:
    kueue.x-k8s.io/queue-name: {queue}{priority}
spec:
  type: Scala
  mode: cluster
  image: docker.io/library/spark:4.0.1
  imagePullPolicy: IfNotPresent
  mainClass: {main_class}
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples.jar
  arguments: {als_args}
  sparkVersion: 4.0.1
  sparkConf:
    "spark.kubernetes.executor.podNamePrefix": "{name}"
    "spark.dynamicAllocation.enabled": "true"
    "spark.dynamicAllocation.shuffleTracking.enabled": "true"
    "spark.dynamicAllocation.minExecutors": "3"
    "spark.dynamicAllocation.maxExecutors": "30"
    "spark.dynamicAllocation.schedulerBacklogTimeout": "{backlog}"
    "spark.dynamicAllocation.executorIdleTimeout": "{idle}"{cap}
  driver:
    cores: 1
    memory: 512m
    serviceAccount: spark-operator-spark
    securityContext: &sc
      capabilities:
        drop: [ALL]
      runAsGroup: 185
      runAsUser: 185
      runAsNonRoot: true
      allowPrivilegeEscalation: false
      seccompProfile:
        type: RuntimeDefault
  executor:
    cores: 1
    memory: 512m
    securityContext: *sc
'''

ap = argparse.ArgumentParser()
ap.add_argument("apps")
ap.add_argument("--partial", action="store_true")
ap.add_argument("--cap", type=int)
ap.add_argument("--idle", default="5s")
ap.add_argument("--als-args", default="1500 1500 12 15 200", help="the example's arguments (SparkALS: M U F iterations slices)")
ap.add_argument("--main", default="org.apache.spark.examples.SparkALS", help="example main class, e.g. org.apache.spark.examples.GroupByTest")
ap.add_argument("--backlog", default="2s")
ap.add_argument("--queue", default="batch-queue-uno", help="LocalQueue name")
ap.add_argument("--static", type=int, metavar="N", help="no Dynamic Allocation: spark.executor.instances N, not elastic")
ap.add_argument("--static-elastic", type=int, metavar="N", help="no Dynamic Allocation: spark.executor.instances N, but keep the elastic opt-in")
ap.add_argument("--floor", type=int, metavar="N", help="spark.kueue.preemption.minExecutors N (partial-preemption floor)")
ap.add_argument("--no-decom", action="store_true", help="explicitly disable Spark decommissioning")
a = ap.parse_args()

docs = []
for spec in a.apps.split(","):
    name, prio = spec.split(":")
    docs.append(TEMPLATE.format(
        name=name, queue=a.queue, idle=a.idle, backlog=a.backlog, als_args=json.dumps(a.als_args.split()), main_class=a.main,
        partial='\n    kueue.x-k8s.io/elastic-job-scale-up-strategy: "partial"' if a.partial else "",
        priority=f"\n    kueue.x-k8s.io/priority-class: {prio}" if prio != "-" else "",
        cap=f'\n    "spark.kubernetes.allocation.maxPendingPods": "{a.cap}"' if a.cap else "",
    ))
out = "---\n".join(docs)
extra = []
if a.floor is not None:
    extra.append(f'"spark.kueue.preemption.minExecutors": "{a.floor}"')
if a.no_decom:
    extra.append('"spark.decommission.enabled": "false"')
if extra:
    out = out.replace('"spark.kubernetes.executor.podNamePrefix"', "\n    ".join(extra) + '\n    "spark.kubernetes.executor.podNamePrefix"')
if a.static_elastic:
    # A static application on the elastic path: drop Dynamic Allocation only, keep the opt-in.
    out = "\n".join(l for l in out.split("\n") if "spark.dynamicAllocation" not in l and "elastic-job-scale-up-strategy" not in l)
    out = out.replace('"spark.kubernetes.executor.podNamePrefix"',
                      f'"spark.executor.instances": "{a.static_elastic}"\n    "spark.kubernetes.executor.podNamePrefix"')
if a.static:
    # A static application: drop Dynamic Allocation and the elastic opt-in, declare a fixed count.
    out = "\n".join(l for l in out.split("\n")
                    if "spark.dynamicAllocation" not in l and "kueue.x-k8s.io/elastic-job" not in l)
    out = out.replace('"spark.kubernetes.executor.podNamePrefix"',
                      f'"spark.executor.instances": "{a.static}"\n    "spark.kubernetes.executor.podNamePrefix"')
    out = out.replace("  annotations:\n  labels:", "  labels:")
print(out, end="")
