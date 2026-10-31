#!/usr/bin/env python3
"""Generate Apache Spark Operator SparkApplications (spark.apache.org/v1) for the demo.

  gen-apache-apps.py NAME:PRIORITY[,NAME:PRIORITY...] [options] | kubectl apply -f -

The shape follows a hand-tested manifest: resources in sparkConf, Pod templates under
driverSpec/executorSpec.podTemplateSpec. Default workload: SparkALS with Dynamic Allocation
(minExecutors 3, maxExecutors 30) on the elastic path. SPARK_VERSION picks the image (default
4.0.1). PRIORITY is a WorkloadPriorityClass name (low/default/high) or "-" for none. Output is a
JSON List, which kubectl applies like multi-document YAML.
"""
import argparse
import copy
import json
import os

ap = argparse.ArgumentParser()
ap.add_argument("apps")
ap.add_argument("--namespace", default="batch-ns-dos")
ap.add_argument("--queue", default="batch-queue-dos", help="LocalQueue name")
ap.add_argument("--partial", action="store_true", help="partial scale-up (elastic-job-scale-up-strategy: partial)")
ap.add_argument("--cap", type=int, help="spark.kubernetes.allocation.maxPendingPods")
ap.add_argument("--idle", default="4s", help="spark.dynamicAllocation.executorIdleTimeout")
ap.add_argument("--backlog", default="2s", help="spark.dynamicAllocation.schedulerBacklogTimeout")
ap.add_argument("--main", default="org.apache.spark.examples.SparkALS")
ap.add_argument("--args", default="1500 1500 12 30 200", help="driverArgs (SparkALS: M U F iterations slices)")
ap.add_argument("--static", type=int, metavar="N", help="no Dynamic Allocation, spark.executor.instances N, not elastic")
ap.add_argument("--static-elastic", type=int, metavar="N", help="no Dynamic Allocation, spark.executor.instances N, elastic")
ap.add_argument("--floor", type=int, metavar="N", help="spark.kueue.preemption.minExecutors N")
ap.add_argument("--min-executors", type=int, default=3, help="spark.dynamicAllocation.minExecutors (default 3)")
ap.add_argument("--max-executors", type=int, default=30, help="spark.dynamicAllocation.maxExecutors (default 30)")
ap.add_argument("--no-decom", action="store_true", help="explicitly disable Spark decommissioning")
ap.add_argument("--pagerank-graph", action="store_true",
                help="SparkPageRank input: an init container writes a 2M-edge graph to /data/edges.txt in every Pod")
ap.add_argument("--conf", action="append", default=[], metavar="KEY=VALUE", help="extra sparkConf entry")
ap.add_argument("--label", action="append", default=[], metavar="KEY=VALUE", help="extra label on each SparkApplication")
a = ap.parse_args()

spark_version = os.environ.get("SPARK_VERSION", "4.0.1")
image = f"docker.io/library/spark:{spark_version}"
jar = "spark-examples.jar" if spark_version.startswith("4.") else f"spark-examples_2.12-{spark_version}.jar"

security = {
    "capabilities": {"drop": ["ALL"]},
    "runAsGroup": 185,
    "runAsUser": 185,
    "runAsNonRoot": True,
    "allowPrivilegeEscalation": False,
    "seccompProfile": {"type": "RuntimeDefault"},
}
GRAPH = 'awk "BEGIN{srand(42); for(i=0;i<2000000;i++) print int(rand()*200000), int(rand()*200000)}" > /data/edges.txt'


def template(container):
    spec = {"containers": [{"name": container, "securityContext": security}]}
    if a.pagerank_graph:
        # Every Pod generates the same graph (fixed seed), so the driver and each executor read an
        # identical local file without shared storage.
        spec["volumes"] = [{"name": "data", "emptyDir": {}}]
        spec["initContainers"] = [{
            "name": "gen-edges", "image": image, "command": ["sh", "-c", GRAPH],
            "volumeMounts": [{"name": "data", "mountPath": "/data"}], "securityContext": security,
        }]
        spec["containers"][0]["volumeMounts"] = [{"name": "data", "mountPath": "/data"}]
    return {"podTemplateSpec": {"metadata": {"labels": {"version": spark_version}}, "spec": spec}}


items = []
for entry in a.apps.split(","):
    name, prio = entry.split(":")
    conf = {
        "spark.kubernetes.container.image": image,
        "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
        "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
        "spark.kubernetes.executor.podNamePrefix": name,
        "spark.driver.cores": "1",
        "spark.kubernetes.driver.request.cores": "1000m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1",
        "spark.kubernetes.executor.request.cores": "1000m",
        "spark.executor.memory": "512m",
    }
    elastic = True
    if a.static is not None or a.static_elastic is not None:
        conf["spark.executor.instances"] = str(a.static if a.static is not None else a.static_elastic)
        elastic = a.static is None
    else:
        conf.update({
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.shuffleTracking.enabled": "true",
            "spark.dynamicAllocation.minExecutors": str(a.min_executors),
            "spark.dynamicAllocation.maxExecutors": str(a.max_executors),
            "spark.dynamicAllocation.schedulerBacklogTimeout": a.backlog,
            "spark.dynamicAllocation.executorIdleTimeout": a.idle,
        })
    if a.cap:
        conf["spark.kubernetes.allocation.maxPendingPods"] = str(a.cap)
    if a.floor is not None:
        conf["spark.kueue.preemption.minExecutors"] = str(a.floor)
    if a.no_decom:
        conf["spark.decommission.enabled"] = "false"
    if a.pagerank_graph:
        conf.update({
            "spark.sql.files.maxPartitionBytes": "2m",
            "spark.default.parallelism": "40",
            # PageRank never truncates its lineage; a deep RDD chain overflows the default stack.
            "spark.driver.extraJavaOptions": "-Xss64m",
            "spark.executor.extraJavaOptions": "-Xss64m",
        })
    for kv in a.conf:
        k, v = kv.split("=", 1)
        conf[k] = v

    annotations = {}
    if elastic:
        annotations["kueue.x-k8s.io/elastic-job"] = "true"
        if a.partial:
            annotations["kueue.x-k8s.io/elastic-job-scale-up-strategy"] = "partial"
    labels = dict(kv.split("=", 1) for kv in a.label)
    labels["kueue.x-k8s.io/queue-name"] = a.queue
    if prio != "-":
        labels["kueue.x-k8s.io/priority-class"] = prio
    meta = {"name": name, "namespace": a.namespace, "labels": labels}
    if annotations:
        meta["annotations"] = annotations
    items.append({
        "apiVersion": "spark.apache.org/v1",
        "kind": "SparkApplication",
        "metadata": meta,
        "spec": {
            "mainClass": a.main,
            "jars": f"local:///opt/spark/examples/jars/{jar}",
            "driverArgs": a.args.split(),
            "runtimeVersions": {"sparkVersion": spark_version},
            "sparkConf": conf,
            "driverSpec": template("spark-kubernetes-driver"),
            "executorSpec": copy.deepcopy(template("spark-kubernetes-executor")),
        },
    })

print(json.dumps({"apiVersion": "v1", "kind": "List", "items": items}, indent=2))
