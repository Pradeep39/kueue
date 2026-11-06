# workload-slice-quota-e2e

End-to-end tests for [kubernetes-sigs/kueue#18396](https://github.com/kubernetes-sigs/kueue/issues/18396):
during an elastic scale-up, a workload slice and the slice it replaces must not both be charged to
the ClusterQueue. The fix is [kubernetes-sigs/kueue#18397](https://github.com/kubernetes-sigs/kueue/pull/18397).

The kit runs against any cluster with Kueue installed. It needs only `kubectl` and `python3` (standard
library), and creates its own namespaces, queues and flavor, all prefixed `wl-slice-quota`.

## Quick start

```sh
git clone --depth 1 -b workload-slice-quota-e2e https://github.com/Pradeep39/kueue.git workload-slice-quota-e2e
cd workload-slice-quota-e2e
./workload-slice-quota-e2e.sh preflight
./workload-slice-quota-e2e.sh all --trials 8
./workload-slice-quota-e2e.sh clean
```

## Commands

| Command | What it does |
|---|---|
| `preflight` | Checks `kubectl` access, the Kueue CRDs, that `ElasticJobsViaWorkloadSlices` is not turned off, that the `batch/job` integration is enabled, and permission to patch `workloads/status`. Creates nothing. |
| `edge` | Runs the cache edge cases. Deterministic. |
| `job [--trials N]` | Runs the elastic batch Job scale-up N times (default 3). |
| `job-shrink [--trials N]` | Shrinks a pending replacement of an elastic batch Job N times (default 3). |
| `all [--trials N]` | `preflight`, then `edge`, `job` and `job-shrink`. |
| `clean` | Deletes everything the kit created. |

Each test prints PASS or FAIL per step or trial and exits non-zero on failure. Logs go to `out/`.
Set `KUEUE_NAMESPACE` if Kueue is not installed in `kueue-system`.

## What each test creates

The tests apply their own manifests when they start; `preflight` does not create anything.

| Test | Resources | Capacity needed |
|---|---|---|
| `edge` (`manifests/edge-setup.yaml`) | namespaces `wl-slice-quota-edge` and `wl-slice-quota-edge-2`, ResourceFlavor `wl-slice-quota`, 10-cpu ClusterQueue `wl-slice-quota-edge`, two held LocalQueues `parked`, three ConfigMaps | none: no Pods run |
| `job`, `job-shrink` (`manifests/job-setup.yaml`, `manifests/job.yaml`) | namespace `wl-slice-quota-job`, ResourceFlavor `wl-slice-quota`, 400m ClusterQueue `wl-slice-quota-job`, LocalQueue, one Job | 400m CPU and 256Mi free; pulls `busybox:1.36` |

## The tests

### edge: cache edge cases (deterministic)

Builds the replacement cases directly with Workload objects, so each one is reproduced exactly:

- Each Workload is owned, as controller, by a ConfigMap that stands in for its job, and carries
  the `kueue.x-k8s.io/workload-slice-name` annotation that every slice of a chain has.
- It sits in a LocalQueue with `stopPolicy: Hold`, so the scheduler never admits it, and never
  finishes the "old" slice on its own.
- Its quota reservation is set through the status subresource.
- Before each reading the ClusterQueue is touched (an annotation), so Kueue rewrites its status from
  the cache. The test then compares the reserved CPU and `reservingWorkloads` with the expected values.

| Case | Checks that |
|---|---|
| basic replacement | B replacing A is charged once (3 cpu, not 5) |
| replacement leaves first | A is charged again when B leaves before A finishes (e.g. a failed admission) |
| chain of three | C replacing B replacing A is charged once, and B is charged again when C leaves |
| chain, middle slice finishes first | A stays uncharged while C is updated after B finished, and is charged again when C leaves |
| replacement that shrinks | a smaller B replacing A is covered by A, the larger slice (2 cpu), until A finishes, then B is charged (1 cpu) |
| self-reference | a Workload naming itself stays charged |
| another namespace | a Workload in another namespace named by the annotation stays charged |
| another job at the replaced slice's key | a Workload of another job reusing A's key stays charged, also after B is updated |
| Workloads without a controller | both stay charged, as before the fix |

On a Kueue without the fix, "basic replacement" fails at its second step with 5 cpu and 2 reserving
workloads, as do the other cases where the old slice should stop being charged. The last case keeps
the previous accounting by design, so it shows what a double charge looks like.

### job: elastic batch Job (real controllers)

Scales one elastic `batch/v1` Job 1 → 2 → 3 → 4 Pods of 100m each, 10s apart, in the 400m ClusterQueue,
while watching the Workloads and the ClusterQueue. The 2 → 3 and 3 → 4 replacements would be charged
500m and 700m if both slices counted. A trial fails if `ClusterQueue.status` ever reports more than 400m
or more than one workload holding quota.

The output also reports, per trial, the window from the replacement showing `Admitted` to the replaced
slice showing `Finished` in the API. Both slices hold `QuotaReserved` in the API during that window by
design; only the ClusterQueue status, which Kueue writes from its cache, must stay within the quota.

Kueue batches ClusterQueue status writes, so on a Kueue without the fix this test catches the window
only in some trials. Use the edge test to tell the two builds apart.

### job-shrink: a replacement that shrinks while it waits (real controllers)

Runs the same Job at 2 Pods, scales it to 6, so its replacement needs 600m and waits for quota, then
scales it to 1 while the replacement is pending. The pending replacement follows the Job and is
admitted with 1 Pod, fewer than the 2 of the slice it replaces; a Spark driver lowering its executor
target does the same. The larger slice covers the smaller one until it finishes, so a trial fails if
`ClusterQueue.status` ever reports more than 400m or more than one reserving workload, or if no
replacement was admitted smaller than the slice it replaces.

## Notes

- The edge test needs permission to patch `workloads/status`, which a cluster admin has.
- `clean` deletes only the kit's resources. The ResourceFlavor `wl-slice-quota` is the kit's own,
  so no shared flavor is touched.
- The kit was written with AI assistance (Claude).
