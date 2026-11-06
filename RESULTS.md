# Results

Run on 2026-11-06 against `ghcr.io/pradeep39/kueue:16ac70d`: the commits of kubernetes-sigs/kueue#18397 (fix as of `60b4e5869`), applied to a Kueue main build that also carries Spark integration changes outside the cache code, Kubernetes v1.36.2. Command: `./workload-slice-quota-e2e.sh all --trials 8`, from a fresh clone of this branch.

```
context:      kubernetes-admin@kubernetes
Kueue image:  ghcr.io/pradeep39/kueue:16ac70d
preflight: PASS

== basic replacement
   ok   A (2 cpu)                                    charged  2 cpu, 1 reserving   (want 2 cpu, 1)
   ok   B (3 cpu) replaces A                         charged  3 cpu, 1 reserving   (want 3 cpu, 1)
   ok   A finishes                                   charged  3 cpu, 1 reserving   (want 3 cpu, 1)
== replacement leaves first
   ok   A (2 cpu)                                    charged  2 cpu, 1 reserving   (want 2 cpu, 1)
   ok   B (3 cpu) replaces A                         charged  3 cpu, 1 reserving   (want 3 cpu, 1)
   ok   B deleted                                    charged  2 cpu, 1 reserving   (want 2 cpu, 1)
== chain of three
   ok   A (2 cpu)                                    charged  2 cpu, 1 reserving   (want 2 cpu, 1)
   ok   B (3 cpu) replaces A                         charged  3 cpu, 1 reserving   (want 3 cpu, 1)
   ok   C (4 cpu) replaces B                         charged  4 cpu, 1 reserving   (want 4 cpu, 1)
   ok   C deleted                                    charged  3 cpu, 1 reserving   (want 3 cpu, 1)
== chain, middle slice finishes first
   ok   A (2 cpu)                                    charged  2 cpu, 1 reserving   (want 2 cpu, 1)
   ok   B (3 cpu) replaces A                         charged  3 cpu, 1 reserving   (want 3 cpu, 1)
   ok   C (4 cpu) replaces B                         charged  4 cpu, 1 reserving   (want 4 cpu, 1)
   ok   B finishes                                   charged  4 cpu, 1 reserving   (want 4 cpu, 1)
   ok   C updated                                    charged  4 cpu, 1 reserving   (want 4 cpu, 1)
   ok   C deleted                                    charged  2 cpu, 1 reserving   (want 2 cpu, 1)
== replacement that shrinks
   ok   A (2 cpu)                                    charged  2 cpu, 1 reserving   (want 2 cpu, 1)
   ok   B (1 cpu) replaces A                         charged  3 cpu, 2 reserving   (want 3 cpu, 2)
   ok   A finishes                                   charged  1 cpu, 1 reserving   (want 1 cpu, 1)
== self-reference
   ok   S (1 cpu) names itself                       charged  1 cpu, 1 reserving   (want 1 cpu, 1)
== another namespace
   ok   X (1 cpu) in wl-slice-quota-edge-2           charged  1 cpu, 1 reserving   (want 1 cpu, 1)
   ok   B (3 cpu) names wl-slice-quota-edge-2/x      charged  4 cpu, 2 reserving   (want 4 cpu, 2)
== another job at the replaced slice's key
   ok   A (2 cpu)                                    charged  2 cpu, 1 reserving   (want 2 cpu, 1)
   ok   B (3 cpu) replaces A                         charged  3 cpu, 1 reserving   (want 3 cpu, 1)
   ok   A deleted                                    charged  3 cpu, 1 reserving   (want 3 cpu, 1)
   ok   A' (1 cpu) of other-job at A's key           charged  4 cpu, 2 reserving   (want 4 cpu, 2)
   ok   B updated                                    charged  4 cpu, 2 reserving   (want 4 cpu, 2)
== Workloads without a controller
   ok   A (2 cpu), no controller                     charged  2 cpu, 1 reserving   (want 2 cpu, 1)
   ok   B (3 cpu), no controller, replaces A         charged  5 cpu, 2 reserving   (want 5 cpu, 2)

edge test: PASS  (log: out/20261106-014254-edge/edge.log)

trial  replacements         window (s)  status writes  max cpu  max reserving  result
1                 3        0.092-0.232              5     400m              1  PASS
2                 3        0.095-0.332              5     400m              1  PASS
3                 3        0.070-0.297              5     400m              1  PASS
4                 3        0.119-0.249              5     400m              1  PASS
5                 3        0.063-0.094              5     400m              1  PASS
6                 3        0.072-0.362              5     400m              1  PASS
7                 3        0.159-0.261              5     400m              1  PASS
8                 3        0.173-0.593              5     400m              1  PASS

window = time from the replacement showing Admitted to the replaced slice showing Finished, in the API.
Both slices hold QuotaReserved in the API during the window by design; only the ClusterQueue status,
which Kueue writes from its cache, must stay within the quota.
job test: PASS  (logs: out/20261106-014549-job)

summary: edge PASS, job PASS
```
