# Results

Run on 2026-11-05 against `ghcr.io/pradeep39/kueue:17afc39`: the four commits of kubernetes-sigs/kueue#18397 (head `cf06afe82`), applied to a Kueue main build that also carries Spark integration changes outside the cache code, Kubernetes v1.36.2. Command: `./workload-slice-quota-e2e.sh all --trials 8`.

```
context:      kubernetes-admin@kubernetes
Kueue image:  ghcr.io/pradeep39/kueue:17afc39
note:         namespace wl-slice-quota-job exists from an earlier run; it is reused
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
   ok   C deleted                                    charged  2 cpu, 1 reserving   (want 2 cpu, 1)
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

edge test: PASS  (log: out/20261105-230947-edge/edge.log)

trial  replacements         window (s)  status writes  max cpu  max reserving  result
1                 3        0.068-0.307              5     400m              1  PASS
2                 3        0.087-0.103              5     400m              1  PASS
3                 3        0.103-0.142              5     400m              1  PASS
4                 3        0.160-0.271              5     400m              1  PASS
5                 3        0.080-0.155              5     400m              1  PASS
6                 3        0.087-0.115              5     400m              1  PASS
7                 3        0.029-0.250              5     400m              1  PASS
8                 3        0.088-0.397              5     400m              1  PASS

window = time from the replacement showing Admitted to the replaced slice showing Finished, in the API.
Both slices hold QuotaReserved in the API during the window by design; only the ClusterQueue status,
which Kueue writes from its cache, must stay within the quota.
job test: PASS  (logs: out/20261105-231225-job)

summary: edge PASS, job PASS
```
