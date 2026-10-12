# Preemption baseline: whole-app eviction under Spark DA (Kueue elastic slices)

Runs: E3v2-preempt-partial-cap2, E4v2-preempt-partial-nocap, E5v2-preempt-atomic-nocap (Kueue 1201-g8ffea3222),
REQ-1011-1054-S4-preempt-partial-cap2, REQ-1011-1054-S5-preempt-atomic (1315-g096f6ff72). Appendix: REQ-1011-0255-S4/S5 (1313-g6ceebaa66).
Setup: als-lo1/als-lo2 (priority low) start, als-hi (high) applied +40s; SparkALS, DA min 3 / max 30, idle 5s; cohort = 12 slots
(CQ nominal 6 cpu + borrowing); 1 slot = 1 cpu + 896Mi, driver and each executor 1 slot.

Scripts: /tmp/preempt-analysis/scripts/preempt.py (per run, JSON), report.py (markdown). Run with `python3 -I`.

## Definitions
- eviction: each (slice, Evicted.lastTransitionTime) pair; a slice can be evicted more than once.
- preemptor slice: from the `PreemptedWorkload` event on the preemptor naming the victim (nearest in time).
- delta = preemptor slice spec slots at eviction time - slots granted to the slice it replaces (initial slice: full spec).
- free = 12 - reserved slots just before the eviction (per app, max over its quota-holding unfinished slices). `*` = computed negative, clamped to 0.
- need = max(0, delta - free). reclaim ratio = victim slots freed / need.
- slice wait = preemptor slice creation -> QuotaReserved; "after eviction" = Evicted -> preemptor QuotaReserved.
- victim attempt age = eviction - current driver pod Running (0 = driver not running yet). exec-s discarded = executor-seconds
  of that attempt's executors (Running, ungated) up to the eviction; the whole attempt restarts, so all of it is lost.
- burst = from the eviction until the preemptor's admitted size drops back to the pre-step size; burst peak = its max size.
- partial fits F: victim slots - need >= F (F=2: driver + 1 executor; F=4: driver + minExecutors 3).
- survives burst F: partial fits F and 12 - burst peak >= F (ignores the other low app, so optimistic).
- Counterfactual B (strict priority, ideal partial reclaim): replay the preemptor's actual size trace; low apps shrink to F before
  any is evicted; evict the youngest when 12 - H < alive*F; restart when F slots free.
- Counterfactual C (floor-protected): never take a low app below F; cap the preemptor at 12 - 2F (partial admission).
  0 evictions; cost = preemptor slot-seconds above the cap.

## Aggregate per run

| run | evictions | low-app restarts | evictions with need<=1 slot | median reclaim ratio | median / max preemptor slice wait s | median wait after eviction s | victim progress lost s | exec-s discarded | victims evicted before running | partial fits F=2/F=4 | survives burst F=2/F=4 | median burst len s | exec-s on burst-survivable evictions (F=2) | B evictions F=2/F=4 | C hi denied slot-s F=2/F=4 | completion hi/lo1/lo2 s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| E3v2-preempt-partial-cap2 | 7 | 5 | 6 | 4.0 | 6 / 8 | 6 | 195 | 561 | 1 | 7/3 | 3/1 | 25 | 170 | 2/4 | 76/273 | 156/255/DNF |
| E4v2-preempt-partial-nocap | 8 | 8 | 5 | 4.0 | 8 / 10 | 8 | 343 | 1162 | 2 | 7/5 | 4/2 | 24 | 319 | 5/7 | 48/323 | 361/197/DNF |
| E5v2-preempt-atomic-nocap | 10 | 10 | 5 | 3.0 | 9 / 12 | 4 | 207 | 581 | 1 | 6/1 | 2/0 | 22 | 69 | 4/5 | 100/347 | 169/DNF/304 |
| REQ-1011-1054-S4-preempt-partial-cap2 | 14 | 13 | 13 | 4.0 | 1 / 2 | 1 | 539 | 1598 | 2 | 13/5 | 10/1 | 16 | 1308 | 5/9 | 109/442 | 296/426/446 |
| REQ-1011-1054-S5-preempt-atomic | 11 | 11 | 7 | 4.0 | 1 / 3 | 1 | 335 | 954 | 2 | 8/2 | 1/0 | 15 | 12 | 9/9 | 128/474 | 234/360/DNF |

### E3v2-preempt-partial-cap2  (window 421s, watch starts {'wl': 1, 'pods': 1, 'sa': 1, 'ev': 1})

| # | evicted at | victim | victim slots (drv+exec) / live exec | preemptor slice | initial? | preemptor req slots | delta slots (exec) | free | need | reclaim ratio | slice wait s | of which after eviction | victim quota release s | victim attempt age s | exec-s discarded | burst peak / len s | partial fits F=2 / F=4 | survives burst F=2 / F=4 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 04:28:30 | als-lo1 | 7 (1+6) / 5 | als-hi/7c50c | Y | 4 | 4 (3) | 1 | 3 | 2.3 | 8 | 8 | 8 | 36 | 113 | 12 / 156 | Y / Y | n / n |
| 2 | 04:28:54 | als-lo1 | 4 (1+3) / 3 | als-hi/90e7b |  | 5 | 1 (1) | 0 | 1 | 4.0 | 7 | 7 | 8 | 13 | 21 | 12 / 60 | Y / n | n / n |
| 3 | 04:29:12 | als-lo2 | 5 (1+4) / 4 | als-hi/be501 |  | 8 | 1 (1) | 0 | 1 | 5.0 | 6 | 6 | 10 | 77 | 257 | 12 / 41 | Y / Y | n / n |
| 4 | 04:29:28 | als-lo2 | 4 (1+3) / 0 | als-hi/53824 |  | 9 | 1 (1) | 0 | 1 | 4.0 | 0 | 0 | 0 | 0 | 0 | 12 / 25 | Y / n | n / n |
| 5 | 04:30:25 | als-lo2 | 4 (1+3) / 3 | als-hi/a09a1 |  | 5 | 1 (1) | 0 | 1 | 4.0 | 7 | 7 | 7 | 26 | 59 | 6 / 9 | Y / n | Y / n |
| 6 | 04:30:39 | als-lo1 | 7 (1+6) / 5 | als-hi/c01fa |  | 6 | 1 (1) | 0 | 1 | 7.0 | 5 | 5 | 8 | 41 | 111 | 5 / 12 | Y / Y | Y / Y |
| 7 | 04:30:57 | als-lo2 | 4 (1+3) / 0 | als-hi/a0c5a |  | 5 | 1 (1) | 0* | 1 | 4.0 | 6 | 6 | - | 2 | 0 | 4 / 9 | Y / n | Y / n |

| app | evictions | driver restarts | progress lost s (sum of attempt ages) | exec-s discarded | completion s (create->COMPLETED) |
|---|---|---|---|---|---|
| als-hi | 0 | 0 | 0 | 0 | 156 |
| als-lo1 | 3 | 3 | 90 | 245 | 255 |
| als-lo2 | 4 | 2 | 105 | 316 | not finished in window (SUSPENDED) |

preemptor peak 12 slots; time at >=8/9/10/11/12 slots: {'8': 30.0, '9': 24.0, '10': 21.0, '11': 18.0, '12': 13.0}  
counterfactual B (strict priority, ideal partial reclaim to floor): F=2 {'evictions': 2, 'lost_xsec': 195, 'hi_cap': 8.0, 'hi_denied_slot_sec': 76}, F=4 {'evictions': 4, 'lost_xsec': 612, 'hi_cap': 4.0, 'hi_denied_slot_sec': 273}  

### E4v2-preempt-partial-nocap  (window 420s, watch starts {'wl': 1, 'pods': 1, 'sa': 1, 'ev': 1})

| # | evicted at | victim | victim slots (drv+exec) / live exec | preemptor slice | initial? | preemptor req slots | delta slots (exec) | free | need | reclaim ratio | slice wait s | of which after eviction | victim quota release s | victim attempt age s | exec-s discarded | burst peak / len s | partial fits F=2 / F=4 | survives burst F=2 / F=4 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 04:35:43 | als-lo1 | 8 (1+7) / 7 | als-hi/def21 | Y | 4 | 4 (3) | 0 | 4 | 2.0 | 8 | 8 | 8 | 37 | 165 | 12 / 361 | Y / Y | n / n |
| 2 | 04:36:06 | als-lo1 | 4 (1+3) / 3 | als-hi/872fc |  | 5 | 1 (1) | 0 | 1 | 4.0 | 8 | 8 | 8 | 11 | 18 | 8 / 46 | Y / n | Y / n |
| 3 | 04:36:16 | als-lo2 | 7 (1+6) / 3 | als-hi/fc1d1 |  | 7 | 2 (2) | 0 | 2 | 3.5 | 10 | 10 | 10 | 69 | 202 | 8 / 36 | Y / Y | Y / Y |
| 4 | 04:38:22 | als-lo2 | 8 (1+7) / 3 | als-hi/e73c9 |  | 5 | 1 (1) | 0 | 1 | 8.0 | 10 | 10 | 11 | 85 | 238 | 11 / 25 | Y / Y | n / n |
| 5 | 04:38:36 | als-lo2 | 4 (1+3) / 0 | als-hi/1e125 |  | 11 | 3 (3) | 0 | 3 | 1.3 | 1 | 1 | 1 | 0 | 0 | 11 / 8 | n / n | n / n |
| 6 | 04:40:42 | als-lo2 | 8 (1+7) / 7 | als-hi/33ebc |  | 5 | 1 (1) | 0 | 1 | 8.0 | 9 | 9 | 9 | 114 | 440 | 12 / 24 | Y / Y | n / n |
| 7 | 04:40:58 | als-lo2 | 4 (1+3) / 0 | als-hi/822cd |  | 9 | 1 (1) | 0 | 1 | 4.0 | 0 | 0 | 0 | 0 | 0 | 9 / 8 | Y / n | Y / n |
| 8 | 04:41:37 | als-lo2 | 8 (1+7) / 7 | als-hi/37ed5 |  | 5 | 1 (1) | 0 | 1 | 8.0 | 5 | 5 | 10 | 27 | 99 | 4 / 7 | Y / Y | Y / Y |

| app | evictions | driver restarts | progress lost s (sum of attempt ages) | exec-s discarded | completion s (create->COMPLETED) |
|---|---|---|---|---|---|
| als-hi | 0 | 0 | 0 | 0 | 361 |
| als-lo1 | 2 | 2 | 48 | 183 | 197 |
| als-lo2 | 6 | 6 | 295 | 979 | not finished in window (RUNNING) |

preemptor peak 12 slots; time at >=8/9/10/11/12 slots: {'8': 50.0, '9': 20.0, '10': 12.0, '11': 12.0, '12': 4.0}  
counterfactual B (strict priority, ideal partial reclaim to floor): F=2 {'evictions': 5, 'lost_xsec': 677, 'hi_cap': 8.0, 'hi_denied_slot_sec': 48}, F=4 {'evictions': 7, 'lost_xsec': 1833, 'hi_cap': 4.0, 'hi_denied_slot_sec': 323}  

### E5v2-preempt-atomic-nocap  (window 421s, watch starts {'wl': 1, 'pods': 1, 'sa': 1, 'ev': 1})

| # | evicted at | victim | victim slots (drv+exec) / live exec | preemptor slice | initial? | preemptor req slots | delta slots (exec) | free | need | reclaim ratio | slice wait s | of which after eviction | victim quota release s | victim attempt age s | exec-s discarded | burst peak / len s | partial fits F=2 / F=4 | survives burst F=2 / F=4 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 04:43:00 | als-lo1 | 8 (1+7) / 7 | als-hi/c7c7e | Y | 4 | 4 (3) | 0 | 4 | 2.0 | 9 | 9 | 9 | 38 | 172 | 12 / 169 | Y / Y | n / n |
| 2 | 04:43:24 | als-lo1 | 4 (1+3) / 3 | als-hi/e9467 |  | 5 | 1 (1) | 0 | 1 | 4.0 | 12 | 12 | 10 | 11 | 12 | 12 / 54 | Y / n | n / n |
| 3 | 04:43:32 | als-lo2 | 4 (1+3) / 3 | als-hi/e9467 |  | 11 | 7 (7) | 0 | 7 | 0.6 | 12 | 4 | 9 | 69 | 205 | 12 / 46 | n / n | n / n |
| 4 | 04:43:48 | als-lo1 | 4 (1+3) / 0 | als-hi/9458c |  | 9 | 1 (1) | 0 | 1 | 4.0 | 2 | 2 | 2 | 1 | 0 | 12 / 30 | Y / n | n / n |
| 5 | 04:44:30 | als-lo2 | 4 (1+3) / 3 | als-hi/76586 |  | 5 | 1 (1) | 0 | 1 | 4.0 | 9 | 9 | 7 | 8 | 10 | 11 / 25 | Y / n | n / n |
| 6 | 04:44:35 | als-lo1 | 4 (1+3) / 3 | als-hi/76586 |  | 9 | 5 (5) | 0 | 5 | 0.8 | 9 | 4 | 7 | 13 | 23 | 11 / 20 | n / n | n / n |
| 7 | 04:44:44 | als-lo1 | 4 (1+3) / 0 | als-hi/66461 |  | 11 | 3 (3) | 0 | 3 | 1.3 | 0 | 1 | 1 | 0 | 0 | 11 / 9 | n / n | n / n |
| 8 | 04:45:26 | als-lo1 | 4 (1+3) / 3 | als-hi/b893a |  | 5 | 1 (1) | 0 | 1 | 4.0 | 9 | 9 | 9 | 28 | 69 | 8 / 20 | Y / n | Y / n |
| 9 | 04:45:31 | als-lo2 | 4 (1+3) / 3 | als-hi/b893a |  | 9 | 5 (5) | 0 | 5 | 0.8 | 9 | 4 | - | 35 | 90 | 8 / 15 | n / n | n / n |
| 10 | 04:45:43 | als-lo2 | 4 (1+3) / 0 | als-hi/c17bb |  | 9 | 1 (1) | 0 | 1 | 4.0 | 2 | 2 | 9 | 4 | 0 | 8 / 2 | Y / n | Y / n |

| app | evictions | driver restarts | progress lost s (sum of attempt ages) | exec-s discarded | completion s (create->COMPLETED) |
|---|---|---|---|---|---|
| als-hi | 0 | 0 | 0 | 0 | 169 |
| als-lo1 | 6 | 6 | 91 | 276 | not finished in window (RUNNING) |
| als-lo2 | 4 | 4 | 116 | 305 | 304 |

preemptor peak 12 slots; time at >=8/9/10/11/12 slots: {'8': 49.0, '9': 35.0, '10': 29.0, '11': 23.0, '12': 13.0}  
counterfactual B (strict priority, ideal partial reclaim to floor): F=2 {'evictions': 4, 'lost_xsec': 242, 'hi_cap': 8.0, 'hi_denied_slot_sec': 100}, F=4 {'evictions': 5, 'lost_xsec': 753, 'hi_cap': 4.0, 'hi_denied_slot_sec': 347}  

### REQ-1011-1054-S4-preempt-partial-cap2  (window 482s, watch starts {'wl': 1, 'pods': 1, 'sa': 1, 'ev': 1})

| # | evicted at | victim | victim slots (drv+exec) / live exec | preemptor slice | initial? | preemptor req slots | delta slots (exec) | free | need | reclaim ratio | slice wait s | of which after eviction | victim quota release s | victim attempt age s | exec-s discarded | burst peak / len s | partial fits F=2 / F=4 | survives burst F=2 / F=4 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 16:14:14 | als-lo1 | 5 (1+4) / 3 | als-hi/4c52e | Y | 4 | 4 (3) | 0 | 4 | 1.2 | 1 | 1 | 1 | 37 | 136 | 12 / 296 | n / n | n / n |
| 2 | 16:14:37 | als-lo2 | 8 (1+7) / 7 | als-hi/cba0f |  | 5 | 1 (1) | 0 | 1 | 8.0 | 1 | 1 | 1 | 60 | 285 | 5 / 8 | Y / Y | Y / Y |
| 3 | 16:14:55 | als-lo1 | 4 (1+3) / 0 | als-hi/d62f2 |  | 5 | 1 (1) | 0 | 1 | 4.0 | 1 | 1 | 1 | 5 | 0 | 9 / 18 | Y / n | Y / n |
| 4 | 16:14:59 | als-lo2 | 6 (1+5) / 3 | als-hi/05969 |  | 7 | 1 (1) | 0 | 1 | 6.0 | 2 | 2 | 2 | 16 | 32 | 9 / 14 | Y / Y | Y / n |
| 5 | 16:15:03 | als-lo1 | 4 (1+3) / 0 | als-hi/80eb6 |  | 9 | 1 (1) | 0 | 1 | 4.0 | 1 | 1 | 1 | 0 | 0 | 9 / 8 | Y / n | Y / n |
| 6 | 16:15:44 | als-lo1 | 4 (1+3) / 3 | als-hi/129d1 |  | 5 | 1 (1) | 0 | 1 | 4.0 | 1 | 1 | 1 | 28 | 66 | 12 / 43 | Y / n | n / n |
| 7 | 16:15:51 | als-lo2 | 6 (1+5) / 3 | als-hi/68571 |  | 7 | 1 (1) | 0 | 1 | 6.0 | 2 | 2 | 2 | 35 | 88 | 12 / 36 | Y / Y | n / n |
| 8 | 16:15:59 | als-lo2 | 4 (1+3) / 0 | als-hi/be476 |  | 9 | 1 (1) | 0 | 1 | 4.0 | 1 | 1 | 1 | 2 | 0 | 12 / 28 | Y / n | n / n |
| 9 | 16:16:39 | als-lo2 | 4 (1+3) / 2 | als-hi/de855 |  | 5 | 1 (1) | 0 | 1 | 4.0 | 1 | 1 | 1 | 8 | 7 | 9 / 17 | Y / n | Y / n |
| 10 | 16:16:44 | als-lo1 | 4 (1+3) / 3 | als-hi/149ba |  | 9 | 1 (1) | 0 | 1 | 4.0 | 2 | 2 | 2 | 13 | 22 | 9 / 11 | Y / n | Y / n |
| 11 | 16:18:46 | als-lo2 | 4 (1+3) / 3 | als-hi/3e816 |  | 5 | 1 (1) | 0 | 1 | 4.0 | 1 | 1 | 2 | 107 | 302 | 10 / 23 | Y / n | Y / n |
| 12 | 16:18:53 | als-lo1 | 6 (1+5) / 4 | als-hi/575f6 |  | 7 | 1 (1) | 0 | 1 | 6.0 | 2 | 2 | 2 | 113 | 327 | 10 / 15 | Y / Y | Y / n |
| 13 | 16:18:55 | als-lo1 | 5 (1+4) / 5 | als-hi/32098 |  | 8 | 1 (1) | 0 | 1 | 5.0 | 1 | 1 | 1 | 115 | 333 | 10 / 13 | Y / Y | Y / n |
| 14 | 16:19:01 | als-lo2 | 4 (1+3) / 0 | als-hi/589c9 |  | 9 | 1 (1) | 0 | 1 | 4.0 | 1 | 1 | 1 | 0 | 0 | 10 / 7 | Y / n | Y / n |

| app | evictions | driver restarts | progress lost s (sum of attempt ages) | exec-s discarded | completion s (create->COMPLETED) |
|---|---|---|---|---|---|
| als-hi | 0 | 0 | 0 | 0 | 296 |
| als-lo1 | 7 | 6 | 311 | 884 | 426 |
| als-lo2 | 7 | 7 | 228 | 714 | 446 |

preemptor peak 12 slots; time at >=8/9/10/11/12 slots: {'8': 68.0, '9': 49.0, '10': 26.0, '11': 21.0, '12': 13.0}  
counterfactual B (strict priority, ideal partial reclaim to floor): F=2 {'evictions': 5, 'lost_xsec': 431, 'hi_cap': 8.0, 'hi_denied_slot_sec': 109}, F=4 {'evictions': 9, 'lost_xsec': 1536, 'hi_cap': 4.0, 'hi_denied_slot_sec': 442}  

### REQ-1011-1054-S5-preempt-atomic  (window 481s, watch starts {'wl': 1, 'pods': 1, 'sa': 1, 'ev': 1})

| # | evicted at | victim | victim slots (drv+exec) / live exec | preemptor slice | initial? | preemptor req slots | delta slots (exec) | free | need | reclaim ratio | slice wait s | of which after eviction | victim quota release s | victim attempt age s | exec-s discarded | burst peak / len s | partial fits F=2 / F=4 | survives burst F=2 / F=4 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 16:22:36 | als-lo1 | 8 (1+7) / 7 | als-hi/2197a | Y | 4 | 4 (3) | 0 | 4 | 2.0 | 1 | 1 | 1 | 37 | 174 | 12 / 234 | Y / Y | n / n |
| 2 | 16:22:53 | als-lo1 | 4 (1+3) / 3 | als-hi/35a85 |  | 5 | 1 (1) | 0 | 1 | 4.0 | 1 | 1 | 1 | 12 | 19 | 12 / 38 | Y / n | n / n |
| 3 | 16:23:05 | als-lo2 | 4 (1+3) / 3 | als-hi/e9040 |  | 9 | 1 (1) | 0 | 1 | 4.0 | 1 | 1 | 1 | 66 | 192 | 12 / 25 | Y / n | n / n |
| 4 | 16:24:27 | als-lo2 | 4 (1+3) / 3 | als-hi/fc933 |  | 5 | 1 (1) | 0 | 1 | 4.0 | 1 | 1 | 1 | 52 | 141 | 12 / 15 | Y / n | n / n |
| 5 | 16:24:31 | als-lo1 | 6 (1+5) / 3 | als-hi/12027 |  | 7 | 1 (1) | 0 | 1 | 6.0 | 1 | 1 | 1 | 56 | 155 | 12 / 11 | Y / Y | n / n |
| 6 | 16:24:34 | als-lo2 | 4 (1+3) / 0 | als-hi/e3bfd |  | 11 | 3 (3) | 0 | 3 | 1.3 | 3 | 3 | 3 | 0 | 0 | 12 / 7 | n / n | n / n |
| 7 | 16:25:31 | als-lo1 | 4 (1+3) / 3 | als-hi/8aed3 |  | 5 | 1 (1) | 0 | 1 | 4.0 | 2 | 2 | 2 | 46 | 123 | 11 / 19 | Y / n | n / n |
| 8 | 16:25:36 | als-lo2 | 4 (1+3) / 3 | als-hi/96314 |  | 11 | 4 (4) | 1 | 3 | 1.3 | 2 | 2 | 2 | 51 | 138 | 11 / 14 | n / n | n / n |
| 9 | 16:25:55 | als-lo1 | 4 (1+3) / 0 | als-hi/9d3e2 |  | 5 | 1 (1) | 0 | 1 | 4.0 | 1 | 1 | 1 | 0 | 0 | 11 / 34 | Y / n | n / n |
| 10 | 16:26:01 | als-lo2 | 4 (1+3) / 0 | als-hi/203a8 |  | 11 | 3 (3) | 0 | 3 | 1.3 | 3 | 3 | 3 | 6 | 0 | 11 / 9 | n / n | n / n |
| 11 | 16:26:24 | als-lo2 | 4 (1+3) / 3 | als-hi/5a44e |  | 9 | 1 (1) | 0 | 1 | 4.0 | 1 | 1 | 1 | 9 | 12 | 9 / 4 | Y / n | Y / n |

| app | evictions | driver restarts | progress lost s (sum of attempt ages) | exec-s discarded | completion s (create->COMPLETED) |
|---|---|---|---|---|---|
| als-hi | 0 | 0 | 0 | 0 | 234 |
| als-lo1 | 5 | 5 | 151 | 471 | 360 |
| als-lo2 | 6 | 6 | 184 | 483 | not finished in window (RUNNING) |

preemptor peak 12 slots; time at >=8/9/10/11/12 slots: {'8': 69.0, '9': 49.0, '10': 38.0, '11': 31.0, '12': 10.0}  
counterfactual B (strict priority, ideal partial reclaim to floor): F=2 {'evictions': 9, 'lost_xsec': 437, 'hi_cap': 8.0, 'hi_denied_slot_sec': 128}, F=4 {'evictions': 9, 'lost_xsec': 1170, 'hi_cap': 4.0, 'hi_denied_slot_sec': 474}  

TOTALS {"n": 50, "restarts": 47, "xsec": 4856.0, "age": 1619.0, "need1": 36, "fit2": 41, "fit4": 16, "surv2": 20, "surv4": 4, "av_x": 1878.0, "zero": 8, "B2": 25, "B4": 34, "C2": 461}

C4 total hi denied slot-s (F=4): 1859 over 5 runs

# Appendix: REQ-1011-0255 (older build 1313)

## Aggregate per run

| run | evictions | low-app restarts | evictions with need<=1 slot | median reclaim ratio | median / max preemptor slice wait s | median wait after eviction s | victim progress lost s | exec-s discarded | victims evicted before running | partial fits F=2/F=4 | survives burst F=2/F=4 | median burst len s | exec-s on burst-survivable evictions (F=2) | B evictions F=2/F=4 | C hi denied slot-s F=2/F=4 | completion hi/lo1/lo2 s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| REQ-1011-0255-S4-preempt-partial-cap2 | 10 | 7 | 9 | 4.5 | 4 / 11 | 4 | 204 | 576 | 4 | 10/6 | 4/2 | 32 | 185 | 2/3 | 75/318 | 155/410/268 |
| REQ-1011-0255-S5-preempt-atomic | 3 | 3 | 2 | 4.0 | 8 / 8 | 8 | 103 | 405 | 0 | 3/1 | 0/0 | 59 | 0 | 2/2 | 98/288 | 83/246/194 |

### REQ-1011-0255-S4-preempt-partial-cap2  (window 481s, watch starts {'wl': 1, 'pods': 1, 'sa': 1, 'ev': 1})

| # | evicted at | victim | victim slots (drv+exec) / live exec | preemptor slice | initial? | preemptor req slots | delta slots (exec) | free | need | reclaim ratio | slice wait s | of which after eviction | victim quota release s | victim attempt age s | exec-s discarded | burst peak / len s | partial fits F=2 / F=4 | survives burst F=2 / F=4 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 08:14:45 | als-lo1 | 7 (1+6) / 5 | als-hi/ba266 | Y | 4 | 4 (3) | 1 | 3 | 2.3 | 11 | 11 | 11 | 37 | 119 | 12 / 155 | Y / Y | n / n |
| 2 | 08:15:10 | als-lo2 | 4 (1+3) / 3 | als-hi/53b8f |  | 5 | 1 (1) | 0 | 1 | 4.0 | 8 | 8 | 8 | 62 | 211 | 12 / 56 | Y / n | n / n |
| 3 | 08:15:24 | als-lo1 | 6 (1+5) / 4 | als-hi/e9d61 |  | 7 | 1 (1) | 0 | 1 | 6.0 | 6 | 6 | 8 | 25 | 61 | 12 / 42 | Y / Y | n / n |
| 4 | 08:15:33 | als-lo1 | 6 (1+5) / 0 | als-hi/e74e3 |  | 7 | 1 (1) | 0 | 1 | 6.0 | 0 | 0 | 0 | 0 | 0 | 12 / 33 | Y / Y | n / n |
| 5 | 08:15:37 | als-lo1 | 5 (1+4) / 0 | als-hi/00947 |  | 8 | 1 (1) | 0 | 1 | 5.0 | 1 | 1 | 0 | 0 | 0 | 12 / 28 | Y / Y | n / n |
| 6 | 08:15:41 | als-lo1 | 4 (1+3) / 0 | als-hi/2cd42 |  | 9 | 1 (1) | 0 | 1 | 4.0 | 0 | 0 | 0 | 0 | 0 | 12 / 24 | Y / n | n / n |
| 7 | 08:16:37 | als-lo2 | 4 (1+3) / 3 | als-hi/d3e9a |  | 5 | 1 (1) | 0 | 1 | 4.0 | 8 | 7 | 8 | 28 | 64 | 8 / 40 | Y / n | Y / n |
| 8 | 08:16:46 | als-lo2 | 4 (1+3) / 0 | als-hi/43f5f |  | 5 | 1 (1) | 0 | 1 | 4.0 | 0 | 0 | 0 | 0 | 0 | 8 / 31 | Y / n | Y / n |
| 9 | 08:16:49 | als-lo1 | 6 (1+5) / 4 | als-hi/64cd9 |  | 7 | 1 (1) | 0 | 1 | 6.0 | 7 | 7 | 9 | 39 | 102 | 8 / 28 | Y / Y | Y / Y |
| 10 | 08:17:14 | als-lo1 | 6 (1+5) / 3 | als-hi/136d1 |  | 9 | 1 (1) | 0* | 1 | 6.0 | 2 | 2 | 7 | 13 | 19 | 8 / 3 | Y / Y | Y / Y |

| app | evictions | driver restarts | progress lost s (sum of attempt ages) | exec-s discarded | completion s (create->COMPLETED) |
|---|---|---|---|---|---|
| als-hi | 0 | 0 | 0 | 0 | 155 |
| als-lo1 | 7 | 4 | 114 | 301 | 410 |
| als-lo2 | 3 | 3 | 90 | 275 | 268 |

preemptor peak 12 slots; time at >=8/9/10/11/12 slots: {'8': 41.0, '9': 24.0, '10': 20.0, '11': 17.0, '12': 14.0}  
counterfactual B (strict priority, ideal partial reclaim to floor): F=2 {'evictions': 2, 'lost_xsec': 193, 'hi_cap': 8.0, 'hi_denied_slot_sec': 75}, F=4 {'evictions': 3, 'lost_xsec': 609, 'hi_cap': 4.0, 'hi_denied_slot_sec': 318}  

### REQ-1011-0255-S5-preempt-atomic  (window 480s, watch starts {'wl': 1, 'pods': 1, 'sa': 1, 'ev': 1})

| # | evicted at | victim | victim slots (drv+exec) / live exec | preemptor slice | initial? | preemptor req slots | delta slots (exec) | free | need | reclaim ratio | slice wait s | of which after eviction | victim quota release s | victim attempt age s | exec-s discarded | burst peak / len s | partial fits F=2 / F=4 | survives burst F=2 / F=4 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 08:23:05 | als-lo2 | 7 (1+6) / 6 | als-hi/7edbf | Y | 4 | 4 (3) | 0 | 4 | 1.8 | 8 | 8 | 8 | 36 | 163 | 12 / 83 | Y / n | n / n |
| 2 | 08:23:28 | als-lo1 | 8 (1+7) / 7 | als-hi/d8651 |  | 5 | 1 (1) | 0 | 1 | 8.0 | 8 | 8 | 8 | 59 | 233 | 12 / 59 | Y / Y | n / n |
| 3 | 08:23:49 | als-lo2 | 4 (1+3) / 3 | als-hi/0b055 |  | 9 | 1 (1) | 0 | 1 | 4.0 | 5 | 5 | 7 | 8 | 9 | 12 / 37 | Y / n | n / n |

| app | evictions | driver restarts | progress lost s (sum of attempt ages) | exec-s discarded | completion s (create->COMPLETED) |
|---|---|---|---|---|---|
| als-hi | 0 | 0 | 0 | 0 | 83 |
| als-lo1 | 1 | 1 | 59 | 233 | 246 |
| als-lo2 | 2 | 2 | 44 | 172 | 194 |

preemptor peak 12 slots; time at >=8/9/10/11/12 slots: {'8': 42.0, '9': 30.0, '10': 27.0, '11': 22.0, '12': 19.0}  
counterfactual B (strict priority, ideal partial reclaim to floor): F=2 {'evictions': 2, 'lost_xsec': 181, 'hi_cap': 8.0, 'hi_denied_slot_sec': 98}, F=4 {'evictions': 2, 'lost_xsec': 462, 'hi_cap': 4.0, 'hi_denied_slot_sec': 288}  

TOTALS {"n": 13, "restarts": 10, "xsec": 981.0, "age": 307.0, "need1": 11, "fit2": 13, "fit4": 7, "surv2": 4, "surv4": 2, "av_x": 185.0, "zero": 4, "B2": 4, "B4": 5, "C2": 173}

## Overall (5 primary runs)
- 50 evictions (edge-analyze.py reports 41: it dedups by slice name and misses re-evictions of the same slice), 47 low-app driver restarts.
- 36/50 evictions needed <=1 slot; 248 slots freed vs 89 needed (2.8x; median 4x). 33/50 victims were at the 4-slot DA floor.
- 5 evictions were the preemptor's initial admission (4 slots); 3 preemptor slices took two victims sequentially because their
  spec grew while pending (e.g. 5 -> 11 executors).
- Preemptor wait median 2s, pre-eviction share 0s: the whole wait is victim teardown. Build 1315: 1-3s; build 1201: 6-12s.
- Victim cost: 4856 executor-s + 1619 driver-s discarded; 8 evictions hit an app whose driver was not running yet.
- Preemptor bursts: median 17s (step -> back to pre-step size), median 13s to its next scale-down; it peaks at 11-12 of 12 slots in every run.
- Partial reclaim fits immediately in 41/50 (F=2) / 16/50 (F=4); survives the burst in 20/50 / 4/50.
- B: evictions 50 -> 25 (F=2) / 34 (F=4). exec-s on burst-survivable evictions 1878 (39%).
- C: 0 restarts, all 4856 exec-s kept; preemptor denied 461 slot-s (F=2) or 1859 (F=4, preemptor pinned at 4 slots = no elasticity).

## Caveats
- Driver logs hold only the first attempt per app (capture.sh attaches by pod name; reruns reuse `<app>-driver`), so progress is
  wall-clock age, not Spark progress. Later-attempt driver logs are missing.
- Watch timestamps are 1s (`now|todate`, Z); log timestamps carry -05:00 offsets (not used for the metrics). Waits <=1s are quantized.
- 1 RESYNC per stream in every run (no watch restarts); watch-errors/log-errors empty.
- The counterfactuals replay the preemptor's actual trace (itself shaped by the thrash), ignore low-app DA growth and low completion,
  and do not price the rework when reclaimed executors drop tasks/shuffle output (shuffleTracking on).
- Low apps did not finish inside the 420-480s window in 4 of 5 runs (DNF), so completion time is a lower bound there.
- Builds differ (1201 vs 1315): teardown latency is not comparable between the E* and REQ-1054 runs.
