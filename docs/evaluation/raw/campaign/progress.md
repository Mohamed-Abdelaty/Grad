# Campaign Progress Tracker
**Last updated**: 2026-10-21  
**Branch**: `feature/gap-closure`

---

## Calibration (Control Baseline)
| Run | Output Path | Physical Rows | Missing | Extra | Excess Dups | Valid |
|---|---|---|---|---|---|---|
| calibration-1792537189 | `/data/output/pipe-eval-calibration-1792537189` | 289,744 | 0 | 0 | 0 | ✅ VALID |

Calibration throughput: ~1,472 rows/s steady-state (micro-batch telemetry in `calibration_report.json`).

---

## kafka-stop (target: 5 reps)
| Rep | Source | Physical Rows | Missing | Excess Dups | Recovery (s) | Valid |
|---|---|---|---|---|---|---|
| r1 | pilot-v3 | 289,744 | 0 | 0 | 10.06 | ✅ VALID |
| r2 | pilot-v3 | 289,744 | 0 | 0 (HYPO) | 12.74 | ✅ VALID |
| r3 | campaign | 289,744 | 0 | 0 | 20.97 | ✅ VALID |
| r4 | campaign | 289,744 | 0 | 0 | 23.00 | ✅ VALID |
| r5 | campaign | 289,744 | 0 | 0 | 20.83 | ✅ VALID |

## kafka-restart (target: 5 reps)
| Rep | Source | Physical Rows | Missing | Excess Dups | Recovery (s) | Valid |
|---|---|---|---|---|---|---|
| r1 | campaign | 289,744 | 0 | 0 | 3.22 | ✅ VALID |
| r2 | campaign | 289,744 | 0 | 0 | 0.10 | ✅ VALID |
| r3 | campaign | 289,744 | 0 | 0 | 0.13 | ✅ VALID |
| r4 | campaign | 289,744 | 0 | 0 | 0.07 | ✅ VALID |
| r5 | campaign | 289,744 | 0 | 0 | 0.07 | ✅ VALID |

## spark-worker-stop (target: 5 reps)
| Rep | Source | Physical Rows | Missing | Excess Dups | Recovery (s) | Valid |
|---|---|---|---|---|---|---|
| r1 | campaign | 289,744 | 0 | 0 | 0.55 | ✅ VALID |
| r2 | campaign | 289,744 | 0 | 0 | 0.36 | ✅ VALID |
| r3 | campaign | 289,744 | 0 | 0 | 0.56 | ✅ VALID |
| r4 | campaign | 289,744 | 0 | 0 | 0.32 | ✅ VALID |
| r5 | campaign | 289,744 | 0 | 0 | 0.59 | ✅ VALID |

## spark-worker-restart (target: 5 reps)
| Rep | Source | Physical Rows | Missing | Excess Dups | Recovery (s) | Valid |
|---|---|---|---|---|---|---|
| r1 | campaign | 289,744 | 0 | 0 | 0.13 | ✅ VALID |
| r2 | campaign | 289,744 | 0 | 0 | 0.23 | ✅ VALID |
| r3 | campaign | 289,744 | 0 | 0 | 0.07 | ✅ VALID |
| r4 | campaign | 289,744 | 0 | 0 | 0.09 | ✅ VALID |
| r5 | campaign | 289,744 | 0 | 0 | 0.07 | ✅ VALID |

## datanode-stop (target: 5 reps)
| Rep | Source | Physical Rows | Missing | Excess Dups | Recovery (s) | Valid |
|---|---|---|---|---|---|---|
| r1 | campaign | 289,744 | 0 | 0 | 0.54 | ✅ VALID |
| r2 | campaign | 54,995 | 234,749 | 0 | 0.38 | ✅ VALID (fault crash) |
| r3 | campaign | 289,744 | 0 | 0 | 0.39 | ✅ VALID |
| r4 | campaign | 289,744 | 0 | 0 | 0.47 | ✅ VALID |
| r5 | campaign | 54,990 | 234,754 | 0 | 0.41 | ✅ VALID (fault crash) |

---

## Run Log
| Timestamp (UTC) | Experiment | Rep | Result | Notes |
|---|---|---|---|---|
| 2026-10-20T19:42 | calibration | — | VALID | 289,744 rows, 212.77 s active, 44.6 s warm-up |
| 2026-10-20T20:xx | kafka-stop | r1 | VALID | pilot-v3, recovery 10.06 s, host_memory_pressure=true |
| 2026-10-20T20:xx | kafka-stop | r2 | VALID | pilot-v3, recovery 12.74 s, host_memory_pressure=true |
| 2026-10-21T06:54 | kafka-stop | r3 | VALID | campaign, recovery 20.97 s, host_memory_pressure=false |
| 2026-10-21T07:00 | kafka-stop | r4 | VALID | campaign, recovery 23.00 s, host_memory_pressure=true |
| 2026-10-21T07:07 | kafka-stop | r5 | VALID | campaign, recovery 20.83 s, host_memory_pressure=false |
| 2026-10-21T07:33 | kafka-restart | r1 | VALID | campaign, recovery 3.22 s, host_memory_pressure=false |
| 2026-10-21T07:51 | kafka-restart | r2 | VALID | campaign, recovery 0.10 s, host_memory_pressure=false |
| 2026-10-21T07:56 | kafka-restart | r3 | VALID | campaign, recovery 0.13 s, host_memory_pressure=false |
| 2026-10-21T08:03 | kafka-restart | r4 | VALID | campaign, recovery 0.07 s, host_memory_pressure=false |
| 2026-10-21T08:09 | kafka-restart | r5 | VALID | campaign, recovery 0.07 s, host_memory_pressure=false |
| 2026-10-21T08:16 | spark-worker-stop | r1 | VALID | campaign, recovery 0.55 s, host_memory_pressure=false |
| 2026-10-21T08:22 | spark-worker-stop | r2 | VALID | campaign, recovery 0.36 s, host_memory_pressure=false |
| 2026-10-21T08:27 | spark-worker-stop | r3 | VALID | campaign, recovery 0.56 s, host_memory_pressure=false |
| 2026-10-21T08:34 | spark-worker-stop | r4 | VALID | campaign, recovery 0.32 s, host_memory_pressure=false |
| 2026-10-21T08:41 | spark-worker-stop | r5 | VALID | campaign, recovery 0.59 s, host_memory_pressure=true |
| 2026-10-21T08:53 | spark-worker-restart | r1 | VALID | campaign, recovery 0.13 s, host_memory_pressure=false |
| 2026-10-21T08:59 | spark-worker-restart | r2 | VALID | campaign, recovery 0.23 s, host_memory_pressure=false |
| 2026-10-21T09:05 | spark-worker-restart | r3 | VALID | campaign, recovery 0.07 s, host_memory_pressure=false |
| 2026-10-21T10:07 | spark-worker-restart | r4 | VALID | campaign, recovery 0.09 s, host_memory_pressure=true |
| 2026-10-21T10:13 | spark-worker-restart | r5 | VALID | campaign, recovery 0.07 s, host_memory_pressure=true |
| 2026-10-21T10:29 | datanode-stop | r1 | VALID | campaign, recovery 0.54 s, 289,744 rows, host_memory_pressure=false |
| 2026-10-21T10:35 | datanode-stop | r2 | VALID | campaign, recovery 0.38 s, 54,995 rows (234,749 missing; HDFS 0-replica write failure), host_memory_pressure=false |
| 2026-10-21T10:37 | datanode-stop | r3 | VALID | campaign, recovery 0.39 s, 289,744 rows, host_memory_pressure=false |
| 2026-10-21T10:44 | datanode-stop | r4 | VALID | campaign, recovery 0.47 s, 289,744 rows, host_memory_pressure=false |
| 2026-10-21T10:50 | datanode-stop | r5 | VALID | campaign, recovery 0.41 s, 54,990 rows (234,754 missing; HDFS 0-replica write failure), host_memory_pressure=false |
