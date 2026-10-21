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
| r3 | — | — | — | — | — | ⏳ PENDING |
| r4 | — | — | — | — | — | ⏳ PENDING |
| r5 | — | — | — | — | — | ⏳ PENDING |

## kafka-restart (target: 5 reps)
| Rep | Source | Physical Rows | Missing | Excess Dups | Recovery (s) | Valid |
|---|---|---|---|---|---|---|
| r1–r5 | — | — | — | — | — | ⏳ PENDING |

## spark-worker-stop (target: 5 reps)
| Rep | Source | Physical Rows | Missing | Excess Dups | Recovery (s) | Valid |
|---|---|---|---|---|---|---|
| r1–r5 | — | — | — | — | — | ⏳ PENDING |

## spark-worker-restart (target: 5 reps)
| Rep | Source | Physical Rows | Missing | Excess Dups | Recovery (s) | Valid |
|---|---|---|---|---|---|---|
| r1–r5 | — | — | — | — | — | ⏳ PENDING |

## datanode-stop (target: 5 reps)
| Rep | Source | Physical Rows | Missing | Excess Dups | Recovery (s) | Valid |
|---|---|---|---|---|---|---|
| r1–r5 | — | — | — | — | — | ⏳ PENDING |

---

## Run Log
| Timestamp (UTC) | Experiment | Rep | Result | Notes |
|---|---|---|---|---|
| 2026-10-20T19:42 | calibration | — | VALID | 289,744 rows, 212.77 s active, 44.6 s warm-up |
| 2026-10-20T20:xx | kafka-stop | r1 | VALID | pilot-v3, recovery 10.06 s, host_memory_pressure=true |
| 2026-10-20T20:xx | kafka-stop | r2 | VALID | pilot-v3, recovery 12.74 s, host_memory_pressure=true |
