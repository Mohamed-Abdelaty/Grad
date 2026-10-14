import { RunProgress } from '../../types/runProgress';
import { formatBytes, formatCount } from '../../lib/format';

export type VizStageState = 'pending' | 'active' | 'done' | 'error' | 'skipped' | 'faulted' | 'recovering';

export type Level = number | null; // 0..1, or null = unknown (indeterminate)

export interface StationModel {
  key: 'source' | 'kafka' | 'spark' | 'hdfs';
  label: string; // "Source", "Kafka", "Spark", "HDFS"
  state: VizStageState;
  level: Level; // fill level of the vessel / ring
  levelMeaning: string; // legend text, see rules below
  counter: string | null; // short real counter text, e.g. "12,000 / 20,000"
  working: boolean; // true: show the "working" motion (rotors, glow)
  note: string | null; // "STOPPED (fault: container_stop)", "recovered in 12.4 s", error message
}

export interface PipeModel {
  from: StationModel['key'];
  to: StationModel['key'];
  flow: number;
  blocked: boolean;
} // flow 0..1

export interface SceneModel {
  stations: StationModel[]; // always 4, in order
  pipes: PipeModel[]; // always 3
  completion: 'running' | 'succeeded' | 'failed' | 'cancelled' | 'live';
  announce: string; // one sentence for aria-live
  summary: string[]; // one sentence per station for the text equivalent
}

export interface SceneContext {
  peakLag: number;
  peakRate: number;
}

function clamp(val: number, min: number, max: number): number {
  if (Number.isNaN(val) || !Number.isFinite(val)) return min;
  return Math.max(min, Math.min(max, val));
}

export function mapFaultTargetToStation(target: string | null | undefined): StationModel['key'] | null {
  if (!target) return null;
  const t = target.trim().toLowerCase();
  if (t === 'kafka' || t.startsWith('kafka-') || t.startsWith('kafka_')) return 'kafka';
  if (t === 'spark' || t.startsWith('spark')) return 'spark';
  if (t === 'namenode' || t === 'datanode' || t === 'hdfs' || t.startsWith('hdfs')) return 'hdfs';
  return null;
}

function getFaultNote(type: string, isDegraded: boolean): string {
  return isDegraded ? `DEGRADED (fault: ${type})` : `STOPPED (fault: ${type})`;
}

export function toSceneModel(p: RunProgress, ctx: SceneContext): SceneModel {
  const stageStates: Record<StationModel['key'], { state: VizStageState; error: string | null }> = {
    source: { state: 'pending', error: null },
    kafka: { state: 'pending', error: null },
    spark: { state: 'pending', error: null },
    hdfs: { state: 'pending', error: null },
  };

  if (Array.isArray(p.stages)) {
    for (const stage of p.stages) {
      if (stage && stage.key in stageStates) {
        stageStates[stage.key] = {
          state: stage.state,
          error: stage.error,
        };
      }
    }
  }

  // Station base details
  const faultedStationKey = p.fault ? mapFaultTargetToStation(p.fault.target) : null;

  // Resolve states and notes per station
  const stationKeys: StationModel['key'][] = ['source', 'kafka', 'spark', 'hdfs'];
  const stationLabels: Record<StationModel['key'], string> = {
    source: 'Source',
    kafka: 'Kafka',
    spark: 'Spark',
    hdfs: 'HDFS',
  };

  const resolvedStations: StationModel[] = stationKeys.map((key) => {
    let state = stageStates[key].state;
    let note: string | null = null;

    if (state === 'error') {
      note = stageStates[key].error;
    }

    // Apply fault overlay if this station is the target
    if (p.fault && faultedStationKey === key) {
      const { phase, type, recoverySeconds } = p.fault;
      if (phase === 'injected') {
        state = 'faulted';
        const isDegraded =
          type.includes('delay') ||
          type.includes('stress') ||
          type.includes('disruption') ||
          type.includes('pressure');
        note = getFaultNote(type, isDegraded);
      } else if (phase === 'recovering') {
        state = 'recovering';
        note = 'Recovering...';
      } else if (phase === 'recovered') {
        state = stageStates[key].state;
        note = typeof recoverySeconds === 'number' && !Number.isNaN(recoverySeconds)
          ? `Recovered in ${recoverySeconds} s`
          : 'Recovered';
      }
    }

    const working = state === 'active';

    if (key === 'source') {
      const rowsTotal = p.source?.rowsTotal;
      const rowsRead = p.source?.rowsRead;

      let level: Level = null;
      if (typeof rowsTotal === 'number' && rowsTotal > 0 && typeof rowsRead === 'number') {
        level = clamp(1 - rowsRead / rowsTotal, 0, 1);
      }

      let counter: string | null = null;
      if (rowsRead != null || rowsTotal != null) {
        if (rowsRead != null && rowsTotal != null) {
          counter = `${formatCount(rowsRead)} / ${formatCount(rowsTotal)}`;
        } else if (rowsRead != null) {
          counter = `${formatCount(rowsRead)}`;
        } else {
          counter = `— / ${formatCount(rowsTotal)}`;
        }
      }

      return {
        key: 'source',
        label: stationLabels.source,
        state,
        level,
        levelMeaning: 'Rows still to be read from the source',
        counter,
        working,
        note,
      };
    }

    if (key === 'kafka') {
      let level: Level = null;
      let levelMeaning = 'Records published to the topic';
      let counter: string | null = null;

      if (p.mode === 'streaming') {
        const lag = p.kafka?.consumerLag;
        if (typeof lag === 'number' && !Number.isNaN(lag)) {
          level = ctx.peakLag > 0 ? clamp(lag / ctx.peakLag, 0, 1) : 0;
          levelMeaning = 'Backlog: records waiting to be consumed (relative to the peak seen in this run)';
        } else {
          level = null;
          if (note == null) {
            note = 'No consumer attached';
          }
        }

        if (typeof lag === 'number' && !Number.isNaN(lag)) {
          counter = `lag: ${formatCount(lag)}`;
        } else if (typeof p.kafka?.published === 'number') {
          counter = formatCount(p.kafka.published);
        }
      } else {
        // Batch mode
        const total = p.source?.rowsTotal;
        const pub = p.kafka?.published;
        if (typeof total === 'number' && total > 0 && typeof pub === 'number') {
          level = clamp(pub / total, 0, 1);
        } else {
          level = null;
        }
        levelMeaning = 'Records published to the topic';

        if (typeof pub === 'number') {
          counter = formatCount(pub);
          if (typeof p.kafka?.failed === 'number' && p.kafka.failed > 0) {
            counter += `, ${formatCount(p.kafka.failed)} failed`;
          }
        } else if (typeof p.kafka?.failed === 'number' && p.kafka.failed > 0) {
          counter = `${formatCount(p.kafka.failed)} failed`;
        }
      }

      return {
        key: 'kafka',
        label: stationLabels.kafka,
        state,
        level,
        levelMeaning,
        counter,
        working,
        note,
      };
    }

    if (key === 'spark') {
      const activeStage = p.spark?.activeStage;
      let level: Level = null;
      if (
        activeStage &&
        typeof activeStage.tasksTotal === 'number' &&
        activeStage.tasksTotal > 0 &&
        typeof activeStage.tasksDone === 'number'
      ) {
        level = clamp(activeStage.tasksDone / activeStage.tasksTotal, 0, 1);
      }

      let counter: string | null = null;
      const tasksDone = p.spark?.tasksDone;
      const isFinished = state === 'done' || p.status === 'succeeded' || p.status === 'failed';
      const hasRecords = typeof p.spark?.recordsIn === 'number' && typeof p.spark?.recordsOut === 'number';

      if (typeof tasksDone === 'number') {
        counter = `${formatCount(tasksDone)} tasks done`;
        if (isFinished && hasRecords) {
          counter += `, in ${formatCount(p.spark?.recordsIn)} / out ${formatCount(p.spark?.recordsOut)}`;
        }
      } else if (isFinished && hasRecords) {
        counter = `in ${formatCount(p.spark?.recordsIn)} / out ${formatCount(p.spark?.recordsOut)}`;
      }

      return {
        key: 'spark',
        label: stationLabels.spark,
        state,
        level,
        levelMeaning: 'Tasks completed in the current Spark stage',
        counter,
        working,
        note,
      };
    }

    // HDFS
    let level: Level = null;
    if (p.hdfs?.verified === true) {
      level = 1;
    }

    const hdfsParts: string[] = [];
    if (typeof p.hdfs?.filesWritten === 'number') {
      const bytesPart = typeof p.hdfs?.bytesWritten === 'number' ? `, ${formatBytes(p.hdfs.bytesWritten)}` : '';
      hdfsParts.push(`${formatCount(p.hdfs.filesWritten)} files${bytesPart}`);
    } else if (typeof p.hdfs?.bytesWritten === 'number') {
      hdfsParts.push(formatBytes(p.hdfs.bytesWritten));
    }

    if (typeof p.hdfs?.rowsWritten === 'number') {
      hdfsParts.push(`rows: ${formatCount(p.hdfs.rowsWritten)}`);
    }

    const counter = hdfsParts.length > 0 ? hdfsParts.join(', ') : null;

    return {
      key: 'hdfs',
      label: stationLabels.hdfs,
      state,
      level,
      levelMeaning: 'Output written and verified',
      counter,
      working,
      note,
    };
  });

  const [sourceStation, kafkaStation, sparkStation, hdfsStation] = resolvedStations;

  // Pipes
  // 1. source to kafka
  let pipe0Flow = 0;
  if (kafkaStation.state === 'active') {
    const rps = p.throughput?.recordsPerSecond;
    if (ctx.peakRate > 0 && typeof rps === 'number' && !Number.isNaN(rps)) {
      pipe0Flow = clamp(rps / ctx.peakRate, 0.15, 1);
    } else {
      pipe0Flow = 0.5;
    }
  }

  // 2. kafka to spark
  const pipe1Flow = sparkStation.state === 'active' ? 0.5 : 0;

  // 3. spark to hdfs
  const pipe2Flow = hdfsStation.state === 'active' ? 0.5 : 0;

  const isUpstreamActiveOrDone = (s: StationModel) => s.state === 'active' || s.state === 'done';
  const isDownstreamFaultedOrError = (s: StationModel) => s.state === 'faulted' || s.state === 'error';

  const pipes: PipeModel[] = [
    {
      from: 'source',
      to: 'kafka',
      flow: pipe0Flow,
      blocked: isUpstreamActiveOrDone(sourceStation) && isDownstreamFaultedOrError(kafkaStation),
    },
    {
      from: 'kafka',
      to: 'spark',
      flow: pipe1Flow,
      blocked: isUpstreamActiveOrDone(kafkaStation) && isDownstreamFaultedOrError(sparkStation),
    },
    {
      from: 'spark',
      to: 'hdfs',
      flow: pipe2Flow,
      blocked: isUpstreamActiveOrDone(sparkStation) && isDownstreamFaultedOrError(hdfsStation),
    },
  ];

  // Completion mapping
  let completion: SceneModel['completion'] = 'running';
  if (p.mode === 'streaming' && p.status === 'running') {
    completion = 'live';
  } else if (p.status === 'succeeded' || p.status === 'failed' || p.status === 'cancelled') {
    completion = p.status;
  } else {
    completion = 'running';
  }

  // Summary generation (one sentence per station)
  const summary = resolvedStations.map((s) => {
    let details = '';
    if (s.state === 'active') {
      if (s.key === 'source') {
        details = s.counter ? ` ${s.counter} rows read.` : '.';
      } else if (s.key === 'kafka') {
        if (p.mode === 'batch' && typeof p.kafka?.published === 'number' && typeof p.source?.rowsTotal === 'number' && p.source.rowsTotal > 0) {
          const pct = Math.round((p.kafka.published / p.source.rowsTotal) * 100);
          details = ` ${formatCount(p.kafka.published)} records published (${pct}% of the total).`;
        } else if (s.counter) {
          details = ` ${s.counter}.`;
        } else {
          details = '.';
        }
      } else if (s.key === 'spark') {
        details = s.counter ? ` ${s.counter} so far.` : '.';
      } else if (s.key === 'hdfs') {
        details = s.counter ? ` ${s.counter}.` : '.';
      }
    } else if (s.state === 'done') {
      if (s.key === 'spark' && typeof p.spark?.recordsIn === 'number' && typeof p.spark?.recordsOut === 'number') {
        details = ` Processed ${formatCount(p.spark.recordsIn)} in, ${formatCount(p.spark.recordsOut)} out.`;
      } else if (s.key === 'hdfs' && p.hdfs?.verified) {
        details = ' Verified.';
      } else {
        details = '.';
      }
    } else if (s.state === 'pending') {
      details = '.';
    } else if (s.note) {
      details = ` ${s.note}.`;
    } else {
      details = '.';
    }

    return `${s.label}: ${s.state}${details}`;
  });

  // Announce: single sentence describing highest priority event:
  // error beats fault beats recovery beats completion beats stage start
  let announce = 'Pipeline run running.';

  const errorStation = resolvedStations.find((s) => s.state === 'error');
  const activeStation = resolvedStations.find((s) => s.state === 'active');

  if (p.status === 'failed' || errorStation) {
    if (errorStation) {
      announce = `${errorStation.label} failed: ${errorStation.note || 'error occurred'}.`;
    } else {
      announce = 'Pipeline run failed.';
    }
  } else if (p.fault && p.fault.phase === 'injected') {
    const targetKey = mapFaultTargetToStation(p.fault.target);
    const targetLabel = targetKey ? stationLabels[targetKey] : p.fault.target;
    announce = `Fault injected on ${targetLabel}: ${p.fault.type}.`;
  } else if (p.fault && (p.fault.phase === 'recovering' || p.fault.phase === 'recovered')) {
    const targetKey = mapFaultTargetToStation(p.fault.target);
    const targetLabel = targetKey ? stationLabels[targetKey] : p.fault.target;
    if (p.fault.phase === 'recovering') {
      announce = `${targetLabel} is recovering.`;
    } else {
      const sec = p.fault.recoverySeconds;
      const secText = typeof sec === 'number' && !Number.isNaN(sec) ? ` in ${sec} s` : '';
      announce = `${targetLabel} recovered${secText}.`;
    }
  } else if (p.status === 'succeeded') {
    announce = 'Pipeline run completed successfully.';
  } else if (p.status === 'cancelled') {
    announce = 'Pipeline run was cancelled.';
  } else if (activeStation) {
    announce = `${activeStation.label} processing started.`;
  } else if (resolvedStations.every((s) => s.state === 'done')) {
    announce = 'All pipeline stages completed.';
  } else if (resolvedStations.every((s) => s.state === 'pending')) {
    announce = 'Pipeline run pending.';
  }

  return {
    stations: resolvedStations,
    pipes,
    completion,
    announce,
    summary,
  };
}
