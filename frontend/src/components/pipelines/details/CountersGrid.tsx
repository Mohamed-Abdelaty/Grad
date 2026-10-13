import React from 'react';
import { RunProgress } from '../../../types/runProgress';
import {
  formatCount,
  formatBytes,
  formatRate,
  formatDuration,
} from '../../../lib/format';
import { useAnimatedNumber } from '../../../hooks/useAnimatedNumber';
import { Tooltip } from '../../ui/Tooltip';

interface CountersGridProps {
  progress: RunProgress | null;
  elapsedMs: number;
}

interface CounterTileProps {
  testId: string;
  label: string;
  value: number | null;
  formatter?: (n: number | null) => string;
  subtitle?: React.ReactNode;
}

function CounterTile({
  testId,
  label,
  value,
  formatter = formatCount,
  subtitle,
}: CounterTileProps) {
  // If value is null, never animate and display em dash
  const animatedValue = useAnimatedNumber(value);

  return (
    <div
      data-testid={testId}
      className="p-3 bg-surface-card border border-surface-border rounded-xl flex flex-col justify-between"
    >
      <div className="text-xs text-slate-400 font-medium">{label}</div>
      <div className="mt-1 flex items-baseline gap-1">
        {value === null ? (
          <Tooltip content="Not measured yet">
            <span className="font-mono text-lg text-slate-500 cursor-default select-none">
              —
            </span>
          </Tooltip>
        ) : (
          <span className="font-mono text-lg font-bold text-slate-100 tabular-nums">
            {formatter(animatedValue)}
          </span>
        )}
      </div>
      {subtitle && <div className="mt-1 text-[11px] text-slate-400">{subtitle}</div>}
    </div>
  );
}

export const CountersGrid = React.memo(function CountersGrid({
  progress,
  elapsedMs,
}: CountersGridProps) {
  const source = progress?.source;
  const kafka = progress?.kafka;
  const spark = progress?.spark;
  const hdfs = progress?.hdfs;
  const throughput = progress?.throughput;
  const errors = progress?.errors ?? null;

  return (
    <div
      data-testid="counters"
      aria-label="Run counters"
      className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3 min-h-[180px]"
    >
      {/* 1. Rows total */}
      <CounterTile
        testId="counter-rows-total"
        label="Rows Total"
        value={source?.rowsTotal ?? null}
      />

      {/* 2. Rows read */}
      <CounterTile
        testId="counter-rows-read"
        label="Rows Read"
        value={source?.rowsRead ?? null}
      />

      {/* 3. Published to Kafka */}
      <CounterTile
        testId="counter-published"
        label="Published to Kafka"
        value={kafka?.published ?? null}
        subtitle={
          kafka && kafka.failed !== null && kafka.failed > 0 ? (
            <span className="text-status-error font-medium">
              failed: {formatCount(kafka.failed)}
            </span>
          ) : undefined
        }
      />

      {/* 4. Spark records in */}
      <CounterTile
        testId="counter-records-in"
        label="Spark Records In"
        value={spark?.recordsIn ?? null}
      />

      {/* 5. Spark records out */}
      <CounterTile
        testId="counter-records-out"
        label="Spark Records Out"
        value={spark?.recordsOut ?? null}
      />

      {/* 6. Rows written to HDFS */}
      <CounterTile
        testId="counter-rows-written"
        label="Rows Written (HDFS)"
        value={hdfs?.rowsWritten ?? null}
      />

      {/* 7. Files written */}
      <CounterTile
        testId="counter-files-written"
        label="Files Written"
        value={hdfs?.filesWritten ?? null}
      />

      {/* 8. Bytes written */}
      <CounterTile
        testId="counter-bytes-written"
        label="Bytes Written"
        value={hdfs?.bytesWritten ?? null}
        formatter={formatBytes}
      />

      {/* 9. Errors */}
      <CounterTile
        testId="counter-errors"
        label="Errors"
        value={errors}
        subtitle={
          errors !== null && errors > 0 ? (
            <span className="text-status-error font-medium">Attention needed</span>
          ) : undefined
        }
      />

      {/* 10. Records per second */}
      <CounterTile
        testId="counter-rate"
        label="Throughput"
        value={throughput?.recordsPerSecond ?? null}
        formatter={formatRate}
      />

      {/* 11. Elapsed */}
      <div
        data-testid="counter-elapsed"
        className="p-3 bg-surface-card border border-surface-border rounded-xl flex flex-col justify-between"
      >
        <div className="text-xs text-slate-400 font-medium">Elapsed</div>
        <div className="mt-1 flex items-baseline gap-1">
          <span className="font-mono text-lg font-bold text-slate-100 tabular-nums">
            {formatDuration(elapsedMs)}
          </span>
        </div>
      </div>
    </div>
  );
});
