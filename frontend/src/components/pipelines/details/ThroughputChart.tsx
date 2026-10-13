import React, { useMemo } from 'react';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import { EmptyState } from '../../ui/EmptyState';
import { formatRate } from '../../../lib/format';

interface ThroughputChartProps {
  samples: { t: number; v: number }[];
}

export const ThroughputChart = React.memo(function ThroughputChart({
  samples,
}: ThroughputChartProps) {
  const chartData = useMemo(() => {
    if (!samples || samples.length === 0) return [];
    const firstT = samples[0].t;
    return samples.map((s) => ({
      relTime: Math.max(0, Math.round(s.t - firstT)),
      rate: s.v,
    }));
  }, [samples]);

  if (!samples || samples.length === 0) {
    return (
      <div
        data-testid="throughput-chart"
        className="p-6 bg-surface-card border border-surface-border rounded-xl min-h-[260px] flex items-center justify-center"
      >
        <EmptyState
          title="No throughput samples yet"
          description="No throughput samples yet. Throughput is measured while records are published to Kafka."
        />
      </div>
    );
  }

  return (
    <div
      data-testid="throughput-chart"
      className="p-4 bg-surface-card border border-surface-border rounded-xl space-y-3"
    >
      <div className="flex items-center justify-between text-xs text-slate-400">
        <span className="font-semibold text-slate-200">Throughput History</span>
        <span className="font-mono tabular-nums">{samples.length} samples</span>
      </div>

      <div className="w-full h-[220px]">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart
            data={chartData}
            margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
          >
            <defs>
              <linearGradient id="rateGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#06b6d4" stopOpacity={0.4} />
                <stop offset="95%" stopColor="#06b6d4" stopOpacity={0.0} />
              </linearGradient>
            </defs>
            <XAxis
              dataKey="relTime"
              stroke="#64748b"
              fontSize={11}
              tickLine={false}
              unit="s"
            />
            <YAxis
              stroke="#64748b"
              fontSize={11}
              tickLine={false}
              axisLine={false}
              tickFormatter={(v) => (v >= 1000 ? `${(v / 1000).toFixed(1)}k` : `${v}`)}
            />
            <Tooltip
              content={({ active, payload }) => {
                if (active && payload && payload.length) {
                  const data = payload[0].payload as { relTime: number; rate: number };
                  return (
                    <div className="bg-slate-900 border border-surface-border p-2 rounded shadow-lg text-xs font-mono space-y-0.5">
                      <div className="text-slate-400">T + {data.relTime}s</div>
                      <div className="text-brand-light font-bold">
                        {formatRate(data.rate)}
                      </div>
                    </div>
                  );
                }
                return null;
              }}
            />
            <Area
              type="monotone"
              dataKey="rate"
              stroke="#06b6d4"
              strokeWidth={2}
              fill="url(#rateGradient)"
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});
