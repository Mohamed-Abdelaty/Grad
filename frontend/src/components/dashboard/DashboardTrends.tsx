import React from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  Legend,
} from 'recharts';
import { RecoveryTrendItem, ThroughputTrendItem } from '../../api/dashboard';
import { Card, CardHeader, CardTitle } from '../ui/Card';
import { EmptyState } from '../ui/EmptyState';

interface DashboardTrendsProps {
  recoveryTrend: RecoveryTrendItem[];
  throughputTrend: ThroughputTrendItem[];
}

export function DashboardTrends({ recoveryTrend, throughputTrend }: DashboardTrendsProps) {
  const hasRecovery = recoveryTrend && recoveryTrend.length > 0;
  const hasThroughput = throughputTrend && throughputTrend.length > 0;

  if (!hasRecovery && !hasThroughput) {
    return (
      <Card data-testid="dash-trends">
        <CardHeader>
          <CardTitle>Reliability & Performance Trends</CardTitle>
        </CardHeader>
        <EmptyState
          title="No trend data yet"
          description="Complete fault-injection experiments to view recovery time and throughput trends."
        />
      </Card>
    );
  }

  const recoveryData = recoveryTrend.map((item) => ({
    name: `Exp ${item.experimentId.slice(0, 6)}`,
    recoverySeconds: item.recoverySeconds,
    result: item.result,
  }));

  const throughputData = throughputTrend.map((item) => ({
    name: `Exp ${item.experimentId.slice(0, 6)}`,
    baseline: item.baseline ?? 0,
    during: item.during ?? 0,
  }));

  return (
    <div data-testid="dash-trends" className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      {/* Recovery Time Trend */}
      <Card>
        <CardHeader>
          <CardTitle>Recovery Time Trend (Last 10 Experiments)</CardTitle>
        </CardHeader>
        {hasRecovery ? (
          <div className="h-64 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={recoveryData} margin={{ top: 10, right: 20, left: 0, bottom: 20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" vertical={false} />
                <XAxis dataKey="name" stroke="#94a3b8" fontSize={11} />
                <YAxis
                  stroke="#94a3b8"
                  fontSize={11}
                  label={{ value: 'Seconds', angle: -90, position: 'insideLeft', fill: '#94a3b8' }}
                />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1e293b', borderColor: '#334155', color: '#f8fafc' }}
                  cursor={{ fill: '#334155', opacity: 0.3 }}
                />
                <Bar
                  dataKey="recoverySeconds"
                  name="Recovery Time (s)"
                  fill="#6366f1"
                  radius={[4, 4, 0, 0]}
                  isAnimationActive={false}
                />
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <EmptyState title="No recovery data" description="No completed experiments with recovery time." />
        )}
      </Card>

      {/* Throughput Trend */}
      <Card>
        <CardHeader>
          <CardTitle>Throughput During Fault (Last 10 Experiments)</CardTitle>
        </CardHeader>
        {hasThroughput ? (
          <div className="h-64 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={throughputData} margin={{ top: 10, right: 20, left: 0, bottom: 20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" vertical={false} />
                <XAxis dataKey="name" stroke="#94a3b8" fontSize={11} />
                <YAxis
                  stroke="#94a3b8"
                  fontSize={11}
                  label={{ value: 'Rec/s', angle: -90, position: 'insideLeft', fill: '#94a3b8' }}
                />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1e293b', borderColor: '#334155', color: '#f8fafc' }}
                  cursor={{ fill: '#334155', opacity: 0.3 }}
                />
                <Legend />
                <Bar
                  dataKey="baseline"
                  name="Baseline Throughput"
                  fill="#38bdf8"
                  radius={[4, 4, 0, 0]}
                  isAnimationActive={false}
                />
                <Bar
                  dataKey="during"
                  name="During Fault"
                  fill="#f59e0b"
                  radius={[4, 4, 0, 0]}
                  isAnimationActive={false}
                />
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <EmptyState title="No throughput data" description="No completed experiments with throughput metrics." />
        )}
      </Card>
    </div>
  );
}

export default DashboardTrends;
