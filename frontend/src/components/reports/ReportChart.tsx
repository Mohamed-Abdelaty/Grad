import React from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';
import { Report } from '../../types/report';

interface Props {
  reports: Report[];
}

export default function ReportChart({ reports }: Props) {
  if (reports.length === 0) return null;

  const data = reports.slice(-10).map((r) => ({
    name: r.experimentName,
    recovery: r.recoveryTimeSeconds ?? 0,
  }));


  return (
    <div className="h-72 w-full bg-slate-800 p-4 rounded-lg border border-slate-700 mb-6">
      <h3 className="text-sm text-slate-300 mb-4">Recovery Times (Recent)</h3>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" vertical={false} />
          <XAxis dataKey="name" stroke="#94a3b8" fontSize={11} tickFormatter={(val) => val.slice(0,8)+'...'} />
          <YAxis stroke="#94a3b8" fontSize={12} label={{ value: 'Seconds', angle: -90, position: 'insideLeft', fill: '#94a3b8' }} />
          <Tooltip 
            contentStyle={{ backgroundColor: '#1e293b', border: 'none', color: '#f8fafc' }}
            cursor={{ fill: '#334155', opacity: 0.4 }}
          />
          <Bar dataKey="recovery" fill="#818cf8" radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
