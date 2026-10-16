import React, { useState, useEffect, useMemo } from 'react';
import { useSearchParams, Link } from 'react-router-dom';
import {
  ArrowDownAZ,
  ArrowUpAZ,
  BarChart2,
  CheckCircle2,
  ExternalLink,
  Filter,
  Grid,
  List,
  RefreshCw,
  RotateCcw,
  XCircle,
} from 'lucide-react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from 'recharts';
import { listReports } from '../api/reports';
import { getOptions } from '../api/experiments';
import { ReportFilterParams, ReportListItem } from '../types/report';
import { ExperimentOptions } from '../types/experiment';
import { recoveryTrend } from '../lib/reliability';
import { ComparePanel } from '../components/reports/ComparePanel';

export default function Reports() {
  const [searchParams, setSearchParams] = useSearchParams();

  // Read URL query params
  const targetFilter = searchParams.get('target') || '';
  const faultFilter = searchParams.get('fault') || '';
  const resultFilter = (searchParams.get('result') || '') as '' | 'PASS' | 'FAIL';
  const fromFilter = searchParams.get('from') || '';
  const toFilter = searchParams.get('to') || '';
  const sortParam =
    (searchParams.get('sort') || 'created_desc') as
      | 'created_desc'
      | 'created_asc'
      | 'recovery_asc'
      | 'recovery_desc';

  // Local state
  const [options, setOptions] = useState<ExperimentOptions | null>(null);
  const [reports, setReports] = useState<ReportListItem[]>([]);
  const [totalCount, setTotalCount] = useState<number>(0);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  // View mode (Cards vs Table, persisted in localStorage)
  const [viewMode, setViewMode] = useState<'cards' | 'table'>(() => {
    return (localStorage.getItem('reports_view_mode') as 'cards' | 'table') || 'cards';
  });

  // Selected reports for comparison (up to 4)
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [compareOpen, setCompareOpen] = useState<boolean>(false);

  // Save viewMode preference
  const handleToggleView = (mode: 'cards' | 'table') => {
    setViewMode(mode);
    localStorage.setItem('reports_view_mode', mode);
  };

  // Load options once
  useEffect(() => {
    getOptions()
      .then(setOptions)
      .catch(() => {});
  }, []);

  // Update query params helper
  const updateQueryParam = (key: string, value: string) => {
    const updated = new URLSearchParams(searchParams);
    if (value) {
      updated.set(key, value);
    } else {
      updated.delete(key);
    }
    setSearchParams(updated, { replace: false });
  };

  // Reset all filters
  const handleResetFilters = () => {
    setSearchParams(new URLSearchParams(), { replace: false });
  };

  // Fetch reports when query params change
  useEffect(() => {
    let active = true;
    async function fetchReports() {
      setLoading(true);
      setError(null);
      try {
        const params: ReportFilterParams = { limit: 100 };
        if (targetFilter) params.target = targetFilter;
        if (faultFilter) params.fault = faultFilter;
        if (resultFilter) params.result = resultFilter;
        if (fromFilter) params.from = fromFilter;
        if (toFilter) params.to = toFilter;
        if (sortParam) params.sort = sortParam;

        const res = await listReports(params);
        if (!active) return;
        setReports(res.items);
        setTotalCount(res.total);
      } catch (err: unknown) {
        if (!active) return;
        setError(err instanceof Error ? err.message : 'Failed to load reports');
      } finally {
        if (active) setLoading(false);
      }
    }

    fetchReports();

    return () => {
      active = false;
    };
  }, [targetFilter, faultFilter, resultFilter, fromFilter, toFilter, sortParam]);

  // Handle checkbox selection for comparison
  const handleToggleSelect = (id: string) => {
    setSelectedIds((prev) => {
      if (prev.includes(id)) {
        return prev.filter((item) => item !== id);
      }
      if (prev.length >= 4) {
        return prev;
      }
      return [...prev, id];
    });
  };

  const selectedReports = useMemo(() => {
    return reports.filter((r) => selectedIds.includes(r.id));
  }, [reports, selectedIds]);

  // Metrics for Trends section
  const trendData = useMemo(() => recoveryTrend(reports), [reports]);

  const stats = useMemo(() => {
    if (reports.length === 0) {
      return { total: 0, passRate: '0%', avgRecovery: 'N/A' };
    }
    const passed = reports.filter((r) => r.result === 'PASS').length;
    const rate = ((passed / reports.length) * 100).toFixed(1);
    const recs = reports
      .map((r) => r.recoveryTimeSeconds)
      .filter((v): v is number => v !== null && v !== undefined);
    const avg =
      recs.length > 0 ? `${(recs.reduce((a, b) => a + b, 0) / recs.length).toFixed(1)} s` : 'N/A';

    return {
      total: reports.length,
      passRate: `${rate}% (${passed}/${reports.length})`,
      avgRecovery: avg,
    };
  }, [reports]);

  return (
    <div data-testid="page-reports" className="space-y-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2.5">
            <BarChart2 className="w-6 h-6 text-brand" />
            Reliability & Resilience Reports
          </h1>
          <p className="text-xs text-slate-400 mt-1">
            Historical audit logs, recovery trends, and side-by-side resilience metrics across all chaos experiments.
          </p>
        </div>

        {/* View Toggle */}
        <div className="flex items-center gap-2 bg-slate-900 border border-slate-800 p-1 rounded-lg">
          <button
            type="button"
            data-testid="view-mode-cards"
            onClick={() => handleToggleView('cards')}
            className={`p-1.5 rounded text-xs font-medium flex items-center gap-1.5 transition-colors ${
              viewMode === 'cards'
                ? 'bg-slate-800 text-slate-100 shadow-sm'
                : 'text-slate-400 hover:text-slate-200'
            }`}
            title="Card Grid View"
          >
            <Grid className="w-4 h-4" />
            <span className="hidden sm:inline">Cards</span>
          </button>
          <button
            type="button"
            data-testid="view-mode-table"
            onClick={() => handleToggleView('table')}
            className={`p-1.5 rounded text-xs font-medium flex items-center gap-1.5 transition-colors ${
              viewMode === 'table'
                ? 'bg-slate-800 text-slate-100 shadow-sm'
                : 'text-slate-400 hover:text-slate-200'
            }`}
            title="Table View"
          >
            <List className="w-4 h-4" />
            <span className="hidden sm:inline">Table</span>
          </button>
        </div>
      </div>

      {/* Trends Section */}
      <div
        data-testid="trends"
        className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-xl flex flex-col gap-5"
      >
        <div className="flex items-center justify-between border-b border-slate-800 pb-3">
          <h2 className="text-sm font-semibold text-slate-200 flex items-center gap-2">
            <BarChart2 className="w-4 h-4 text-brand-light" />
            Reliability Trends
          </h2>
          <span className="text-xs text-slate-400 font-mono">Total Reports: {totalCount}</span>
        </div>

        {/* Summary Metric Tiles */}
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          <div className="bg-slate-950/60 border border-slate-800 p-4 rounded-xl flex flex-col gap-1">
            <span className="text-xs text-slate-400 font-medium">Pass Rate</span>
            <span className="text-xl font-bold font-mono text-emerald-400">{stats.passRate}</span>
          </div>
          <div className="bg-slate-950/60 border border-slate-800 p-4 rounded-xl flex flex-col gap-1">
            <span className="text-xs text-slate-400 font-medium">Average Recovery Time</span>
            <span className="text-xl font-bold font-mono text-brand-light">{stats.avgRecovery}</span>
          </div>
          <div className="bg-slate-950/60 border border-slate-800 p-4 rounded-xl flex flex-col gap-1">
            <span className="text-xs text-slate-400 font-medium">Recorded Experiments</span>
            <span className="text-xl font-bold font-mono text-slate-100">{stats.total}</span>
          </div>
        </div>

        {/* Recovery Time Trend Line Chart */}
        {trendData && trendData.length > 1 && (
          <div className="flex flex-col gap-2 pt-2">
            <span className="text-xs text-slate-400 font-medium">
              Recovery Time History (seconds per run)
            </span>
            <div className="h-44 w-full bg-slate-950/40 p-2 rounded-xl border border-slate-800/80">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={trendData} margin={{ top: 10, right: 20, left: -20, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#334155" opacity={0.5} />
                  <XAxis dataKey="date" stroke="#64748b" tick={{ fontSize: 10 }} />
                  <YAxis stroke="#64748b" tick={{ fontSize: 10 }} unit="s" />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#0f172a',
                      borderColor: '#334155',
                      borderRadius: '0.5rem',
                      fontSize: '0.75rem',
                      color: '#f8fafc',
                    }}
                  />
                  <Line
                    type="monotone"
                    dataKey="recoveryTime"
                    name="Recovery Time"
                    stroke="#38bdf8"
                    strokeWidth={2}
                    dot={{ fill: '#38bdf8', r: 3 }}
                    activeDot={{ r: 5 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        )}
      </div>

      {/* Filter Bar */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-4 shadow-md flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-1.5 text-xs font-semibold text-slate-300 mr-1">
          <Filter className="w-3.5 h-3.5 text-brand-light" />
          <span>Filters:</span>
        </div>

        {/* Target Filter */}
        <select
          data-testid="filter-target"
          aria-label="Filter by Target"
          value={targetFilter}
          onChange={(e) => updateQueryParam('target', e.target.value)}
          className="bg-slate-800 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs text-slate-200 focus:outline-none focus:ring-1 focus:ring-brand"
        >
          <option value="">All Targets</option>
          {options?.targets.map((t) => (
            <option key={t.id} value={t.id}>
              {t.label}
            </option>
          ))}
        </select>

        {/* Fault Filter */}
        <select
          data-testid="filter-fault"
          aria-label="Filter by Fault"
          value={faultFilter}
          onChange={(e) => updateQueryParam('fault', e.target.value)}
          className="bg-slate-800 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs text-slate-200 focus:outline-none focus:ring-1 focus:ring-brand"
        >
          <option value="">All Faults</option>
          {options?.faults.map((f) => (
            <option key={f.id} value={f.id}>
              {f.label}
            </option>
          ))}
        </select>

        {/* Result Filter */}
        <select
          data-testid="filter-result"
          aria-label="Filter by Result"
          value={resultFilter}
          onChange={(e) => updateQueryParam('result', e.target.value)}
          className="bg-slate-800 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs text-slate-200 focus:outline-none focus:ring-1 focus:ring-brand"
        >
          <option value="">All Results</option>
          <option value="PASS">PASS</option>
          <option value="FAIL">FAIL</option>
        </select>

        {/* Date From */}
        <input
          type="date"
          aria-label="From Date"
          value={fromFilter}
          onChange={(e) => updateQueryParam('from', e.target.value)}
          className="bg-slate-800 border border-slate-700 rounded-lg px-2.5 py-1 text-xs text-slate-200 focus:outline-none focus:ring-1 focus:ring-brand"
        />

        {/* Date To */}
        <input
          type="date"
          aria-label="To Date"
          value={toFilter}
          onChange={(e) => updateQueryParam('to', e.target.value)}
          className="bg-slate-800 border border-slate-700 rounded-lg px-2.5 py-1 text-xs text-slate-200 focus:outline-none focus:ring-1 focus:ring-brand"
        />

        {/* Reset Filters */}
        {(targetFilter || faultFilter || resultFilter || fromFilter || toFilter || sortParam !== 'created_desc') && (
          <button
            type="button"
            onClick={handleResetFilters}
            className="text-xs px-2.5 py-1.5 rounded-lg bg-slate-800 border border-slate-700 hover:bg-slate-700 text-slate-300 flex items-center gap-1 transition-colors ml-auto"
          >
            <RotateCcw className="w-3 h-3" />
            Reset Filters
          </button>
        )}
      </div>

      {/* Reports Display (Cards vs Table) */}
      {loading ? (
        <div className="p-12 text-center text-slate-400 text-sm flex items-center justify-center gap-2">
          <RefreshCw className="w-4 h-4 animate-spin text-brand" />
          Loading reports...
        </div>
      ) : error ? (
        <div className="p-6 bg-rose-950/60 border border-rose-800 rounded-xl text-rose-300 text-sm">
          Failed to load reports: {error}
        </div>
      ) : reports.length === 0 ? (
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-12 text-center text-slate-400 text-sm">
          No reports match the current filter criteria. Run an experiment or reset your filters.
        </div>
      ) : viewMode === 'cards' ? (
        /* Cards View */
        <div data-testid="reports-cards" className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
          {reports.map((report) => {
            const isChecked = selectedIds.includes(report.id);
            return (
              <div
                key={report.id}
                className={`bg-slate-900 border rounded-xl p-5 shadow-lg flex flex-col justify-between gap-4 transition-all relative ${
                  isChecked
                    ? 'border-brand ring-1 ring-brand bg-brand/5'
                    : 'border-slate-800 hover:border-slate-700 hover:bg-slate-900/90'
                }`}
              >
                {/* Header row */}
                <div className="flex items-start justify-between gap-3">
                  <div className="flex items-center gap-2.5">
                    <input
                      type="checkbox"
                      data-testid={`cmp-${report.id}`}
                      checked={isChecked}
                      onChange={() => handleToggleSelect(report.id)}
                      className="w-4 h-4 rounded border-slate-700 text-brand focus:ring-brand accent-brand cursor-pointer"
                      title="Select for comparison"
                      aria-label="Select report for comparison"
                    />
                    <div>
                      <h3 className="font-bold text-sm text-slate-100 font-mono truncate max-w-[180px]">
                        {report.title || report.id}
                      </h3>
                      <span className="text-[11px] text-slate-400">
                        {report.createdAt ? new Date(report.createdAt).toLocaleDateString() : ''}
                      </span>
                    </div>
                  </div>

                  {/* Verdict Badge */}
                  {report.result === 'PASS' ? (
                    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-bold bg-emerald-950 text-emerald-300 border border-emerald-800">
                      <CheckCircle2 className="w-3.5 h-3.5" /> PASS
                    </span>
                  ) : report.result === 'FAIL' ? (
                    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-bold bg-rose-950 text-rose-300 border border-rose-800">
                      <XCircle className="w-3.5 h-3.5" /> FAIL
                    </span>
                  ) : (
                    <span className="px-2 py-0.5 rounded text-[11px] bg-slate-800 text-slate-400">
                      INCOMPLETE
                    </span>
                  )}
                </div>

                {/* Target & Fault */}
                <div className="flex flex-wrap gap-2 text-xs">
                  <span className="bg-slate-800 px-2 py-0.5 rounded text-slate-300">
                    Target: {report.targetService || 'N/A'}
                  </span>
                  <span className="bg-slate-800 px-2 py-0.5 rounded text-slate-300">
                    Fault: {report.faultType || 'N/A'}
                  </span>
                </div>

                {/* Metrics Grid */}
                <div className="grid grid-cols-2 gap-2 bg-slate-950/60 p-3 rounded-lg border border-slate-800/80 text-xs font-mono">
                  <div>
                    <span className="text-[10px] text-slate-400 block font-sans">Recovery Time</span>
                    <span className="font-bold text-slate-100">
                      {report.recoveryTimeSeconds !== null ? `${report.recoveryTimeSeconds.toFixed(1)} s` : 'N/A'}
                    </span>
                  </div>
                  <div>
                    <span className="text-[10px] text-slate-400 block font-sans">Data Loss</span>
                    <span className="font-bold text-slate-100">{report.dataLoss !== null && report.dataLoss !== undefined ? `${report.dataLoss} rec` : 'N/A'}</span>
                  </div>
                  <div>
                    <span className="text-[10px] text-slate-400 block font-sans">Availability during fault window</span>
                    <span className="font-bold text-slate-100">
                      {report.availabilityPercentage !== null ? `${report.availabilityPercentage.toFixed(1)}%` : 'N/A'}
                    </span>
                  </div>
                  <div>
                    <span className="text-[10px] text-slate-400 block font-sans">Error Rate</span>
                    <span className="font-bold text-slate-100">
                      {report.errorRate !== null ? `${(report.errorRate * 100).toFixed(1)}%` : 'N/A'}
                    </span>
                  </div>
                </div>

                {/* Link */}
                <Link
                  to={`/reports/${report.id}`}
                  className="mt-1 w-full py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-semibold rounded-lg flex items-center justify-center gap-1.5 transition-colors border border-slate-700"
                >
                  View Report Details
                  <ExternalLink className="w-3.5 h-3.5" />
                </Link>
              </div>
            );
          })}
        </div>
      ) : (
        /* Table View */
        <div data-testid="reports-table" className="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden shadow-xl">
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs border-collapse">
              <thead className="bg-slate-950 text-slate-400 uppercase font-semibold text-[11px] border-b border-slate-800">
                <tr>
                  <th className="py-3 px-4 w-12 text-center">Cmp</th>
                  <th className="py-3 px-4">Verdict</th>
                  <th
                    data-testid="sort-date"
                    onClick={() =>
                      updateQueryParam(
                        'sort',
                        sortParam === 'created_desc' ? 'created_asc' : 'created_desc'
                      )
                    }
                    className="py-3 px-4 cursor-pointer hover:text-slate-100 select-none"
                  >
                    <div className="flex items-center gap-1">
                      <span>Date / Name</span>
                      {sortParam === 'created_desc' ? (
                        <ArrowDownAZ className="w-3 h-3 text-brand-light" />
                      ) : (
                        <ArrowUpAZ className="w-3 h-3 text-brand-light" />
                      )}
                    </div>
                  </th>
                  <th className="py-3 px-4">Target</th>
                  <th className="py-3 px-4">Fault</th>
                  <th
                    data-testid="sort-recovery"
                    onClick={() =>
                      updateQueryParam(
                        'sort',
                        sortParam === 'recovery_asc' ? 'recovery_desc' : 'recovery_asc'
                      )
                    }
                    className="py-3 px-4 cursor-pointer hover:text-slate-100 select-none"
                  >
                    <div className="flex items-center gap-1">
                      <span>Recovery Time</span>
                      {sortParam === 'recovery_asc' ? (
                        <ArrowUpAZ className="w-3 h-3 text-brand-light" />
                      ) : (
                        <ArrowDownAZ className="w-3 h-3 text-brand-light" />
                      )}
                    </div>
                  </th>
                  <th className="py-3 px-4">Data Loss</th>
                  <th className="py-3 px-4">Availability during fault window</th>
                  <th className="py-3 px-4">Action</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/80 font-mono text-slate-200">
                {reports.map((report) => {
                  const isChecked = selectedIds.includes(report.id);
                  return (
                    <tr
                      key={report.id}
                      className={`hover:bg-slate-800/40 transition-colors ${
                        isChecked ? 'bg-brand/5' : ''
                      }`}
                    >
                      <td className="py-2.5 px-4 text-center">
                        <input
                          type="checkbox"
                          data-testid={`cmp-${report.id}`}
                          checked={isChecked}
                          onChange={() => handleToggleSelect(report.id)}
                          className="w-4 h-4 rounded border-slate-700 text-brand focus:ring-brand accent-brand cursor-pointer"
                          aria-label="Select report for comparison"
                        />
                      </td>
                      <td className="py-2.5 px-4">
                        {report.result === 'PASS' ? (
                          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold bg-emerald-950 text-emerald-300 border border-emerald-800 font-sans">
                            <CheckCircle2 className="w-3 h-3" /> PASS
                          </span>
                        ) : report.result === 'FAIL' ? (
                          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold bg-rose-950 text-rose-300 border border-rose-800 font-sans">
                            <XCircle className="w-3 h-3" /> FAIL
                          </span>
                        ) : (
                          <span className="px-2 py-0.5 rounded text-[10px] bg-slate-800 text-slate-400 font-sans">
                            INCOMPLETE
                          </span>
                        )}
                      </td>
                      <td className="py-2.5 px-4 font-sans">
                        <div className="flex flex-col">
                          <span className="font-semibold text-slate-100 font-mono">{report.title || report.id}</span>
                          <span className="text-[10px] text-slate-400">
                            {report.createdAt ? new Date(report.createdAt).toLocaleDateString() : ''}
                          </span>
                        </div>
                      </td>
                      <td className="py-2.5 px-4 font-sans">{report.targetService || 'N/A'}</td>
                      <td className="py-2.5 px-4 font-sans">{report.faultType || 'N/A'}</td>
                      <td className="py-2.5 px-4 font-bold text-slate-100">
                        {report.recoveryTimeSeconds !== null
                          ? `${report.recoveryTimeSeconds.toFixed(1)} s`
                          : 'N/A'}
                      </td>
                      <td className="py-2.5 px-4">{report.dataLoss !== null && report.dataLoss !== undefined ? report.dataLoss : 'N/A'}</td>
                      <td className="py-2.5 px-4">
                        {report.availabilityPercentage !== null
                          ? `${report.availabilityPercentage.toFixed(1)}%`
                          : 'N/A'}
                      </td>
                      <td className="py-2.5 px-4 font-sans">
                        <Link
                          to={`/reports/${report.id}`}
                          className="text-brand hover:underline flex items-center gap-1 text-xs font-semibold"
                        >
                          View <ExternalLink className="w-3 h-3" />
                        </Link>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Floating Compare Action Bar */}
      {selectedIds.length > 0 && (
        <div data-testid="compare-selection-bar" className="fixed bottom-6 left-1/2 -translate-x-1/2 z-40 bg-slate-900 border border-brand/60 shadow-2xl rounded-2xl px-5 py-3 flex items-center gap-4 text-xs font-semibold text-slate-100 backdrop-blur-md animate-in slide-in-from-bottom-6">
          <span className="text-brand-light font-mono">
            {selectedIds.length} {selectedIds.length === 1 ? 'report' : 'reports'} selected
          </span>
          <span className="text-slate-600">|</span>

          <button
            type="button"
            data-testid="btn-compare-now"
            onClick={() => setCompareOpen(true)}
            disabled={selectedIds.length < 2}
            className={`px-4 py-1.5 rounded-lg text-xs font-bold transition-all shadow-md ${
              selectedIds.length >= 2
                ? 'bg-brand hover:bg-brand-dark text-white'
                : 'bg-slate-800 text-slate-500 cursor-not-allowed'
            }`}
          >
            Compare ({selectedIds.length})
          </button>

          <button
            type="button"
            onClick={() => setSelectedIds([])}
            className="text-slate-400 hover:text-slate-200 transition-colors"
          >
            Clear selection
          </button>
        </div>
      )}

      {/* Compare Modal Panel */}
      <ComparePanel
        open={compareOpen}
        onClose={() => setCompareOpen(false)}
        reports={selectedReports}
      />
    </div>
  );
}
