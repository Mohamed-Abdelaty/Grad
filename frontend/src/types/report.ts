// Raw shape returned by GET /api/reports (list endpoint).
export interface ReportListItem {
  report_id: string;
  title: string;
  created_at: string;
  // summary == Report.metadata_ — a dict of canonical metric fields.
  summary: Record<string, unknown>;
}

// Canonical frontend Report type — produced by mapToReport() in api/experiments.ts.
// All metric fields are nullable because not every experiment path populates every metric.
export interface Report {
  id: string;
  title: string;
  createdAt: string;
  experimentName: string;
  target: string | null;
  failureType: string | null;
  recoveryTimeSeconds: number | null;
  availabilityPercentage: number | null;
  throughputRecordsPerSec: number | null;
  dataLossPercentage: number | null;
  // Full metadata dict for charts that need arbitrary fields.
  summary: Record<string, unknown>;
}
