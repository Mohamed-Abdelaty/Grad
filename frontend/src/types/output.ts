/**
 * frontend/src/types/output.ts
 * ----------------------------
 * Type definitions for UI-6 output inspection, preview, and downloads.
 */

export type OutputColumnType =
  | 'string'
  | 'int32'
  | 'int64'
  | 'double'
  | 'bool'
  | 'timestamp'
  | 'date'
  | 'decimal'
  | 'other';

export interface OutputColumn {
  name: string;
  type: OutputColumnType;
}

export interface OutputFile {
  name: string;
  sizeBytes: number;
}

export interface OutputMetadata {
  format: 'parquet';
  path: string;
  fileCount: number;
  totalSizeBytes: number;
  rowCount: number | null;
  verified: boolean;
  columns: OutputColumn[];
  files: OutputFile[];
}

export interface OutputPreview {
  columns: OutputColumn[];
  rows: Record<string, unknown>[];
  limit: number;
  totalRows: number | null;
  truncated: boolean;
}

export interface DownloadLink {
  url: string;
  expiresAt: string;
  sizeBytes: number;
  format: 'csv' | 'parquet';
}
