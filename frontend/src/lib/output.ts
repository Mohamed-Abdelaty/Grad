/**
 * frontend/src/lib/output.ts
 * --------------------------
 * Formatting, alignment, CSV serialization, and size threshold helpers for UI-6 output inspection.
 */

import { OutputColumnType, OutputPreview } from '../types/output';

export function isNumericType(type: OutputColumnType): boolean {
  return type === 'int32' || type === 'int64' || type === 'double' || type === 'decimal';
}

export function columnAlignment(type: OutputColumnType): 'left' | 'right' {
  return isNumericType(type) ? 'right' : 'left';
}

export function formatCell(value: unknown, type: OutputColumnType): string {
  if (value === null || value === undefined) {
    return '—';
  }

  if (type === 'bool' || typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }

  if (typeof value === 'number') {
    if (Number.isNaN(value) || !Number.isFinite(value)) {
      return '—';
    }
    return value.toLocaleString('en-US');
  }

  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }

  return String(value);
}

function escapeCsvField(val: string): string {
  if (val.includes('"') || val.includes(',') || val.includes('\n') || val.includes('\r')) {
    return `"${val.replace(/"/g, '""')}"`;
  }
  return val;
}

export function previewToCsvText(preview: OutputPreview): string {
  const header = preview.columns.map((c) => escapeCsvField(c.name)).join(',');
  const rows = preview.rows.map((row) => {
    return preview.columns
      .map((col) => {
        const raw = row[col.name];
        const formatted = raw === null || raw === undefined ? '' : typeof raw === 'object' ? JSON.stringify(raw) : String(raw);
        return escapeCsvField(formatted);
      })
      .join(',');
  });

  return [header, ...rows].join('\r\n');
}

export function downloadSizeWarning(sizeBytes: number): boolean {
  // Threshold: 100 MB
  return sizeBytes > 100 * 1024 * 1024;
}
