/**
 * frontend/src/__tests__/outputLib.test.ts
 * ---------------------------------------
 * Unit tests for output library formatting, alignment, CSV generation, and error mapping.
 */

import { describe, it, expect } from 'vitest';
import axios from 'axios';
import {
  columnAlignment,
  downloadSizeWarning,
  formatCell,
  isNumericType,
  previewToCsvText,
} from '../lib/output';
import { toOutputError } from '../api/output';
import { OutputColumnType, OutputPreview } from '../types/output';

describe('output helpers (lib/output.ts)', () => {
  it('isNumericType returns true only for numeric types', () => {
    const numericTypes: OutputColumnType[] = ['int32', 'int64', 'double', 'decimal'];
    const nonNumericTypes: OutputColumnType[] = ['string', 'bool', 'timestamp', 'date', 'other'];

    numericTypes.forEach((t) => expect(isNumericType(t)).toBe(true));
    nonNumericTypes.forEach((t) => expect(isNumericType(t)).toBe(false));
  });

  it('columnAlignment aligns numeric columns right and others left', () => {
    expect(columnAlignment('int32')).toBe('right');
    expect(columnAlignment('double')).toBe('right');
    expect(columnAlignment('decimal')).toBe('right');
    expect(columnAlignment('string')).toBe('left');
    expect(columnAlignment('bool')).toBe('left');
    expect(columnAlignment('timestamp')).toBe('left');
  });

  it('formatCell correctly formats various data types', () => {
    // null / undefined
    expect(formatCell(null, 'string')).toBe('—');
    expect(formatCell(undefined, 'int32')).toBe('—');

    // booleans
    expect(formatCell(true, 'bool')).toBe('true');
    expect(formatCell(false, 'bool')).toBe('false');

    // numbers & floats
    expect(formatCell(1234567, 'int64')).toBe('1,234,567');
    expect(formatCell(99.5, 'double')).toBe('99.5');
    expect(formatCell(NaN, 'double')).toBe('—');
    expect(formatCell(Infinity, 'double')).toBe('—');

    // objects & json
    expect(formatCell({ a: 1 }, 'other')).toBe('{"a":1}');

    // strings
    expect(formatCell('hello world', 'string')).toBe('hello world');
  });

  it('previewToCsvText generates RFC 4180 compliant CSV', () => {
    const preview: OutputPreview = {
      columns: [
        { name: 'id', type: 'string' },
        { name: 'amount', type: 'double' },
        { name: 'notes', type: 'string' },
      ],
      rows: [
        { id: '1', amount: 100.5, notes: 'Simple' },
        { id: '2', amount: 200, notes: 'Contains, comma' },
        { id: '3', amount: 300, notes: 'Contains "quotes"' },
        { id: '4', amount: null, notes: 'Line\nBreak' },
      ],
      limit: 50,
      totalRows: 4,
      truncated: false,
    };

    const csv = previewToCsvText(preview);
    const lines = csv.split('\r\n');

    expect(lines[0]).toBe('id,amount,notes');
    expect(lines[1]).toBe('1,100.5,Simple');
    expect(lines[2]).toBe('2,200,"Contains, comma"');
    expect(lines[3]).toBe('3,300,"Contains ""quotes"""');
    expect(lines[4]).toContain('"Line\nBreak"');
  });

  it('downloadSizeWarning triggers only above 100 MB', () => {
    expect(downloadSizeWarning(50 * 1024 * 1024)).toBe(false);
    expect(downloadSizeWarning(100 * 1024 * 1024)).toBe(false);
    expect(downloadSizeWarning(100 * 1024 * 1024 + 1)).toBe(true);
    expect(downloadSizeWarning(300 * 1024 * 1024)).toBe(true);
  });
});

describe('toOutputError mapping', () => {
  it('maps 404, 409, 413, and 503 HTTP status codes', () => {
    const makeAxiosError = (status: number, detail?: string) => {
      const err = new axios.AxiosError('Request failed');
      err.response = {
        status,
        data: { detail },
        statusText: 'Error',
        headers: {},
        config: {} as any,
      };
      return err;
    };

    expect(toOutputError(makeAxiosError(404))).toBe('No output found for this run');
    expect(toOutputError(makeAxiosError(409))).toBe(
      'Output is not available until the run completes successfully'
    );
    expect(toOutputError(makeAxiosError(413))).toBe(
      'Output exceeds the maximum downloadable size (500 MB). Read it from HDFS directly.'
    );
    expect(
      toOutputError(makeAxiosError(413, 'Output is 600 MB; maximum downloadable size is 500 MB.'))
    ).toBe('Output is 600 MB; maximum downloadable size is 500 MB.');
    expect(toOutputError(makeAxiosError(503))).toBe('Storage service is temporarily unavailable');
    expect(toOutputError(makeAxiosError(400, 'Custom error message'))).toBe('Custom error message');
    expect(toOutputError(new Error('Network offline'))).toBe('Network offline');
  });
});
