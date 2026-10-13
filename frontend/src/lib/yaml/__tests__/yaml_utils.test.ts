import { describe, it, expect } from 'vitest';
import {
  parseFieldPath,
  resolveYamlLine,
  yamlSyntaxIssues,
  summarizePipeline,
  validateUpload,
  validateUploadSync,
  safeFileName,
} from '../index';

describe('parseFieldPath', () => {
  it('parses dot separated paths', () => {
    expect(parseFieldPath('storage.path')).toEqual(['storage', 'path']);
  });

  it('parses array index notations like operations[2].condition', () => {
    expect(parseFieldPath('processing.operations[2].condition')).toEqual([
      'processing',
      'operations',
      2,
      'condition',
    ]);
  });

  it('parses numeric dot segments like schema.0.name', () => {
    expect(parseFieldPath('schema.0.name')).toEqual(['schema', 0, 'name']);
  });

  it('handles empty string and non-string inputs', () => {
    expect(parseFieldPath('')).toEqual([]);
    expect(parseFieldPath(null as unknown as string)).toEqual([]);
    expect(parseFieldPath(undefined as unknown as string)).toEqual([]);
  });

  it('handles odd inputs with consecutive dots and brackets', () => {
    expect(parseFieldPath('foo..bar')).toEqual(['foo', 'bar']);
    expect(parseFieldPath('[0]')).toEqual([0]);
    expect(parseFieldPath('items[1][2].name')).toEqual(['items', 1, 2, 'name']);
  });
});

describe('resolveYamlLine', () => {
  const sampleYaml = `pipeline:
  name: sales-pipeline
  version: "1.0"
source:
  type: csv
  path: /data/input.csv
schema:
  - name: id
    type: string
  - name: amount
    type: double
processing:
  operations:
    - type: filter
      condition: "amount > 0"
storage:
  type: hdfs
  path: /data/output
`;

  it('resolves top-level key', () => {
    const loc = resolveYamlLine(sampleYaml, 'source');
    expect(loc).not.toBeNull();
    expect(loc?.line).toBe(4);
  });

  it('resolves nested key', () => {
    const loc = resolveYamlLine(sampleYaml, 'source.path');
    expect(loc).not.toBeNull();
    expect(loc?.line).toBe(6);
  });

  it('resolves array item key', () => {
    const loc = resolveYamlLine(sampleYaml, 'schema.1.name');
    expect(loc).not.toBeNull();
    expect(loc?.line).toBe(10);
  });

  it('falls back to parent when key does not exist', () => {
    const loc = resolveYamlLine(sampleYaml, 'storage.nonexistent_field');
    expect(loc).not.toBeNull();
    // falls back to storage (line 16)
    expect(loc?.line).toBe(16);
  });

  it('handles empty document by falling back to line 1 without throwing', () => {
    const loc = resolveYamlLine('', 'storage.path');
    expect(loc?.line).toBe(1);
  });

  it('never throws on invalid YAML and returns fallback', () => {
    const brokenYaml = `::: invalid [[[ yaml :::`;
    const loc = resolveYamlLine(brokenYaml, 'storage.path');
    expect(loc).not.toBeNull();
    expect(loc?.line).toBe(1);
  });
});

describe('yamlSyntaxIssues', () => {
  it('detects a tab character in indentation', () => {
    const tabYaml = "pipeline:\n\tname: test";
    const issues = yamlSyntaxIssues(tabYaml);
    expect(issues.length).toBeGreaterThan(0);
    expect(issues[0].line).toBe(2);
    expect(issues[0].message).toMatch(/tab/i);
  });

  it('detects an unclosed quote with correct line number', () => {
    const unclosedYaml = 'pipeline:\n  name: "unclosed\nstorage:\n  path: /foo';
    const issues = yamlSyntaxIssues(unclosedYaml);
    expect(issues.length).toBeGreaterThan(0);
    expect(issues[0].line).toBeGreaterThanOrEqual(2);
  });

  it('returns empty array on valid YAML', () => {
    const valid = 'pipeline:\n  name: test\n';
    expect(yamlSyntaxIssues(valid)).toEqual([]);
  });
});

describe('summarizePipeline', () => {
  const completeYaml = `pipeline:
  name: sales-report
  version: "2.0"
source:
  type: csv
  path: /app/sample-data/sales.csv
schema:
  - { name: id, type: string }
  - { name: amount, type: double }
processing:
  operations:
    - { type: remove_nulls }
    - { type: filter }
messaging:
  broker: kafka
  topic: sales-topic
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/output
streaming:
  enabled: false
`;

  it('summarizes complete template', () => {
    const s = summarizePipeline(completeYaml);
    expect(s).not.toBeNull();
    expect(s?.name).toBe('sales-report');
    expect(s?.version).toBe('2.0');
    expect(s?.sourceType).toBe('csv');
    expect(s?.sourcePath).toBe('/app/sample-data/sales.csv');
    expect(s?.schemaFields).toBe(2);
    expect(s?.operations).toEqual(['remove_nulls', 'filter']);
    expect(s?.topic).toBe('sales-topic');
    expect(s?.engine).toBe('spark');
    expect(s?.storageType).toBe('hdfs');
    expect(s?.storagePath).toBe('/data/output');
    expect(s?.streaming).toBe(false);
  });

  it('summarizes partial config showing only what exists', () => {
    const partial = `pipeline:\n  name: minimal\nsource:\n  type: json\n`;
    const s = summarizePipeline(partial);
    expect(s).not.toBeNull();
    expect(s?.name).toBe('minimal');
    expect(s?.sourceType).toBe('json');
    expect(s?.schemaFields).toBeUndefined();
    expect(s?.storagePath).toBeUndefined();
  });

  it('returns null on garbage input', () => {
    expect(summarizePipeline('::: broken [[')).toBeNull();
    expect(summarizePipeline('')).toBeNull();
    expect(summarizePipeline('just a string')).toBeNull();
  });
});

describe('validateUpload and safeFileName', () => {
  it('rejects wrong extension', () => {
    const file = new File(['hello'], 'script.exe', { type: 'application/octet-stream' });
    const res = validateUploadSync(file);
    expect(res.ok).toBe(false);
    if (!res.ok) {
      expect(res.reason).toMatch(/yaml/i);
    }
  });

  it('rejects empty file', () => {
    const file = new File([], 'empty.yaml', { type: 'text/yaml' });
    const res = validateUploadSync(file);
    expect(res.ok).toBe(false);
    if (!res.ok) {
      expect(res.reason).toMatch(/empty/i);
    }
  });

  it('rejects 300 KB file (> 256 KB)', () => {
    const largeContent = new Uint8Array(300 * 1024);
    const file = new File([largeContent], 'large.yaml', { type: 'text/yaml' });
    const res = validateUploadSync(file);
    expect(res.ok).toBe(false);
    if (!res.ok) {
      expect(res.reason).toMatch(/256 KB/i);
    }
  });

  it('rejects NUL byte content', async () => {
    const file = new File(['pipeline:\x00evil: true'], 'nul.yaml', { type: 'text/yaml' });
    const res = await validateUpload(file);
    expect(res.ok).toBe(false);
    if (!res.ok) {
      expect(res.reason).toMatch(/binary|NUL/i);
    }
  });

  it('accepts valid small yaml file', async () => {
    const file = new File(['pipeline:\n  name: ok'], 'valid.yaml', { type: 'text/yaml' });
    const res = await validateUpload(file);
    expect(res.ok).toBe(true);
  });

  it('sanitizes safeFileName', () => {
    expect(safeFileName('sales-pipeline')).toBe('sales-pipeline.yaml');
    expect(safeFileName('sales_pipeline.yaml')).toBe('sales_pipeline.yaml');
    expect(safeFileName('../../My Cool Pipeline!!')).toBe('my-cool-pipeline.yaml');
    expect(safeFileName('...')).toBe('pipeline.yaml');
    expect(safeFileName('')).toBe('pipeline.yaml');
    expect(safeFileName(undefined)).toBe('pipeline.yaml');
  });
});
