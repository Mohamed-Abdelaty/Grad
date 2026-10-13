import { LineCounter, parseDocument, isNode } from 'yaml';

export interface FieldLocation {
  line: number;
  from: number;
  to: number;
}

export interface SyntaxIssue {
  line: number;
  from: number;
  to: number;
  message: string;
}

export interface PipelineSummary {
  name?: string;
  version?: string;
  sourceType?: string;
  sourcePath?: string;
  schemaFields?: number;
  operations?: string[];
  topic?: string;
  engine?: string;
  storageType?: string;
  storagePath?: string;
  streaming?: boolean;
}

export type UploadValidationResult = { ok: true } | { ok: false; reason: string };

/**
 * Converts backend field paths such as storage.path, processing.operations[2].condition,
 * schema.0.name into tokens (dot segments, [n] and numeric segments become numbers).
 */
export function parseFieldPath(field: string): (string | number)[] {
  if (!field || typeof field !== 'string') return [];
  const normalized = field.replace(/\[(\d+)\]/g, '.$1');
  const rawParts = normalized.split('.').filter(Boolean);
  return rawParts.map((part) => {
    if (/^\d+$/.test(part)) {
      return parseInt(part, 10);
    }
    return part;
  });
}

/**
 * Resolves the 1-based line and character offsets for a given field path in a YAML document.
 * Falls back to the nearest existing ancestor, then line 1. Never throws.
 */
export function resolveYamlLine(text: string, field: string): FieldLocation | null {
  if (!text || typeof text !== 'string') {
    return { line: 1, from: 0, to: 0 };
  }
  try {
    const lineCounter = new LineCounter();
    const doc = parseDocument(text, { lineCounter });
    const tokens = parseFieldPath(field);

    let current: any = doc.contents;
    let bestRange: [number, number, number] | [number, number] | null = current ? current.range : null;

    for (const token of tokens) {
      if (!current) break;

      if (Array.isArray(current.items)) {
        if (typeof token === 'number') {
          const item = current.items[token];
          if (!item) break;
          bestRange = item.range || bestRange;
          current = item;
        } else {
          const pair = current.items.find(
            (p: any) => p && p.key && (p.key.value === token || String(p.key) === token)
          );
          if (!pair) break;
          bestRange = pair.key && pair.key.range ? pair.key.range : pair.range || bestRange;
          current = pair.value;
        }
      } else {
        break;
      }
    }

    if (!bestRange) {
      return { line: 1, from: 0, to: 0 };
    }

    const from = bestRange[0] ?? 0;
    const to = bestRange[1] ?? from;
    const linePos = lineCounter.linePos(from);

    return {
      line: linePos?.line ?? 1,
      from,
      to,
    };
  } catch {
    return { line: 1, from: 0, to: 0 };
  }
}

/**
 * Returns syntax errors from parsing the YAML document.
 */
export function yamlSyntaxIssues(text: string): SyntaxIssue[] {
  if (!text || typeof text !== 'string') return [];
  try {
    const lineCounter = new LineCounter();
    const doc = parseDocument(text, { lineCounter });
    if (!doc.errors || doc.errors.length === 0) return [];

    return doc.errors.map((err) => {
      let line = 1;
      let from = 0;
      let to = 0;
      if (err.linePos && err.linePos[0]) {
        line = err.linePos[0].line;
      }
      if (err.pos && Array.isArray(err.pos)) {
        from = err.pos[0] ?? 0;
        to = err.pos[1] ?? from;
      }
      const cleanMessage = err.message.split('\n')[0].replace(/:\s*$/, '');
      return {
        line,
        from,
        to,
        message: cleanMessage,
      };
    });
  } catch {
    return [];
  }
}

/**
 * Summarizes high-level pipeline properties for display.
 * Returns null if the YAML is invalid or not an object.
 */
export function summarizePipeline(text: string): PipelineSummary | null {
  if (!text || typeof text !== 'string') return null;
  try {
    const doc = parseDocument(text);
    if (doc.errors && doc.errors.length > 0) return null;
    const json = doc.toJSON();
    if (!json || typeof json !== 'object' || Array.isArray(json)) return null;

    const summary: PipelineSummary = {};

    if (json.pipeline && typeof json.pipeline === 'object') {
      if (json.pipeline.name) summary.name = String(json.pipeline.name);
      if (json.pipeline.version) summary.version = String(json.pipeline.version);
    }

    if (json.source && typeof json.source === 'object') {
      if (json.source.type) summary.sourceType = String(json.source.type);
      if (json.source.path) summary.sourcePath = String(json.source.path);
    }

    if (Array.isArray(json.schema)) {
      summary.schemaFields = json.schema.length;
    }

    if (json.processing && typeof json.processing === 'object') {
      if (Array.isArray(json.processing.operations)) {
        summary.operations = json.processing.operations
          .map((op: any) => (op && typeof op === 'object' ? op.type : null))
          .filter(Boolean);
      }
    }

    if (json.messaging && typeof json.messaging === 'object' && json.messaging.topic) {
      summary.topic = String(json.messaging.topic);
    }

    if (
      json.processing_engine &&
      typeof json.processing_engine === 'object' &&
      json.processing_engine.type
    ) {
      summary.engine = String(json.processing_engine.type);
    }

    if (json.storage && typeof json.storage === 'object') {
      if (json.storage.type) summary.storageType = String(json.storage.type);
      if (json.storage.path) summary.storagePath = String(json.storage.path);
    }

    if (json.streaming && typeof json.streaming === 'object') {
      if (typeof json.streaming.enabled === 'boolean') {
        summary.streaming = json.streaming.enabled;
      }
    }

    if (Object.keys(summary).length === 0) {
      return null;
    }

    return summary;
  } catch {
    return null;
  }
}

/**
 * Helper to read text from File or Blob in any environment (including jsdom).
 */
export async function readFileText(file: File): Promise<string> {
  if (typeof (file as any).text === 'function') {
    return await (file as any).text();
  }
  return new Promise<string>((resolve, reject) => {
    if (typeof FileReader === 'undefined') {
      resolve('');
      return;
    }
    const reader = new FileReader();
    reader.onload = () => resolve((reader.result as string) || '');
    reader.onerror = () => reject(reader.error);
    reader.readAsText(file);
  });
}

/**
 * Synchronous file metadata validation.
 */
export function validateUploadSync(file: File, textContent?: string): UploadValidationResult {
  const ext = file.name.slice(file.name.lastIndexOf('.')).toLowerCase();
  if (ext !== '.yaml' && ext !== '.yml') {
    return { ok: false, reason: 'Only .yaml and .yml files are allowed.' };
  }
  if (file.size === 0) {
    return { ok: false, reason: 'File is empty.' };
  }
  if (file.size > 256 * 1024) {
    return { ok: false, reason: 'File exceeds maximum size of 256 KB.' };
  }
  if (textContent !== undefined && textContent.includes('\0')) {
    return { ok: false, reason: 'File contains invalid binary/NUL characters.' };
  }
  return { ok: true };
}

/**
 * Validates uploaded file: .yaml/.yml extension, <= 256 KB, non-empty, no NUL characters.
 * Supports both async await validateUpload(file) and synchronous checks.
 */
export function validateUpload(
  file: File,
  textContent?: string
): Promise<UploadValidationResult> & UploadValidationResult {
  const syncRes = validateUploadSync(file, textContent);
  const promise = (async () => {
    if (!syncRes.ok) return syncRes;
    const content = textContent !== undefined ? textContent : await readFileText(file);
    if (content.includes('\0')) {
      return { ok: false, reason: 'File contains invalid binary/NUL characters.' };
    }
    return { ok: true };
  })();

  Object.assign(promise, syncRes);
  return promise as any;
}

/**
 * Generates a safe download filename: lowercase letters, digits, dash, underscore only;
 * fallback: pipeline.yaml.
 */
export function safeFileName(name: string | undefined): string {
  if (!name || typeof name !== 'string') {
    return 'pipeline.yaml';
  }
  let cleaned = name.trim().replace(/\.(ya?ml)$/i, '');
  cleaned = cleaned
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '');
  if (!cleaned) {
    return 'pipeline.yaml';
  }
  return `${cleaned}.yaml`;
}
