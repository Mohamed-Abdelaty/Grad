import React, {
  forwardRef,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
} from 'react';
import CodeMirror, { ReactCodeMirrorRef } from '@uiw/react-codemirror';
import { yaml } from '@codemirror/lang-yaml';
import { lintGutter, setDiagnostics, Diagnostic } from '@codemirror/lint';
import { EditorView, keymap } from '@codemirror/view';
import { indentUnit } from '@codemirror/language';
import { oneDark } from '@codemirror/theme-one-dark';
import { Prec } from '@codemirror/state';

export interface EditorDiagnostic {
  from: number;
  to: number;
  severity: 'error' | 'warning' | 'info';
  message: string;
}

export interface YamlEditorHandle {
  focusLine: (line: number) => void;
  focus: () => void;
}

export interface YamlEditorProps {
  value: string;
  onChange?: (value: string) => void;
  readOnly?: boolean;
  placeholder?: string;
  diagnostics?: EditorDiagnostic[];
  className?: string;
  onCursorChange?: (cursor: { line: number; col: number; lines: number }) => void;
}

const customTheme = EditorView.theme(
  {
    '&': {
      backgroundColor: 'var(--surface-base, #0f172a)',
      color: 'var(--text-primary, #f1f5f9)',
      minHeight: '420px',
      fontSize: '13px',
      fontFamily: "'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
    },
    '.cm-scroller': {
      fontFamily: "'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
      lineHeight: '1.6',
      overflow: 'auto',
      minHeight: '420px',
    },
    '.cm-content': {
      padding: '8px 0',
      caretColor: 'var(--brand, #6366f1)',
    },
    '.cm-line': {
      padding: '0 12px',
    },
    '&.cm-focused .cm-cursor': {
      borderLeftColor: 'var(--brand, #6366f1)',
    },
    '&.cm-focused .cm-selectionBackground, .cm-selectionBackground, ::selection': {
      backgroundColor: 'rgba(99, 102, 241, 0.25)',
    },
    '.cm-gutters': {
      backgroundColor: 'var(--surface-raised, #1e293b)',
      color: 'var(--text-muted, #64748b)',
      borderRight: '1px solid var(--surface-border, #334155)',
    },
    '.cm-activeLine': {
      backgroundColor: 'rgba(255, 255, 255, 0.04)',
    },
    '.cm-activeLineGutter': {
      backgroundColor: 'rgba(255, 255, 255, 0.08)',
      color: 'var(--text-primary, #f1f5f9)',
    },
    '.cm-lintRange-error': {
      backgroundImage: 'none',
      textDecoration: 'underline wavy var(--status-error, #fca5a5) 2px',
    },
    '.cm-lintRange-warning': {
      backgroundImage: 'none',
      textDecoration: 'underline wavy var(--status-warning, #fbbf24) 2px',
    },
    '.ͼq, &.cm-editor .ͼq': {
      color: '#fca5a5 !important',
    },
  },
  { dark: true }
);

export const YamlEditor = forwardRef<YamlEditorHandle, YamlEditorProps>(function YamlEditor(
  {
    value,
    onChange,
    readOnly = false,
    placeholder = '# Start typing YAML or choose a template above...',
    diagnostics = [],
    className = '',
    onCursorChange,
  },
  ref
) {
  const cmRef = useRef<ReactCodeMirrorRef>(null);
  const [escapeArmed, setEscapeArmed] = useState(false);

  useImperativeHandle(
    ref,
    () => ({
      focusLine: (line: number) => {
        const view = cmRef.current?.view;
        if (!view) return;
        const totalLines = view.state.doc.lines;
        const targetLine = Math.max(1, Math.min(line, totalLines));
        const lineObj = view.state.doc.line(targetLine);
        view.dispatch({
          selection: { anchor: lineObj.from, head: lineObj.to },
          scrollIntoView: true,
        });
        view.focus();
        onCursorChange?.({
          line: targetLine,
          col: 1,
          lines: totalLines,
        });
      },
      focus: () => {
        cmRef.current?.view?.focus();
      },
    }),
    [onCursorChange]
  );

  // Sync external diagnostics
  useEffect(() => {
    const view = cmRef.current?.view;
    if (!view) return;
    const docLength = view.state.doc.length;

    const cmDiags: Diagnostic[] = diagnostics
      .filter((d) => d.from <= docLength)
      .map((d) => ({
        from: Math.max(0, Math.min(d.from, docLength)),
        to: Math.max(d.from, Math.min(d.to, docLength)),
        severity: d.severity,
        message: d.message,
      }));

    try {
      view.dispatch(setDiagnostics(view.state, cmDiags));
    } catch {
      // safe fallback if view is torn down
    }
  }, [diagnostics]);

  // Extensions
  const extensions = useMemo(() => {
    let armed = false;

    const trapKeymap = Prec.highest(
      keymap.of([
        {
          key: 'Escape',
          run: () => {
            armed = true;
            setEscapeArmed(true);
            return false;
          },
        },
        {
          key: 'Tab',
          run: (view) => {
            if (armed) {
              armed = false;
              setEscapeArmed(false);
              view.contentDOM.blur();
              return false;
            }
            view.dispatch(view.state.replaceSelection('  '));
            return true;
          },
        },
      ])
    );

    const updateListener = EditorView.updateListener.of((update) => {
      if (update.selectionSet || update.docChanged) {
        const pos = update.state.selection.main.head;
        const lineObj = update.state.doc.lineAt(pos);
        onCursorChange?.({
          line: lineObj.number,
          col: pos - lineObj.from + 1,
          lines: update.state.doc.lines,
        });
      }
      if (update.docChanged && armed) {
        armed = false;
        setEscapeArmed(false);
      }
    });

    return [
      yaml(),
      indentUnit.of('  '),
      lintGutter(),
      trapKeymap,
      updateListener,
      EditorView.contentAttributes.of({
        'aria-label': 'Pipeline YAML configuration editor',
      }),
      oneDark,
      customTheme,
    ];
  }, [onCursorChange]);

  return (
    <div
      data-testid="yaml-editor"
      aria-label="Pipeline YAML editor"
      className={`relative min-h-[420px] rounded-lg border border-slate-700 bg-slate-900 overflow-hidden resize-y ${className}`}
      onBlur={() => setEscapeArmed(false)}
    >
      {escapeArmed && (
        <div className="absolute top-2 right-2 z-10 rounded bg-indigo-950/90 border border-indigo-500/30 px-2 py-0.5 text-xs text-indigo-300 pointer-events-none">
          Escape active: press Tab to exit editor
        </div>
      )}
      <CodeMirror
        ref={cmRef}
        value={value}
        onChange={(val) => onChange?.(val)}
        readOnly={readOnly}
        placeholder={placeholder}
        extensions={extensions}
        basicSetup={{
          lineNumbers: true,
          highlightActiveLineGutter: true,
          highlightActiveLine: true,
          foldGutter: true,
          bracketMatching: true,
          closeBrackets: true,
          autocompletion: true,
        }}
        theme="none"
      />
    </div>
  );
});

export default YamlEditor;
