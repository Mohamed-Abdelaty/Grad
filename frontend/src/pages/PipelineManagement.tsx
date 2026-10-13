import React, {
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import {
  CheckCircle2Icon,
  CopyIcon,
  DownloadIcon,
  FileCodeIcon,
  FolderOpenIcon,
  PlayIcon,
  SquareIcon,
  UploadIcon,
} from 'lucide-react';
import { toast } from 'sonner';

import {
  listTemplates,
  runPipeline,
  validatePipeline,
  cancelRun,
} from '../api/pipelines';
import {
  PipelineTemplate,
  ValidationIssue,
  ValidationResult,
} from '../types/pipeline';
import {
  resolveYamlLine,
  yamlSyntaxIssues,
  summarizePipeline,
  validateUpload,
  safeFileName,
} from '../lib/yaml';
import { YamlEditorHandle, EditorDiagnostic } from '../components/pipelines/YamlEditor';
import RunsTable from '../components/pipelines/RunsTable';
import { Card, CardHeader, CardTitle } from '../components/ui/Card';
import { Button } from '../components/ui/Button';
import { StatusPill } from '../components/ui/StatusPill';
import { Modal } from '../components/ui/Modal';
import { ConfirmDialog } from '../components/ui/ConfirmDialog';
import { ErrorState } from '../components/ui/ErrorState';
import { Skeleton } from '../components/ui/Skeleton';

const YamlEditor = React.lazy(() => import('../components/pipelines/YamlEditor'));

interface CombinedIssue {
  field: string;
  message: string;
  line: number;
  from: number;
  to: number;
  isSyntax?: boolean;
}

export default function PipelineManagement() {
  const navigate = useNavigate();
  const editorRef = useRef<YamlEditorHandle>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const templatesButtonRef = useRef<HTMLButtonElement>(null);

  // Editor content & file metadata
  const [yamlContent, setYamlContent] = useState('');
  const [fileName, setFileName] = useState('pipeline.yaml');
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);

  // Cursor position
  const [cursorInfo, setCursorInfo] = useState({ line: 1, col: 1, lines: 1 });

  // Auto-validate toggle
  const [autoValidate, setAutoValidate] = useState(true);

  // Validation states
  type ValidationStatus = 'idle' | 'validating' | 'valid' | 'invalid';
  const [validationStatus, setValidationStatus] = useState<ValidationStatus>('idle');
  const [validationIssues, setValidationIssues] = useState<CombinedIssue[]>([]);
  const [validationError, setValidationError] = useState<string | null>(null);

  // Active run tracking
  const [isSubmittingRun, setIsSubmittingRun] = useState(false);
  const [activeRunId, setActiveRunId] = useState<string | null>(null);
  const [isStopConfirmOpen, setIsStopConfirmOpen] = useState(false);

  // Modal dialog states
  const [isTemplatesModalOpen, setIsTemplatesModalOpen] = useState(false);
  const [pendingContentToLoad, setPendingContentToLoad] = useState<{
    content: string;
    filename: string;
  } | null>(null);
  const [isReplaceConfirmOpen, setIsReplaceConfirmOpen] = useState(false);

  // Drag-and-drop state
  const [isDraggingOver, setIsDraggingOver] = useState(false);

  // Validation request sequence counter to discard stale responses
  const validationSeqRef = useRef(0);

  // Fetch templates for modal
  const { data: templates = [], isLoading: isLoadingTemplates } = useQuery({
    queryKey: ['pipelineTemplates'],
    queryFn: listTemplates,
    staleTime: 60000,
  });

  // Summary computed from current YAML
  const pipelineSummary = useMemo(() => {
    return summarizePipeline(yamlContent);
  }, [yamlContent]);

  // Unsaved changes beforeunload warning
  useEffect(() => {
    const handleBeforeUnload = (e: BeforeUnloadEvent) => {
      if (hasUnsavedChanges) {
        e.preventDefault();
        e.returnValue = '';
      }
    };
    if (hasUnsavedChanges) {
      window.addEventListener('beforeunload', handleBeforeUnload);
    }
    return () => {
      window.removeEventListener('beforeunload', handleBeforeUnload);
    };
  }, [hasUnsavedChanges]);

  // Execute validation
  const performValidation = useCallback(
    async (text: string) => {
      if (!text.trim()) {
        setValidationStatus('idle');
        setValidationIssues([]);
        setValidationError(null);
        return;
      }

      const seq = ++validationSeqRef.current;
      setValidationStatus('validating');
      setValidationError(null);

      // Check client-side YAML syntax errors first
      const syntaxErrors = yamlSyntaxIssues(text);
      if (syntaxErrors.length > 0) {
        if (seq !== validationSeqRef.current) return;
        const issues: CombinedIssue[] = syntaxErrors.map((err) => ({
          field: 'syntax',
          message: err.message,
          line: err.line,
          from: err.from,
          to: err.to,
          isSyntax: true,
        }));
        setValidationIssues(issues);
        setValidationStatus('invalid');
        return;
      }

      // Call backend validation endpoint
      try {
        const result: ValidationResult = await validatePipeline(text);
        if (seq !== validationSeqRef.current) return;

        if (result.valid) {
          setValidationIssues([]);
          setValidationStatus('valid');
        } else {
          const resolvedIssues: CombinedIssue[] = (result.errors || []).map((err: ValidationIssue) => {
            const loc = resolveYamlLine(text, err.field);
            return {
              field: err.field,
              message: err.message,
              line: loc?.line ?? 1,
              from: loc?.from ?? 0,
              to: loc?.to ?? 0,
            };
          });
          setValidationIssues(resolvedIssues);
          setValidationStatus('invalid');
        }
      } catch (err) {
        if (seq !== validationSeqRef.current) return;
        setValidationError(
          err instanceof Error ? err.message : 'Validation request failed'
        );
      }
    },
    []
  );

  // Debounced auto-validate effect
  useEffect(() => {
    if (!autoValidate || !yamlContent.trim()) {
      if (!yamlContent.trim()) {
        setValidationStatus('idle');
        setValidationIssues([]);
        setValidationError(null);
      }
      return;
    }

    const timer = setTimeout(() => {
      performValidation(yamlContent);
    }, 800);

    return () => clearTimeout(timer);
  }, [yamlContent, autoValidate, performValidation]);

  // Handle user typing
  const handleEditorChange = (newVal: string) => {
    setYamlContent(newVal);
    setHasUnsavedChanges(true);
    // Any edit invalidates previous successful validation
    if (validationStatus === 'valid') {
      setValidationStatus('idle');
    }
  };

  // Safe load helper (confirms if unsaved edits)
  const applyContent = (content: string, name: string) => {
    setYamlContent(content);
    setFileName(name);
    setHasUnsavedChanges(false);
    performValidation(content);
  };

  const requestLoadContent = (content: string, name: string) => {
    if (hasUnsavedChanges && yamlContent.trim().length > 0) {
      setPendingContentToLoad({ content, filename: name });
      setIsReplaceConfirmOpen(true);
    } else {
      applyContent(content, name);
    }
  };

  // Templates selection
  const handleSelectTemplate = (template: PipelineTemplate) => {
    setIsTemplatesModalOpen(false);
    requestLoadContent(template.yaml, `${template.id}.yaml`);
  };

  // File upload processing
  const processUploadedFile = async (file: File) => {
    const check = await validateUpload(file);
    if (!check.ok) {
      toast.error(check.reason);
      return;
    }
    try {
      const text = await file.text();
      requestLoadContent(text, file.name);
      toast.success(`Loaded ${file.name}`);
    } catch {
      toast.error('Failed to read file content');
    }
  };

  const handleFileInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      processUploadedFile(file);
    }
    // Reset file input so re-uploading same file triggers change
    e.target.value = '';
  };

  // Drag and drop handlers
  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDraggingOver(true);
  };

  const handleDragLeave = () => {
    setIsDraggingOver(false);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDraggingOver(false);
    const file = e.dataTransfer.files?.[0];
    if (file) {
      processUploadedFile(file);
    }
  };

  // Download handler
  const handleDownload = () => {
    const downloadName = safeFileName(pipelineSummary?.name || fileName);
    const blob = new Blob([yamlContent], { type: 'text/yaml' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = downloadName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    toast.success(`Downloaded ${downloadName}`);
  };

  // Manual validate button
  const handleManualValidate = () => {
    performValidation(yamlContent);
  };

  // Run pipeline handler
  const handleRunPipeline = async () => {
    if (validationStatus !== 'valid') return;
    setIsSubmittingRun(true);

    try {
      const res = await runPipeline(yamlContent, false);
      const runId = res.pipelineRunId || res.pipeline_run_id;
      if (runId) {
        toast.success(`Pipeline run started (${runId})`);
        setActiveRunId(runId);
        setHasUnsavedChanges(false);
        navigate(`/pipelines/${runId}`);
      }
    } catch (err: unknown) {
      const axiosErr = err as {
        response?: { status?: number; data?: { errors?: ValidationIssue[] } };
      };
      if (axiosErr.response?.status === 422 && axiosErr.response?.data?.errors) {
        const issues = axiosErr.response.data.errors.map((e) => {
          const loc = resolveYamlLine(yamlContent, e.field);
          return {
            field: e.field,
            message: e.message,
            line: loc?.line ?? 1,
            from: loc?.from ?? 0,
            to: loc?.to ?? 0,
          };
        });
        setValidationIssues(issues);
        setValidationStatus('invalid');
      } else {
        toast.error(err instanceof Error ? err.message : 'Failed to start pipeline run');
      }
    } finally {
      setIsSubmittingRun(false);
    }
  };

  // Stop active run
  const handleStopRun = async () => {
    if (!activeRunId) return;
    try {
      const res = await cancelRun(activeRunId);
      if (res.accepted) {
        toast.success(`Stop requested for run ${activeRunId}`);
        setActiveRunId(null);
      } else {
        toast.error(res.detail || 'Stop request could not be accepted');
      }
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to stop run');
    } finally {
      setIsStopConfirmOpen(false);
    }
  };

  // Convert issues into editor diagnostics
  const editorDiagnostics = useMemo<EditorDiagnostic[]>(() => {
    return validationIssues.map((issue) => ({
      from: issue.from,
      to: Math.max(issue.to, issue.from + 1),
      severity: 'error',
      message: issue.message,
    }));
  }, [validationIssues]);

  // Navigate caret to issue line
  const handleGoToLine = (line: number) => {
    editorRef.current?.focusLine(line);
  };

  // Copy issue to clipboard
  const handleCopyIssue = (issue: CombinedIssue) => {
    navigator.clipboard.writeText(`${issue.field}: ${issue.message} (Line ${issue.line})`);
    toast.success('Issue details copied');
  };

  // Re-run callback from RunsTable
  const handleRerun = (rerunYaml: string) => {
    requestLoadContent(rerunYaml, 'rerun.yaml');
    // Scroll smoothly to editor
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  return (
    <div className="space-y-6 max-w-7xl mx-auto pb-12">
      {/* Hidden file input */}
      <input
        ref={fileInputRef}
        type="file"
        accept=".yaml,.yml"
        onChange={handleFileInputChange}
        className="hidden"
        aria-hidden="true"
      />

      {/* Main Two-Column Layout */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
        {/* Left Column: Editor Card (~62% / 7.5 cols on desktop) */}
        <div className="lg:col-span-7 xl:col-span-7 space-y-3">
          <Card padding="none" className="overflow-hidden border border-surface-border">
            {/* Editor Toolbar Header */}
            <CardHeader className="p-3 sm:p-4 border-b border-surface-border mb-0 bg-surface-base/80">
              <div className="flex flex-wrap items-center justify-between gap-3 w-full">
                {/* File chip & unsaved dot */}
                <div className="flex items-center gap-2">
                  <div className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded bg-surface-raised border border-surface-border text-xs font-mono text-slate-200">
                    <FileCodeIcon className="h-3.5 w-3.5 text-brand-light" />
                    <span>{fileName}</span>
                  </div>
                  {hasUnsavedChanges && (
                    <span
                      role="status"
                      title="Unsaved changes"
                      className="h-2 w-2 rounded-full bg-status-warning inline-block"
                      aria-label="Unsaved changes"
                    />
                  )}
                </div>

                {/* Toolbar Buttons */}
                <div className="flex flex-wrap items-center gap-2">
                  <Button
                    ref={templatesButtonRef}
                    data-testid="btn-templates"
                    variant="secondary"
                    size="sm"
                    onClick={() => setIsTemplatesModalOpen(true)}
                  >
                    <FolderOpenIcon className="h-3.5 w-3.5 text-slate-400" />
                    Templates
                  </Button>

                  <Button
                    data-testid="btn-upload"
                    variant="secondary"
                    size="sm"
                    onClick={() => fileInputRef.current?.click()}
                  >
                    <UploadIcon className="h-3.5 w-3.5 text-slate-400" />
                    Upload
                  </Button>

                  <Button
                    data-testid="btn-download"
                    variant="secondary"
                    size="sm"
                    onClick={handleDownload}
                    disabled={!yamlContent.trim()}
                  >
                    <DownloadIcon className="h-3.5 w-3.5 text-slate-400" />
                    Download
                  </Button>

                  <Button
                    data-testid="btn-validate"
                    variant="secondary"
                    size="sm"
                    onClick={handleManualValidate}
                    disabled={!yamlContent.trim() || validationStatus === 'validating'}
                  >
                    <CheckCircle2Icon className="h-3.5 w-3.5 text-slate-400" />
                    Validate
                  </Button>

                  {/* Auto-validate switch */}
                  <label className="inline-flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer select-none px-1">
                    <input
                      type="checkbox"
                      checked={autoValidate}
                      onChange={(e) => setAutoValidate(e.target.checked)}
                      className="rounded border-slate-700 bg-slate-900 text-brand focus:ring-brand h-3.5 w-3.5"
                    />
                    <span>Auto-validate</span>
                  </label>

                  <Button
                    data-testid="btn-run"
                    variant="primary"
                    size="sm"
                    onClick={handleRunPipeline}
                    disabled={validationStatus !== 'valid' || isSubmittingRun}
                    loading={isSubmittingRun}
                  >
                    <PlayIcon className="h-3.5 w-3.5 fill-current" />
                    Run
                  </Button>

                  {activeRunId && (
                    <Button
                      data-testid="btn-stop"
                      variant="danger"
                      size="sm"
                      onClick={() => setIsStopConfirmOpen(true)}
                    >
                      <SquareIcon className="h-3.5 w-3.5 fill-current" />
                      Stop
                    </Button>
                  )}
                </div>
              </div>
            </CardHeader>

            {/* Drop Zone & CodeMirror Editor */}
            <div
              className="relative"
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              onDrop={handleDrop}
            >
              {isDraggingOver && (
                <div className="absolute inset-0 z-20 bg-brand/20 border-2 border-dashed border-brand backdrop-blur-xs flex flex-col items-center justify-center pointer-events-none">
                  <UploadIcon className="h-10 w-10 text-brand animate-bounce" />
                  <p className="text-sm font-medium text-slate-100 mt-2">
                    Drop YAML file here to load
                  </p>
                </div>
              )}

              <Suspense
                fallback={
                  <Skeleton className="min-h-[420px] w-full rounded-none bg-slate-900" />
                }
              >
                <YamlEditor
                  ref={editorRef}
                  value={yamlContent}
                  onChange={handleEditorChange}
                  readOnly={isSubmittingRun}
                  diagnostics={editorDiagnostics}
                  onCursorChange={setCursorInfo}
                  placeholder="# Start typing YAML or choose a template above..."
                />
              </Suspense>
            </div>

            {/* Editor Status Bar */}
            <div className="flex items-center justify-between px-3 py-2 bg-surface-base/90 border-t border-surface-border text-xs text-slate-400 font-mono">
              <div className="flex items-center gap-3">
                <span>
                  Ln {cursorInfo.line}, Col {cursorInfo.col}
                </span>
                <span>{cursorInfo.lines} lines</span>
                <span>YAML</span>
              </div>
              <div>
                {validationStatus === 'valid' && (
                  <span className="text-status-success">Valid</span>
                )}
                {validationStatus === 'invalid' && (
                  <span className="text-status-error">
                    Invalid ({validationIssues.length} issues)
                  </span>
                )}
                {validationStatus === 'validating' && (
                  <span className="text-brand-light animate-pulse">Validating...</span>
                )}
                {validationStatus === 'idle' && (
                  <span className="text-slate-500">Not validated</span>
                )}
              </div>
            </div>
          </Card>
        </div>

        {/* Right Column: Validation Panel (~38% / 5 cols on desktop) */}
        <div className="lg:col-span-5 xl:col-span-5">
          <Card
            data-testid="validation-panel"
            aria-live="polite"
            padding="none"
            className="overflow-hidden border border-surface-border"
          >
            <CardHeader className="p-4 border-b border-surface-border mb-0">
              <div className="flex items-center justify-between w-full">
                <CardTitle>Validation</CardTitle>
                <div data-testid="validation-state">
                  {validationStatus === 'idle' && (
                    <StatusPill status="neutral" label="Not validated" />
                  )}
                  {validationStatus === 'validating' && (
                    <StatusPill status="running" label="Validating..." dot />
                  )}
                  {validationStatus === 'valid' && (
                    <StatusPill status="success" label="Valid" />
                  )}
                  {validationStatus === 'invalid' && (
                    <StatusPill
                      status="error"
                      label={`Invalid (${validationIssues.length} issue${
                        validationIssues.length === 1 ? '' : 's'
                      })`}
                    />
                  )}
                </div>
              </div>
            </CardHeader>

            <div className="p-4 space-y-4 max-h-[500px] overflow-y-auto">
              {validationError ? (
                <ErrorState
                  title="Validation error"
                  description={validationError}
                  onRetry={() => performValidation(yamlContent)}
                />
              ) : validationStatus === 'idle' ? (
                <div className="text-center py-10 text-slate-500 text-sm">
                  Click <strong className="text-slate-300">Validate</strong> or edit YAML to inspect configuration diagnostics.
                </div>
              ) : validationStatus === 'validating' ? (
                <div className="space-y-3 py-6">
                  <Skeleton className="h-6 w-3/4 rounded" />
                  <Skeleton className="h-6 w-full rounded" />
                  <Skeleton className="h-6 w-2/3 rounded" />
                </div>
              ) : validationStatus === 'invalid' ? (
                <div className="space-y-2.5">
                  <p className="text-xs text-status-error font-medium">
                    Found {validationIssues.length} issue{validationIssues.length === 1 ? '' : 's'} to fix:
                  </p>
                  {validationIssues.map((issue, idx) => (
                    <div
                      key={idx}
                      data-testid="issue-item"
                      className="p-3 rounded-lg bg-status-error/10 border border-status-error/25 text-xs space-y-1.5"
                    >
                      <div className="flex items-center justify-between gap-2">
                        <span className="font-mono font-semibold text-rose-300 truncate">
                          {issue.field}
                        </span>
                        <span className="text-slate-400 font-mono shrink-0">
                          Line {issue.line}
                        </span>
                      </div>
                      <p className="text-slate-200">{issue.message}</p>
                      <div className="flex items-center justify-end gap-2 pt-1">
                        <button
                          type="button"
                          onClick={() => handleCopyIssue(issue)}
                          className="inline-flex items-center gap-1 text-slate-400 hover:text-slate-200"
                        >
                          <CopyIcon className="h-3 w-3" />
                          Copy
                        </button>
                        <button
                          type="button"
                          onClick={() => handleGoToLine(issue.line)}
                          className="inline-flex items-center gap-1 text-brand-light hover:underline font-medium"
                        >
                          Go to line
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                /* Valid State: Pipeline summary + ready text */
                <div className="space-y-4">
                  <div className="flex items-center gap-2 text-status-success text-sm font-semibold">
                    <CheckCircle2Icon className="h-4 w-4" />
                    <span>Ready to run</span>
                  </div>

                  {pipelineSummary && (
                    <div className="space-y-2 text-xs border border-surface-border rounded-lg p-3 bg-surface-base/50">
                      {pipelineSummary.name && (
                        <div className="flex justify-between py-1 border-b border-surface-border/50">
                          <span className="text-slate-400">Name:</span>
                          <span className="font-mono text-slate-200">{pipelineSummary.name}</span>
                        </div>
                      )}
                      {pipelineSummary.version && (
                        <div className="flex justify-between py-1 border-b border-surface-border/50">
                          <span className="text-slate-400">Version:</span>
                          <span className="font-mono text-slate-200">{pipelineSummary.version}</span>
                        </div>
                      )}
                      {pipelineSummary.sourceType && (
                        <div className="flex justify-between py-1 border-b border-surface-border/50">
                          <span className="text-slate-400">Source:</span>
                          <span className="text-slate-200">
                            {pipelineSummary.sourceType}{' '}
                            {pipelineSummary.sourcePath && `(${pipelineSummary.sourcePath})`}
                          </span>
                        </div>
                      )}
                      {pipelineSummary.schemaFields !== undefined && (
                        <div className="flex justify-between py-1 border-b border-surface-border/50">
                          <span className="text-slate-400">Schema fields:</span>
                          <span className="font-mono text-slate-200">{pipelineSummary.schemaFields}</span>
                        </div>
                      )}
                      {pipelineSummary.operations && pipelineSummary.operations.length > 0 && (
                        <div className="py-1 border-b border-surface-border/50">
                          <span className="text-slate-400 block mb-1">Operations:</span>
                          <ol className="list-decimal list-inside pl-1 text-slate-200 space-y-0.5 font-mono">
                            {pipelineSummary.operations.map((op, i) => (
                              <li key={i}>{op}</li>
                            ))}
                          </ol>
                        </div>
                      )}
                      {pipelineSummary.topic && (
                        <div className="flex justify-between py-1 border-b border-surface-border/50">
                          <span className="text-slate-400">Kafka Topic:</span>
                          <span className="font-mono text-slate-200">{pipelineSummary.topic}</span>
                        </div>
                      )}
                      {pipelineSummary.engine && (
                        <div className="flex justify-between py-1 border-b border-surface-border/50">
                          <span className="text-slate-400">Engine:</span>
                          <span className="text-slate-200 uppercase">{pipelineSummary.engine}</span>
                        </div>
                      )}
                      {pipelineSummary.storageType && (
                        <div className="flex justify-between py-1">
                          <span className="text-slate-400">Storage:</span>
                          <span className="text-slate-200">
                            {pipelineSummary.storageType}{' '}
                            {pipelineSummary.storagePath && `(${pipelineSummary.storagePath})`}
                          </span>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}
            </div>
          </Card>
        </div>
      </div>

      {/* Runs Card Below Both Columns */}
      <RunsTable
        onRerun={handleRerun}
        onFocusTemplates={() => templatesButtonRef.current?.focus()}
      />

      {/* Templates Modal */}
      <Modal
        open={isTemplatesModalOpen}
        onClose={() => setIsTemplatesModalOpen(false)}
        title="Pipeline Templates"
        width="lg"
      >
        <div className="space-y-4">
          <p className="text-sm text-slate-400">
            Select a verified production template to load into the workspace:
          </p>

          {isLoadingTemplates ? (
            <div className="space-y-3">
              <Skeleton className="h-20 w-full rounded" />
              <Skeleton className="h-20 w-full rounded" />
              <Skeleton className="h-20 w-full rounded" />
            </div>
          ) : (
            <div className="grid grid-cols-1 gap-3">
              {templates.map((tpl) => {
                const s = summarizePipeline(tpl.yaml);
                return (
                  <div
                    key={tpl.id}
                    data-testid={`template-${tpl.id}`}
                    onClick={() => handleSelectTemplate(tpl)}
                    className="p-4 rounded-lg bg-surface-base border border-surface-border hover:border-brand cursor-pointer transition-colors space-y-2 group"
                  >
                    <div className="flex items-center justify-between">
                      <h4 className="text-sm font-semibold text-slate-100 group-hover:text-brand-light">
                        {tpl.name}
                      </h4>
                      <div className="flex items-center gap-1.5">
                        {s?.sourceType && (
                          <span className="px-2 py-0.5 rounded text-[11px] font-mono bg-surface-raised text-slate-300 border border-surface-border">
                            {s.sourceType}
                          </span>
                        )}
                        {s?.storageType && (
                          <span className="px-2 py-0.5 rounded text-[11px] font-mono bg-surface-raised text-slate-300 border border-surface-border">
                            {s.storageType}
                          </span>
                        )}
                      </div>
                    </div>
                    <p className="text-xs text-slate-400">{tpl.description}</p>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </Modal>

      {/* Replace Confirmation Dialog for Unsaved Edits */}
      <ConfirmDialog
        open={isReplaceConfirmOpen}
        onClose={() => {
          setIsReplaceConfirmOpen(false);
          setPendingContentToLoad(null);
        }}
        onConfirm={() => {
          if (pendingContentToLoad) {
            applyContent(pendingContentToLoad.content, pendingContentToLoad.filename);
          }
          setIsReplaceConfirmOpen(false);
          setPendingContentToLoad(null);
        }}
        title="Replace current content?"
        description="You have unsaved changes in the editor. Loading a new configuration will overwrite them."
        confirmLabel="Replace"
        confirmVariant="danger"
      />

      {/* Stop Run Confirmation Dialog */}
      <ConfirmDialog
        open={isStopConfirmOpen}
        onClose={() => setIsStopConfirmOpen(false)}
        onConfirm={handleStopRun}
        title="Stop pipeline run?"
        description={`Send cancellation request to pipeline run ${activeRunId}?`}
        confirmLabel="Stop Run"
        confirmVariant="danger"
      />
    </div>
  );
}
