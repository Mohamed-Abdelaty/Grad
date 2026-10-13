import React, { Suspense, useMemo, useState } from 'react';
import { CopyIcon, CheckIcon, FileCodeIcon } from 'lucide-react';
import { summarizePipeline } from '../../../lib/yaml';
import { EmptyState } from '../../ui/EmptyState';
import { Button } from '../../ui/Button';
import { Skeleton } from '../../ui/Skeleton';
import { toast } from '../../ui/Toast';

const LazyYamlEditor = React.lazy(() => import('../YamlEditor'));

interface ConfigTabProps {
  yamlContent?: string | null;
}

export const ConfigTab = React.memo(function ConfigTab({
  yamlContent,
}: ConfigTabProps) {
  const [copied, setCopied] = useState(false);

  const summary = useMemo(() => {
    return summarizePipeline(yamlContent || '');
  }, [yamlContent]);

  if (!yamlContent || !yamlContent.trim()) {
    return (
      <div
        data-testid="config-tab"
        className="p-8 bg-surface-card border border-surface-border rounded-xl flex items-center justify-center min-h-[300px]"
      >
        <EmptyState
          title="Configuration not stored"
          description="The configuration of this run was not stored."
        />
      </div>
    );
  }

  const handleCopyYaml = async () => {
    try {
      await navigator.clipboard.writeText(yamlContent);
      setCopied(true);
      toast.success('Configuration YAML copied');
      setTimeout(() => setCopied(false), 2000);
    } catch {
      toast.error('Failed to copy YAML');
    }
  };

  return (
    <div
      data-testid="config-tab"
      className="grid grid-cols-1 lg:grid-cols-3 gap-4 items-start"
    >
      {/* Left Column: Parsed Summary */}
      <div className="lg:col-span-1 p-4 bg-surface-card border border-surface-border rounded-xl space-y-4">
        <div className="flex items-center gap-2 text-sm font-semibold text-slate-200">
          <FileCodeIcon className="h-4 w-4 text-brand-light" />
          <span>Parsed Summary</span>
        </div>

        <div className="space-y-3 text-xs">
          <div>
            <span className="text-slate-500 block">Pipeline Name:</span>
            <span className="font-medium text-slate-200">
              {summary?.name || 'Unnamed'}
            </span>
          </div>

          <div>
            <span className="text-slate-500 block">Version:</span>
            <span className="font-mono text-slate-300">
              {summary?.version || '1.0'}
            </span>
          </div>

          <div>
            <span className="text-slate-500 block">Source:</span>
            <span className="font-mono text-slate-300">
              {summary?.sourceType || '—'} {summary?.sourcePath ? `(${summary.sourcePath})` : ''}
            </span>
          </div>

          <div>
            <span className="text-slate-500 block">Storage:</span>
            <span className="font-mono text-slate-300">
              {summary?.storageType || '—'} {summary?.storagePath ? `(${summary.storagePath})` : ''}
            </span>
          </div>

          <div>
            <span className="text-slate-500 block">
              Operations ({summary?.operations?.length || 0}):
            </span>
            {summary?.operations && summary.operations.length > 0 ? (
              <ul className="mt-1 space-y-1 pl-4 list-disc text-slate-300">
                {summary.operations.map((op, i) => (
                  <li key={i}>{op}</li>
                ))}
              </ul>
            ) : (
              <span className="text-slate-500">None</span>
            )}
          </div>

          <div className="pt-2 border-t border-surface-border flex justify-between">
            <span className="text-slate-500">Streaming:</span>
            <span className="text-slate-300 font-mono">
              {summary?.streaming ? 'Enabled' : 'Disabled (Batch)'}
            </span>
          </div>

          <div className="flex justify-between">
            <span className="text-slate-500">Schema Fields:</span>
            <span className="text-slate-300 font-mono">
              {summary?.schemaFields ?? 0}
            </span>
          </div>
        </div>
      </div>

      {/* Right Column: Read-Only CodeMirror Editor */}
      <div className="lg:col-span-2 bg-surface-card border border-surface-border rounded-xl overflow-hidden space-y-2">
        <div className="px-4 py-2 bg-surface-base border-b border-surface-border flex items-center justify-between">
          <span className="text-xs font-medium text-slate-400">
            Pipeline Specification (YAML)
          </span>
          <Button
            variant="ghost"
            size="sm"
            onClick={handleCopyYaml}
            className="text-xs text-slate-400 hover:text-slate-200"
          >
            {copied ? (
              <>
                <CheckIcon className="h-3.5 w-3.5 mr-1 text-status-success" />
                Copied
              </>
            ) : (
              <>
                <CopyIcon className="h-3.5 w-3.5 mr-1" />
                Copy YAML
              </>
            )}
          </Button>
        </div>

        <div className="p-2">
          <Suspense fallback={<Skeleton className="h-[420px] w-full rounded" />}>
            <LazyYamlEditor
              value={yamlContent}
              readOnly={true}
              className="border border-surface-border rounded-lg"
            />
          </Suspense>
        </div>
      </div>
    </div>
  );
});
