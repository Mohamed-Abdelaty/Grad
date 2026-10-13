import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ExternalLinkIcon, CopyIcon, CheckIcon } from 'lucide-react';
import { getSystemStatus } from '../../../api/system';
import { RunProgress } from '../../../types/runProgress';
import { StatusPill } from '../../ui/StatusPill';
import { IconButton } from '../../ui/IconButton';
import { Tooltip } from '../../ui/Tooltip';
import { toast } from '../../ui/Toast';
import { formatCount, formatBytes } from '../../../lib/format';

interface ServicesPanelProps {
  progress: RunProgress | null;
}

export const ServicesPanel = React.memo(function ServicesPanel({
  progress,
}: ServicesPanelProps) {
  const [copiedAppId, setCopiedAppId] = useState(false);

  const { data: systemStatus, isLoading } = useQuery({
    queryKey: ['systemStatus'],
    queryFn: getSystemStatus,
    refetchInterval: () => (document.hidden ? false : 10000),
  });

  const sparkUiBaseUrl = import.meta.env.VITE_SPARK_UI_URL || 'http://localhost:8080';
  const appId = progress?.spark?.appId || null;

  const handleCopyAppId = async () => {
    if (!appId) return;
    try {
      await navigator.clipboard.writeText(appId);
      setCopiedAppId(true);
      toast.success('Spark App ID copied');
      setTimeout(() => setCopiedAppId(false), 2000);
    } catch {
      toast.error('Failed to copy App ID');
    }
  };

  const services = [
    {
      id: 'database',
      name: 'Database',
      connected: systemStatus?.database?.connected ?? false,
      detail: systemStatus?.database?.latency_ms !== undefined
        ? `${systemStatus.database.latency_ms}ms latency`
        : systemStatus?.database?.error || (isLoading ? 'Checking...' : 'Not connected'),
    },
    {
      id: 'kafka',
      name: 'Kafka Broker',
      connected: systemStatus?.kafka?.connected ?? false,
      detail: systemStatus?.kafka?.broker_count !== undefined
        ? `${systemStatus.kafka.broker_count} broker(s)`
        : systemStatus?.kafka?.error || (isLoading ? 'Checking...' : 'Not connected'),
    },
    {
      id: 'spark',
      name: 'Spark Master',
      connected: systemStatus?.spark?.connected ?? false,
      detail: systemStatus?.spark?.worker_count !== undefined
        ? `${systemStatus.spark.worker_count} worker(s)`
        : systemStatus?.spark?.error || (isLoading ? 'Checking...' : 'Not connected'),
    },
    {
      id: 'hdfs',
      name: 'Hadoop HDFS',
      connected: systemStatus?.hdfs?.connected ?? false,
      detail: systemStatus?.hdfs?.num_live_datanodes !== undefined
        ? `${systemStatus.hdfs.num_live_datanodes} live datanode(s)`
        : systemStatus?.hdfs?.error || (isLoading ? 'Checking...' : 'Not connected'),
    },
  ];

  return (
    <div
      data-testid="services-panel"
      className="p-4 bg-surface-card border border-surface-border rounded-xl space-y-4"
    >
      <div className="flex items-center justify-between text-xs text-slate-400">
        <span className="font-semibold text-slate-200">Infrastructure Services</span>
        <span className="text-slate-500">Refreshes every 10s</span>
      </div>

      {/* Services status cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {services.map((svc) => (
          <div
            key={svc.id}
            data-testid={`service-${svc.id}`}
            className="p-3 bg-surface-base border border-surface-border rounded-lg space-y-1.5"
          >
            <div className="flex items-center justify-between gap-1">
              <span className="text-xs font-medium text-slate-300">{svc.name}</span>
              <StatusPill
                status={svc.connected ? 'succeeded' : 'failed'}
                label={svc.connected ? 'Connected' : 'Unavailable'}
                dot={svc.connected}
              />
            </div>
            <div className="text-[11px] text-slate-400 truncate" title={svc.detail}>
              {svc.detail}
            </div>
          </div>
        ))}
      </div>

      {/* Run-specific details */}
      <div className="pt-3 border-t border-surface-border/50 grid grid-cols-1 md:grid-cols-3 gap-3 text-xs">
        {/* Kafka Run Details */}
        <div className="bg-surface-base/60 p-2.5 rounded-lg border border-surface-border/40 space-y-1">
          <div className="text-slate-400 font-medium">Kafka Topic & Partitions</div>
          <div className="font-mono text-slate-200">
            {progress?.kafka?.topic ? (
              <span>{progress.kafka.topic}</span>
            ) : (
              <span className="text-slate-500">—</span>
            )}
            <span className="text-slate-500 mx-1">/</span>
            {progress?.kafka?.partitions !== null && progress?.kafka?.partitions !== undefined ? (
              <span>{progress.kafka.partitions} partitions</span>
            ) : (
              <span className="text-slate-500">—</span>
            )}
          </div>
        </div>

        {/* Spark Run Details */}
        <div className="bg-surface-base/60 p-2.5 rounded-lg border border-surface-border/40 space-y-1">
          <div className="text-slate-400 font-medium flex items-center justify-between">
            <span>Spark App ID</span>
            {appId && sparkUiBaseUrl && (
              <a
                href={`${sparkUiBaseUrl}/app/?appId=${appId}`}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1 text-[11px] text-brand-light hover:underline"
              >
                Open in Spark UI
                <ExternalLinkIcon className="h-3 w-3" />
              </a>
            )}
          </div>
          <div className="flex items-center gap-1.5">
            {appId ? (
              <>
                <code className="font-mono text-slate-200 text-xs truncate max-w-[180px]">
                  {appId}
                </code>
                <Tooltip content="Copy App ID">
                  <IconButton
                    label="Copy App ID"
                    size="sm"
                    onClick={handleCopyAppId}
                    className="text-slate-400 hover:text-slate-200"
                  >
                    {copiedAppId ? (
                      <CheckIcon className="h-3 w-3 text-status-success" />
                    ) : (
                      <CopyIcon className="h-3 w-3" />
                    )}
                  </IconButton>
                </Tooltip>
              </>
            ) : (
              <span className="font-mono text-slate-500">—</span>
            )}
          </div>
        </div>

        {/* HDFS Run Details */}
        <div className="bg-surface-base/60 p-2.5 rounded-lg border border-surface-border/40 space-y-1">
          <div className="text-slate-400 font-medium">HDFS Destination</div>
          <div className="font-mono text-slate-200 truncate" title={progress?.hdfs?.path || ''}>
            {progress?.hdfs?.path || <span className="text-slate-500">—</span>}
          </div>
          <div className="text-[11px] text-slate-400">
            {progress?.hdfs?.filesWritten !== null && progress?.hdfs?.filesWritten !== undefined ? (
              `${formatCount(progress.hdfs.filesWritten)} files`
            ) : (
              '— files'
            )}
            <span className="mx-1">·</span>
            {progress?.hdfs?.bytesWritten !== null && progress?.hdfs?.bytesWritten !== undefined ? (
              formatBytes(progress.hdfs.bytesWritten)
            ) : (
              '— bytes'
            )}
          </div>
        </div>
      </div>
    </div>
  );
});
