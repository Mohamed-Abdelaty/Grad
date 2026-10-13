import React, { useState, useMemo, useEffect } from 'react';
import {
  ArrowDownIcon,
  CopyIcon,
  DownloadIcon,
  PauseIcon,
  PlayIcon,
  SearchIcon,
} from 'lucide-react';
import { useRunEvents } from '../../../hooks/useRunEvents';
import { LogViewer, LogEntry } from '../../ui/LogViewer';
import { Button } from '../../ui/Button';
import { IconButton } from '../../ui/IconButton';
import { Tooltip } from '../../ui/Tooltip';
import { toast } from '../../ui/Toast';
import { formatEventMessage, safeFileName } from '../../../lib/format';

interface LogsTabProps {
  runId: string;
  pipelineName?: string;
  isRunning: boolean;
  startedAt?: string | null;
}

type LevelFilter = 'all' | 'info' | 'warn' | 'error';

export const LogsTab = React.memo(function LogsTab({
  runId,
  pipelineName,
  isRunning,
  startedAt,
}: LogsTabProps) {
  const { events } = useRunEvents(runId, isRunning);

  const [levelFilter, setLevelFilter] = useState<LevelFilter>('all');
  const [searchInput, setSearchInput] = useState('');
  const [debouncedSearch, setDebouncedSearch] = useState('');
  const [isFollow, setIsFollow] = useState(true);

  // Debounce search input by 200 ms
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearch(searchInput);
    }, 200);
    return () => clearTimeout(timer);
  }, [searchInput]);

  // Format and filter events
  const logEntries: LogEntry[] = useMemo(() => {
    const startTime = startedAt ? new Date(startedAt).getTime() : null;

    return events.map((e) => {
      const formatted = formatEventMessage(e);
      let timeLabel = '';
      if (startTime) {
        const offsetSec = Math.max(0, Math.floor((new Date(e.ts).getTime() - startTime) / 1000));
        timeLabel = `+${offsetSec}s`;
      } else {
        timeLabel = new Date(e.ts).toLocaleTimeString();
      }

      return {
        id: e.seq,
        timestamp: timeLabel,
        level: formatted.level,
        message: formatted.text,
      };
    });
  }, [events, startedAt]);

  const filteredEntries = useMemo(() => {
    return logEntries.filter((entry) => {
      // Level filter
      if (levelFilter !== 'all' && entry.level !== levelFilter) {
        return false;
      }
      // Text search
      if (debouncedSearch.trim()) {
        const query = debouncedSearch.toLowerCase();
        if (!entry.message.toLowerCase().includes(query)) {
          return false;
        }
      }
      return true;
    });
  }, [logEntries, levelFilter, debouncedSearch]);

  const handleCopyVisible = async () => {
    if (filteredEntries.length === 0) return;
    const text = filteredEntries
      .map((e) => `[${e.timestamp}] [${(e.level || 'info').toUpperCase()}] ${e.message}`)
      .join('\n');
    try {
      await navigator.clipboard.writeText(text);
      toast.success(`Copied ${filteredEntries.length} log lines`);
    } catch {
      toast.error('Failed to copy logs');
    }
  };

  const handleDownloadLog = () => {
    if (events.length === 0) return;
    const text = events
      .map((e) => {
        const f = formatEventMessage(e);
        return `[${e.ts}] [seq:${e.seq}] [${f.level.toUpperCase()}] ${f.text}`;
      })
      .join('\n');

    const blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    const baseName = pipelineName ? safeFileName(pipelineName).replace(/\.yaml$/, '') : runId;
    a.download = `run-${baseName}.log`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    toast.success('Downloaded log file');
  };

  return (
    <div data-testid="logs-tab" className="space-y-3">
      {/* Controls Bar */}
      <div className="flex flex-wrap items-center justify-between gap-3 p-3 bg-surface-card border border-surface-border rounded-xl">
        {/* Filters and search */}
        <div className="flex items-center gap-2 flex-wrap">
          {/* Level Filter Buttons */}
          <div className="inline-flex rounded-lg border border-surface-border bg-surface-base p-0.5 text-xs">
            {(['all', 'info', 'warn', 'error'] as LevelFilter[]).map((lvl) => (
              <button
                key={lvl}
                type="button"
                onClick={() => setLevelFilter(lvl)}
                className={`px-2.5 py-1 rounded-md font-medium uppercase text-[11px] transition-colors ${
                  levelFilter === lvl
                    ? 'bg-brand text-brand-light shadow-sm'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                {lvl}
              </button>
            ))}
          </div>

          {/* Search Box */}
          <div className="relative min-w-[200px]">
            <SearchIcon className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-slate-500" />
            <input
              type="text"
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
              placeholder="Filter logs (200ms debounce)..."
              className="w-full bg-surface-base border border-surface-border rounded-lg pl-8 pr-3 py-1 text-xs text-slate-200 placeholder-slate-500 focus:outline-none focus:border-brand"
            />
            {searchInput && (
              <button
                onClick={() => setSearchInput('')}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-300 text-xs"
              >
                ×
              </button>
            )}
          </div>
        </div>

        {/* Actions */}
        <div className="flex items-center gap-2 flex-wrap">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setIsFollow(!isFollow)}
            className={`text-xs ${isFollow ? 'text-brand-light' : 'text-slate-400'}`}
          >
            {isFollow ? (
              <>
                <PauseIcon className="h-3.5 w-3.5 mr-1" />
                Pause Scroll
              </>
            ) : (
              <>
                <PlayIcon className="h-3.5 w-3.5 mr-1" />
                Resume Scroll
              </>
            )}
          </Button>

          {!isFollow && (
            <Button
              variant="secondary"
              size="sm"
              onClick={() => setIsFollow(true)}
              className="text-xs animate-fade-in"
            >
              <ArrowDownIcon className="h-3.5 w-3.5 mr-1" />
              Jump to Latest
            </Button>
          )}

          <Tooltip content="Copy visible logs to clipboard">
            <Button
              variant="ghost"
              size="sm"
              onClick={handleCopyVisible}
              disabled={filteredEntries.length === 0}
              className="text-slate-400 hover:text-slate-200"
            >
              <CopyIcon className="h-3.5 w-3.5 mr-1" />
              Copy
            </Button>
          </Tooltip>

          <Tooltip content="Download full plain text log">
            <IconButton
              label="Download log"
              size="sm"
              onClick={handleDownloadLog}
              disabled={events.length === 0}
              className="text-slate-400 hover:text-slate-200"
            >
              <DownloadIcon className="h-4 w-4" />
            </IconButton>
          </Tooltip>
        </div>
      </div>

      {/* LogViewer Display */}
      <LogViewer
        entries={filteredEntries}
        searchQuery={debouncedSearch}
        label={`Pipeline Execution Log (${filteredEntries.length} of ${events.length})`}
        maxHeight="32rem"
        follow={isFollow}
        onFollowChange={setIsFollow}
        emptyMessage={
          events.length === 0
            ? 'No log events yet'
            : 'No log events match the current filter criteria.'
        }
      />
    </div>
  );
});
