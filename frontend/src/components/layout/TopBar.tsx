import React from 'react';
import { useLocation, useNavigate, Link } from 'react-router-dom';
import { ChevronRightIcon, LogOutIcon, UserIcon } from 'lucide-react';
import { useAuth } from '../../hooks/useAuth';
import { setApiToken } from '../../api/client';
import { useQuery } from '@tanstack/react-query';
import { getSystemStatus } from '../../api/system';
import { useSidebar } from './Sidebar';
import { Tooltip } from '../ui/Tooltip';
import { IconButton } from '../ui/IconButton';

/* ── Breadcrumb helpers ──────────────────────────────────────────────────── */
type Crumb = { label: string; to?: string };

function buildCrumbs(pathname: string): Crumb[] {
  const map: Record<string, Crumb[]> = {
    '/':            [{ label: 'Dashboard' }],
    '/pipelines':   [{ label: 'Pipelines' }],
    '/reliability': [{ label: 'Reliability Testing' }],
    '/reports':     [{ label: 'Reports' }],
  };
  if (pathname.startsWith('/pipelines/')) {
    return [
      { label: 'Pipelines', to: '/pipelines' },
      { label: 'Run detail' },
    ];
  }
  return map[pathname] ?? [{ label: 'Platform' }];
}

/* ── Service health dot ──────────────────────────────────────────────────── */
function ServiceDot({ name, connected }: { name: string; connected: boolean }) {
  return (
    <Tooltip content={`${name}: ${connected ? 'connected' : 'unavailable'}`} side="bottom">
      <span className="flex items-center gap-1 text-xs text-slate-400">
        <span
          className={[
            'h-2 w-2 rounded-full',
            connected ? 'bg-status-success' : 'bg-status-error',
          ].join(' ')}
          aria-hidden="true"
        />
        <span className="hidden sm:inline">{name}</span>
      </span>
    </Tooltip>
  );
}

export default function TopBar() {
  const { logout, username, role } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const { collapsed } = useSidebar();

  const { data: status } = useQuery({
    queryKey: ['systemStatus'],
    queryFn: getSystemStatus,
    refetchInterval: 10_000,
    staleTime: 10_000,
  });

  const crumbs = buildCrumbs(location.pathname);

  const handleLogout = () => {
    setApiToken(null);
    logout();
    navigate('/login');
  };

  const services: Array<[string, boolean]> = status
    ? [
        ['DB',    status.database.connected],
        ['Kafka', status.kafka.connected],
        ['Spark', status.spark.connected],
        ['HDFS',  status.hdfs.connected],
      ]
    : [];

  return (
    <header
      className={[
        'fixed top-0 right-0 z-20 h-topbar',
        'flex items-center justify-between gap-4 px-6',
        'bg-surface-raised border-b border-surface-border',
        'transition-[left] duration-base',
        collapsed ? 'left-sidebar-sm' : 'left-sidebar',
      ].join(' ')}
      aria-label="Top navigation bar"
    >
      {/* Breadcrumbs */}
      <nav aria-label="Breadcrumb" className="flex items-center gap-1 min-w-0">
        {crumbs.map((crumb, i) => (
          <React.Fragment key={i}>
            {i > 0 && <ChevronRightIcon className="h-3.5 w-3.5 text-slate-600 shrink-0" />}
            {crumb.to ? (
              <Link
                to={crumb.to}
                className="text-sm text-slate-400 hover:text-slate-100 transition-colors truncate"
              >
                {crumb.label}
              </Link>
            ) : (
              <span className="text-sm font-semibold text-slate-100 truncate">
                {crumb.label}
              </span>
            )}
          </React.Fragment>
        ))}
      </nav>

      {/* Right side */}
      <div className="flex items-center gap-4 shrink-0">
        {/* Service health strip */}
        {services.length > 0 && (
          <div
            className="hidden md:flex items-center gap-3 px-3 py-1 rounded-full bg-surface-base border border-surface-border"
            aria-label="Service health"
          >
            {services.map(([name, ok]) => (
              <ServiceDot key={name} name={name} connected={ok} />
            ))}
          </div>
        )}

        {/* User info + logout */}
        <div className="flex items-center gap-2">
          <Tooltip content={role ? `Role: ${role}` : 'Logged in'} side="bottom">
            <span className="flex items-center gap-1.5 text-sm text-slate-400">
              <UserIcon className="h-4 w-4" />
              <span className="hidden sm:inline">{username}</span>
            </span>
          </Tooltip>
          <Tooltip content="Sign out" side="bottom">
            <IconButton label="Sign out" onClick={handleLogout} size="sm">
              <LogOutIcon className="h-4 w-4" />
            </IconButton>
          </Tooltip>
        </div>
      </div>
    </header>
  );
}
