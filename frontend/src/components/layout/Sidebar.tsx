import React, { useState, createContext, useContext } from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import {
  LayoutDashboardIcon,
  GitBranchIcon,
  FlaskConicalIcon,
  BarChart3Icon,
  ChevronsLeftIcon,
  ChevronsRightIcon,
  ActivityIcon,
} from 'lucide-react';
import { Tooltip } from '../ui/Tooltip';
import { IconButton } from '../ui/IconButton';

const LINKS = [
  { to: '/',           label: 'Dashboard',          Icon: LayoutDashboardIcon,  end: true  },
  { to: '/pipelines',  label: 'Pipelines',           Icon: GitBranchIcon,         end: false },
  { to: '/reliability',label: 'Reliability Testing', Icon: FlaskConicalIcon,      end: false },
  { to: '/reports',    label: 'Reports',             Icon: BarChart3Icon,         end: false },
] as const;

/* ── Sidebar collapse context (consumed by PageLayout to shift content) ──── */
interface SidebarContextType {
  collapsed: boolean;
  toggle: () => void;
}

export const SidebarContext = createContext<SidebarContextType>({
  collapsed: false,
  toggle: () => {},
});

export function useSidebar() {
  return useContext(SidebarContext);
}

export function SidebarProvider({ children }: { children: React.ReactNode }) {
  const [collapsed, setCollapsed] = useState(false);
  return (
    <SidebarContext.Provider value={{ collapsed, toggle: () => setCollapsed((c) => !c) }}>
      {children}
    </SidebarContext.Provider>
  );
}

export default function Sidebar() {
  const { collapsed, toggle } = useSidebar();
  const location = useLocation();

  return (
    <aside
      className={[
        'fixed left-0 top-0 z-30 h-screen flex flex-col',
        'bg-surface-raised border-r border-surface-border sidebar-transition',
        collapsed ? 'w-sidebar-sm' : 'w-sidebar',
      ].join(' ')}
      aria-label="Main navigation"
    >
      {/* Wordmark / logo */}
      <div className="flex items-center gap-3 px-4 h-topbar border-b border-surface-border shrink-0 overflow-hidden">
        <ActivityIcon className="h-6 w-6 text-brand-light shrink-0" />
        {!collapsed && (
          <span className="font-bold text-sm text-slate-100 truncate leading-tight">
            ChaosFlow
          </span>
        )}
      </div>

      {/* Nav links */}
      <nav className="flex-1 py-4 space-y-1 px-2 overflow-y-auto" aria-label="Sidebar navigation">
        {LINKS.map(({ to, label, Icon, end }) => {
          const isActive = end
            ? location.pathname === to
            : location.pathname.startsWith(to);

          const linkCls = [
            'flex items-center gap-3 rounded-lg transition-colors duration-fast',
            collapsed ? 'justify-center px-2 py-2.5' : 'px-3 py-2.5',
            isActive
              ? 'bg-brand/15 text-brand-light font-medium'
              : 'text-slate-400 hover:text-slate-100 hover:bg-surface-raised',
          ].join(' ');

          return collapsed ? (
            <Tooltip key={to} content={label} side="right">
              <NavLink to={to} className={linkCls} aria-current={isActive ? 'page' : undefined}>
                <Icon className="h-5 w-5 shrink-0" />
              </NavLink>
            </Tooltip>
          ) : (
            <NavLink
              key={to}
              to={to}
              className={linkCls}
              aria-current={isActive ? 'page' : undefined}
            >
              <Icon className="h-5 w-5 shrink-0" />
              <span className="text-sm truncate">{label}</span>
              {isActive && (
                <span className="ml-auto h-1.5 w-1.5 rounded-full bg-brand-light" />
              )}
            </NavLink>
          );
        })}
      </nav>

      {/* Collapse toggle */}
      <div className="px-2 py-3 border-t border-surface-border">
        <IconButton
          label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          onClick={toggle}
          className="w-full justify-center"
        >
          {collapsed
            ? <ChevronsRightIcon className="h-4 w-4" />
            : <ChevronsLeftIcon className="h-4 w-4" />}
        </IconButton>
      </div>
    </aside>
  );
}
