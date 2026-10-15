import React from 'react';
import { Outlet } from 'react-router-dom';
import Sidebar, { SidebarProvider, useSidebar } from './Sidebar';
import TopBar from './TopBar';
import { OfflineBanner } from './OfflineBanner';
import { RouteAnnouncer } from './RouteAnnouncer';
import { ErrorBoundary } from '../ui/ErrorBoundary';
import { ErrorState } from '../ui/ErrorState';

function ShellContent() {
  const { collapsed } = useSidebar();
  return (
    <div className="flex min-h-screen bg-surface-base text-slate-100 flex-col">
      {/* Skip to Main Content Link for accessibility */}
      <a
        href="#main"
        className="sr-only focus:not-sr-only focus:fixed focus:top-3 focus:left-3 focus:z-50 focus:px-4 focus:py-2 focus:bg-indigo-600 focus:text-white focus:rounded-md focus:shadow-lg focus:outline-none focus:ring-2 focus:ring-indigo-400 font-medium text-sm"
      >
        Skip to main content
      </a>

      {/* Screen reader live route announcement & title management */}
      <RouteAnnouncer />

      {/* Offline connectivity warning banner */}
      <OfflineBanner />

      <div className="flex flex-1 min-h-0">
        <Sidebar />
        {/* Main content shifts right to clear the fixed sidebar */}
        <div
          className={[
            'flex flex-col flex-1 min-w-0 transition-[margin-left] duration-base',
            collapsed ? 'ml-sidebar-sm' : 'ml-sidebar',
          ].join(' ')}
        >
          <TopBar />
          <main
            className="flex-1 overflow-auto p-6 mt-topbar focus:outline-none"
            id="main"
            data-testid="main-content"
            tabIndex={-1}
          >
            <ErrorBoundary
              fallback={
                <ErrorState
                  title="Page encountered an error"
                  description="An unexpected error occurred while rendering this page."
                  onRetry={() => window.location.reload()}
                />
              }
            >
              <Outlet />
            </ErrorBoundary>
          </main>
        </div>
      </div>
    </div>
  );
}

export default function PageLayout() {
  return (
    <SidebarProvider>
      <ShellContent />
    </SidebarProvider>
  );
}
