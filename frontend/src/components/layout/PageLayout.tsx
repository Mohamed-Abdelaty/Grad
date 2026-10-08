import React from 'react';
import { Outlet } from 'react-router-dom';
import Sidebar, { SidebarProvider, useSidebar } from './Sidebar';
import TopBar from './TopBar';
import { ErrorBoundary } from '../ui/ErrorBoundary';

function ShellContent() {
  const { collapsed } = useSidebar();
  return (
    <div className="flex min-h-screen bg-surface-base text-slate-100">
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
          className="flex-1 overflow-auto p-6 mt-topbar"
          id="main-content"
        >
          <ErrorBoundary>
            <Outlet />
          </ErrorBoundary>
        </main>
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
