import React, { createContext, useContext, useState } from 'react';

interface TabsContextType {
  active: string;
  setActive: (id: string) => void;
}

const TabsContext = createContext<TabsContextType | undefined>(undefined);

function useTabsContext() {
  const ctx = useContext(TabsContext);
  if (!ctx) throw new Error('Tab sub-components must be used inside <Tabs>');
  return ctx;
}

export function Tabs({
  defaultTab,
  children,
  className = '',
}: {
  defaultTab: string;
  children: React.ReactNode;
  className?: string;
}) {
  const [active, setActive] = useState(defaultTab);
  return (
    <TabsContext.Provider value={{ active, setActive }}>
      <div className={className}>{children}</div>
    </TabsContext.Provider>
  );
}

export function TabList({ children }: { children: React.ReactNode }) {
  return (
    <div
      role="tablist"
      className="flex gap-1 border-b border-surface-border mb-4"
    >
      {children}
    </div>
  );
}

export function Tab({ id, label }: { id: string; label: string }) {
  const { active, setActive } = useTabsContext();
  const isActive = active === id;
  return (
    <button
      role="tab"
      aria-selected={isActive}
      aria-controls={`tabpanel-${id}`}
      onClick={() => setActive(id)}
      className={[
        'px-4 py-2 text-sm font-medium transition-colors duration-fast -mb-px border-b-2',
        isActive
          ? 'border-brand text-brand-light'
          : 'border-transparent text-slate-400 hover:text-slate-200',
      ].join(' ')}
    >
      {label}
    </button>
  );
}

export function TabPanel({ id, children }: { id: string; children: React.ReactNode }) {
  const { active } = useTabsContext();
  if (active !== id) return null;
  return (
    <div role="tabpanel" id={`tabpanel-${id}`} className="animate-fade-in">
      {children}
    </div>
  );
}
