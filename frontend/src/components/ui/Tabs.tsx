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
  value,
  onChange,
  children,
  className = '',
}: {
  defaultTab?: string;
  value?: string;
  onChange?: (id: string) => void;
  children: React.ReactNode;
  className?: string;
}) {
  const [internalActive, setInternalActive] = useState(defaultTab ?? '');
  const active = value !== undefined ? value : internalActive;
  const setActive = (id: string) => {
    if (value === undefined) {
      setInternalActive(id);
    }
    onChange?.(id);
  };

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

  const handleKeyDown = (e: React.KeyboardEvent<HTMLButtonElement>) => {
    const tablist = e.currentTarget.parentElement;
    if (!tablist) return;
    const tabs = Array.from(tablist.querySelectorAll<HTMLButtonElement>('[role="tab"]'));
    const index = tabs.indexOf(e.currentTarget);
    if (index === -1) return;

    let nextIndex = -1;
    if (e.key === 'ArrowRight') {
      nextIndex = (index + 1) % tabs.length;
    } else if (e.key === 'ArrowLeft') {
      nextIndex = (index - 1 + tabs.length) % tabs.length;
    } else if (e.key === 'Home') {
      nextIndex = 0;
    } else if (e.key === 'End') {
      nextIndex = tabs.length - 1;
    }

    if (nextIndex !== -1) {
      e.preventDefault();
      tabs[nextIndex].focus();
      tabs[nextIndex].click();
    }
  };

  return (
    <button
      role="tab"
      id={`tab-${id}`}
      tabIndex={isActive ? 0 : -1}
      aria-selected={isActive}
      aria-controls={`tabpanel-${id}`}
      onClick={() => setActive(id)}
      onKeyDown={handleKeyDown}
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
    <div
      role="tabpanel"
      id={`tabpanel-${id}`}
      aria-labelledby={`tab-${id}`}
      tabIndex={0}
      className="animate-fade-in outline-none"
    >
      {children}
    </div>
  );
}
