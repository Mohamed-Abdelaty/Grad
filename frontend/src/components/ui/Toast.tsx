// Re-exports sonner's Toaster and toast() with our design-system config
// applied so callers don't need to set theme/position/styling everywhere.
import React from 'react';
export { toast } from 'sonner';
import { Toaster as SonnerToaster } from 'sonner';

export function Toaster() {
  return (
    <SonnerToaster
      position="top-right"
      theme="dark"
      toastOptions={{
        classNames: {
          toast:
            'bg-surface-raised border border-surface-border text-slate-100 shadow-xl rounded-lg font-sans text-sm',
          description: 'text-slate-400',
          actionButton: 'bg-brand text-white',
          cancelButton: 'bg-surface-border text-slate-200',
          error:   'border-status-error/40',
          success: 'border-status-success/40',
          warning: 'border-status-warning/40',
          info:    'border-status-info/40',
        },
      }}
    />
  );
}
