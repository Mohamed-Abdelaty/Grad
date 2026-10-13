import React from 'react';

export const VizSlot = React.memo(function VizSlot() {
  return (
    <div
      data-testid="viz-slot"
      className="w-full h-[280px] md:h-[360px] rounded-xl border border-surface-border/60 bg-surface-base/40 flex items-center justify-center text-slate-600 text-sm font-medium tracking-wide select-none"
    >
      Pipeline visualization
    </div>
  );
});
