import React, { useId } from 'react';

interface TooltipProps {
  content: string;
  children: React.ReactElement;
  side?: 'top' | 'bottom' | 'left' | 'right';
}

const sideClasses = {
  top:    'bottom-full left-1/2 -translate-x-1/2 mb-2',
  bottom: 'top-full left-1/2 -translate-x-1/2 mt-2',
  left:   'right-full top-1/2 -translate-y-1/2 mr-2',
  right:  'left-full top-1/2 -translate-y-1/2 ml-2',
};

export function Tooltip({ content, children, side = 'top' }: TooltipProps) {
  const id = useId();
  return (
    <div className="relative inline-flex group">
      {React.cloneElement(children, { 'aria-describedby': id })}
      <div
        id={id}
        role="tooltip"
        className={[
          'absolute z-50 pointer-events-none px-2 py-1 text-xs font-medium',
          'bg-slate-700 text-slate-100 rounded shadow-lg whitespace-nowrap',
          'opacity-0 group-hover:opacity-100 transition-opacity duration-fast',
          sideClasses[side],
        ].join(' ')}
      >
        {content}
      </div>
    </div>
  );
}
