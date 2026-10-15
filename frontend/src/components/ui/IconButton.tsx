import React from 'react';

interface IconButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  label: string; // required for aria-label
  size?: 'sm' | 'md' | 'lg';
}

const sizeClasses = {
  sm: 'h-8 w-8 text-sm min-h-[40px] min-w-[40px]',
  md: 'h-9 w-9 text-base min-h-[40px] min-w-[40px]',
  lg: 'h-11 w-11 text-lg min-h-[44px] min-w-[44px]',
};

export function IconButton({
  label,
  size = 'md',
  children,
  className = '',
  ...props
}: IconButtonProps) {
  return (
    <button
      aria-label={label}
      title={label}
      className={[
        'inline-flex items-center justify-center rounded-md transition-colors duration-fast',
        'text-slate-400 hover:text-slate-100 hover:bg-surface-raised',
        'disabled:opacity-50 disabled:cursor-not-allowed',
        sizeClasses[size],
        className,
      ].join(' ')}
      {...props}
    >
      {children}
    </button>
  );
}
