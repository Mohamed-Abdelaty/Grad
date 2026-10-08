import React, { Component, ErrorInfo } from 'react';
import { AlertTriangleIcon } from 'lucide-react';

interface Props {
  children: React.ReactNode;
  fallback?: React.ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('[ErrorBoundary]', error, info.componentStack);
  }

  render() {
    if (this.state.hasError) {
      if (this.props.fallback) return this.props.fallback;
      return (
        <div className="flex flex-col items-center justify-center min-h-[40vh] gap-4 text-center p-8">
          <div className="rounded-full bg-status-error/10 border border-status-error/30 p-5 text-status-error">
            <AlertTriangleIcon className="h-8 w-8" />
          </div>
          <div>
            <p className="text-base font-semibold text-slate-200">An unexpected error occurred</p>
            <p className="mt-1 text-sm text-slate-400 max-w-sm">
              {this.state.error?.message ?? 'An unknown error occurred.'}
            </p>
          </div>
          <button
            onClick={() => this.setState({ hasError: false, error: null })}
            className="px-4 py-2 text-sm bg-surface-raised border border-surface-border rounded-md text-slate-300 hover:text-slate-100 transition-colors"
          >
            Try again
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}
