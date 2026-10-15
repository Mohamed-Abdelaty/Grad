import React, { useEffect } from 'react';
import { Link } from 'react-router-dom';
import { AlertTriangleIcon, HomeIcon } from 'lucide-react';

export default function NotFound() {
  useEffect(() => {
    document.title = 'Not Found · ChaosFlow';
  }, []);

  return (
    <div className="min-h-screen bg-slate-900 flex items-center justify-center p-6 text-slate-100">
      <div className="max-w-md w-full bg-slate-800 border border-slate-700 rounded-xl p-8 text-center space-y-6 shadow-2xl">
        <div className="h-16 w-16 bg-amber-500/10 border border-amber-500/30 rounded-full flex items-center justify-center mx-auto text-amber-400">
          <AlertTriangleIcon className="h-8 w-8" />
        </div>

        <div className="space-y-2">
          <h1 className="text-3xl font-extrabold text-slate-100 tracking-tight">Page Not Found</h1>
          <p className="text-slate-400 text-sm">
            The page you are looking for doesn't exist or has been moved.
          </p>
        </div>

        <div>
          <Link
            to="/"
            className="inline-flex items-center justify-center gap-2 bg-indigo-600 hover:bg-indigo-500 text-white font-medium px-5 py-2.5 rounded-lg text-sm transition-colors min-h-[40px]"
          >
            <HomeIcon className="h-4 w-4" />
            Back to Dashboard
          </Link>
        </div>
      </div>
    </div>
  );
}
