import React, { useState, useEffect } from 'react';
import { WifiOffIcon } from 'lucide-react';

export function OfflineBanner() {
  const [isOffline, setIsOffline] = useState(() => {
    return typeof navigator !== 'undefined' ? !navigator.onLine : false;
  });

  useEffect(() => {
    const handleOnline = () => setIsOffline(false);
    const handleOffline = () => setIsOffline(true);

    window.addEventListener('online', handleOnline);
    window.addEventListener('offline', handleOffline);

    return () => {
      window.removeEventListener('online', handleOnline);
      window.removeEventListener('offline', handleOffline);
    };
  }, []);

  if (!isOffline) return null;

  return (
    <div
      role="alert"
      data-testid="offline-banner"
      className="w-full bg-amber-600 text-white text-xs font-semibold px-4 py-2 text-center flex items-center justify-center gap-2 shadow-md z-50 sticky top-0"
    >
      <WifiOffIcon className="h-4 w-4 shrink-0 animate-pulse" />
      <span>You are currently offline. Changes may not be saved and real-time streams are paused.</span>
    </div>
  );
}
