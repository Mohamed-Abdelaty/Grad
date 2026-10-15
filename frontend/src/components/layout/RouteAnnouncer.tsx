import React, { useEffect, useState } from 'react';
import { useLocation } from 'react-router-dom';

function getPageTitle(pathname: string): string {
  if (pathname === '/') return 'Dashboard';
  if (pathname === '/pipelines') return 'Pipelines';
  if (pathname.startsWith('/pipelines/')) return 'Pipeline Details';
  if (pathname === '/reliability') return 'Reliability Testing';
  if (pathname === '/reports') return 'Reports';
  if (pathname.startsWith('/reports/')) return 'Report Details';
  if (pathname === '/login') return 'Login';
  return 'Not Found';
}

export function RouteAnnouncer() {
  const location = useLocation();
  const [announcement, setAnnouncement] = useState('');

  useEffect(() => {
    const title = getPageTitle(location.pathname);
    document.title = `${title} · ChaosFlow`;
    setAnnouncement(`${title} page loaded`);

    // Focus main h1 on route change for accessible keyboard navigation
    const timer = setTimeout(() => {
      const h1 = document.querySelector('h1');
      if (h1) {
        if (!h1.hasAttribute('tabindex')) {
          h1.setAttribute('tabindex', '-1');
        }
        h1.focus();
      } else {
        const main = document.getElementById('main');
        main?.focus();
      }
    }, 50);

    return () => clearTimeout(timer);
  }, [location.pathname]);

  return (
    <div
      role="status"
      aria-live="polite"
      aria-atomic="true"
      className="sr-only"
      data-testid="route-announcement"
    >
      {announcement}
    </div>
  );
}
