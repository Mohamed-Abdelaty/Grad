import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import App from './App';
import './styles/fonts.css';
import './styles/index.css';
import { AuthProvider } from './context/AuthContext';
import { Toaster } from './components/ui/Toast';
import { ErrorBoundary } from './components/ui/ErrorBoundary';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Retry once before showing an error (default is 3 — causes 30-second waits).
      retry: 1,
      // Data is considered fresh for 30 seconds before a background refetch.
      staleTime: 30_000,
    },
  },
});

// AuthProvider must be inside BrowserRouter because it calls useNavigate() to
// redirect to /login on 401 responses (see registerUnauthorizedHandler in client.ts).
ReactDOM.createRoot(document.getElementById('root') as HTMLElement).render(
  <React.StrictMode>
    <ErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <AuthProvider>
            <App />
            <Toaster />
          </AuthProvider>
        </BrowserRouter>
      </QueryClientProvider>
    </ErrorBoundary>
  </React.StrictMode>
);
