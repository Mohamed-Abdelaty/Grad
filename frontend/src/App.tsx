import React, { Suspense, lazy } from 'react';
import { Routes, Route, Navigate, Outlet } from 'react-router-dom';
import { useAuth } from './hooks/useAuth';
import PageLayout from './components/layout/PageLayout';
import { SkeletonCard } from './components/ui';

// Route-level code splitting: each page is loaded on demand.
// The Skeleton fallback shows the same placeholder that page-level loading states use,
// keeping the transition visually consistent.
const Login = lazy(() => import('./pages/Login'));
const Dashboard = lazy(() => import('./pages/Dashboard'));
const PipelineManagement = lazy(() => import('./pages/PipelineManagement'));
const PipelineDetails = lazy(() => import('./pages/PipelineDetails'));
const ReliabilityTesting = lazy(() => import('./pages/ReliabilityTesting'));
const Reports = lazy(() => import('./pages/Reports'));

function PageFallback() {
  return (
    <div className="p-6 space-y-4">
      <SkeletonCard />
      <SkeletonCard />
    </div>
  );
}

const AuthGuard = () => {
  const { token } = useAuth();
  if (!token) {
    return <Navigate to="/login" replace />;
  }
  return <Outlet />;
};

export default function App() {
  return (
    <Suspense fallback={<PageFallback />}>
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route element={<AuthGuard />}>
          <Route element={<PageLayout />}>
            <Route path="/" element={<Dashboard />} />
            <Route path="/pipelines" element={<PipelineManagement />} />
            <Route path="/pipelines/:runId" element={<PipelineDetails />} />
            <Route path="/reliability" element={<ReliabilityTesting />} />
            <Route path="/reports" element={<Reports />} />
          </Route>
        </Route>
      </Routes>
    </Suspense>
  );
}
