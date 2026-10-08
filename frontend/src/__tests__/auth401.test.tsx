import React from 'react';
import { render, screen, waitFor, act } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom';
import { AuthProvider } from '../context/AuthContext';
import { useAuth } from '../hooks/useAuth';
import { apiClient, setApiToken } from '../api/client';

vi.mock('../api/system', () => ({
  getMe: vi.fn().mockResolvedValue({ username: 'admin', role: 'admin' }),
}));

function LoginProbe() {
  const location = useLocation();
  const expired = (location.state as { sessionExpired?: boolean } | null)?.sessionExpired;
  return <div>login-page expired={String(Boolean(expired))}</div>;
}

function Home() {
  const { token, login } = useAuth();
  return (
    <div>
      <span>token={token ?? 'none'}</span>
      <button onClick={() => void login('tok', 'admin')}>do-login</button>
    </div>
  );
}

function mount() {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <AuthProvider>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/login" element={<LoginProbe />} />
        </Routes>
      </AuthProvider>
    </MemoryRouter>
  );
}

function respondWith(status: number) {
  apiClient.defaults.adapter = async (config) => {
    const response = { status, statusText: '', data: {}, headers: {}, config };
    if (status >= 400) {
      throw Object.assign(new Error(`HTTP ${status}`), { config, response, isAxiosError: true });
    }
    return response;
  };
}

describe('401 callback to logout flow (real apiClient + AuthProvider)', () => {
  beforeEach(() => {
    setApiToken(null);
  });

  it('logs out and redirects to /login with sessionExpired when an authenticated call gets 401', async () => {
    mount();
    await act(async () => {
      screen.getByText('do-login').click();
    });
    await waitFor(() => expect(screen.getByText('token=tok')).toBeInTheDocument());
    setApiToken('tok');

    respondWith(401);
    await act(async () => {
      await apiClient.get('/api/anything').catch(() => undefined);
    });

    await waitFor(() =>
      expect(screen.getByText('login-page expired=true')).toBeInTheDocument()
    );
  });

  it('does NOT trigger session-expired for a 401 on an unauthenticated request (failed login)', async () => {
    mount();
    respondWith(401);
    await act(async () => {
      await apiClient.post('/api/auth/login', {}).catch(() => undefined);
    });
    expect(screen.queryByText(/login-page/)).not.toBeInTheDocument();
    expect(screen.getByText('token=none')).toBeInTheDocument();
  });
});
