import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { MemoryRouter } from 'react-router-dom';
import Login from '../pages/Login';
import { AuthProvider } from '../context/AuthContext';
import * as system from '../api/system';

vi.mock('../api/system', () => ({
  login: vi.fn(),
  getMe: vi.fn().mockResolvedValue({ username: 'u', role: 'admin' }),
}));

function renderLogin(state?: Record<string, unknown>) {
  return render(
    <MemoryRouter initialEntries={[{ pathname: '/login', state }]}>
      <AuthProvider>
        <Login />
      </AuthProvider>
    </MemoryRouter>
  );
}

describe('Login page', () => {
  beforeEach(() => vi.clearAllMocks());

  it('has no pre-filled username or password', () => {
    renderLogin();
    expect((screen.getByLabelText(/username/i) as HTMLInputElement).value).toBe('');
    expect((screen.getByLabelText(/password/i) as HTMLInputElement).value).toBe('');
  });

  it('shows the session-expired banner only when navigated with sessionExpired state', () => {
    const { unmount } = renderLogin({ sessionExpired: true });
    expect(screen.getByText(/session has expired/i)).toBeInTheDocument();
    unmount();
    renderLogin();
    expect(screen.queryByText(/session has expired/i)).not.toBeInTheDocument();
  });

  it('shows an error message when credentials are rejected', async () => {
    vi.mocked(system.login).mockRejectedValue(new Error('401'));
    renderLogin();
    fireEvent.change(screen.getByLabelText(/username/i), { target: { value: 'x' } });
    fireEvent.change(screen.getByLabelText(/password/i), { target: { value: 'y' } });
    fireEvent.click(screen.getByRole('button', { name: /sign in/i }));
    await waitFor(() =>
      expect(screen.getByText(/invalid credentials/i)).toBeInTheDocument()
    );
  });
});
