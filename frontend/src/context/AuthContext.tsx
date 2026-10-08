import React, { createContext, useState, useEffect, ReactNode } from 'react';
import { useNavigate } from 'react-router-dom';
import { setApiToken, registerUnauthorizedHandler } from '../api/client';
import { getMe } from '../api/system';

// JWT is stored IN MEMORY ONLY in React context (not localStorage/sessionStorage).
// Rationale: localStorage is vulnerable to XSS — any injected script can read it.
// In-memory storage means the token is lost on page refresh (user must re-login),
// which is an acceptable trade-off for this internal academic platform.
// Production systems should use httpOnly cookies set by the server.

export type UserRole = 'admin' | 'viewer';

interface AuthContextType {
  token: string | null;
  username: string | null;
  role: UserRole | null;
  login: (token: string, username: string) => Promise<void>;
  logout: () => void;
}

export const AuthContext = createContext<AuthContextType | undefined>(undefined);

interface AuthProviderProps {
  children: ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps) {
  const [token, setToken] = useState<string | null>(null);
  const [username, setUsername] = useState<string | null>(null);
  const [role, setRole] = useState<UserRole | null>(null);
  const navigate = useNavigate();

  // Register the 401 handler once on mount so that expired-session responses
  // trigger a clean logout + navigation instead of a full page reload.
  useEffect(() => {
    registerUnauthorizedHandler(() => {
      setToken(null);
      setUsername(null);
      setRole(null);
      setApiToken(null);
      navigate('/login', { state: { sessionExpired: true } });
    });
  }, [navigate]);

  const login = async (newToken: string, newUsername: string) => {
    setToken(newToken);
    setUsername(newUsername);
    // Fetch role from the server so we don't trust client-provided claims.
    try {
      const me = await getMe();
      setRole((me.role as UserRole) ?? 'viewer');
    } catch {
      setRole('viewer');
    }
  };

  const logout = () => {
    setToken(null);
    setUsername(null);
    setRole(null);
    setApiToken(null);
  };

  return (
    <AuthContext.Provider value={{ token, username, role, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}
