import axios from 'axios';

export const apiClient = axios.create({
  baseURL: '',
});

let currentToken: string | null = null;

// Called by AuthContext.login() and AuthContext.logout() to keep the
// module-level token in sync with the in-memory auth state.
export const setApiToken = (token: string | null) => {
  currentToken = token;
};

export const getApiToken = (): string | null => currentToken;

apiClient.interceptors.request.use((config) => {
  if (currentToken) {
    config.headers.Authorization = `Bearer ${currentToken}`;
  }
  return config;
});

// 401 handler callback — set by AuthProvider on mount so that this module
// can trigger a clean logout and router navigation without importing React
// context or the router directly (which would create circular dependencies).
let onUnauthorized: (() => void) | null = null;

export const registerUnauthorizedHandler = (handler: () => void) => {
  onUnauthorized = handler;
};

apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    const hadToken = Boolean(error.config?.headers?.Authorization);
    if (error.response?.status === 401 && hadToken && onUnauthorized) {
      onUnauthorized();
    }
    return Promise.reject(error);
  }
);
