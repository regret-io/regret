"use client";

import { createContext, useContext, useState, useCallback, useEffect, type ReactNode } from "react";

interface AuthState {
  isAdmin: boolean;
  password: string | null;
  login: (password: string) => void;
  logout: () => void;
}

const AuthContext = createContext<AuthState>({
  isAdmin: false,
  password: null,
  login: () => {},
  logout: () => {},
});

const STORAGE_KEY = "regret_admin_password";

export function AuthProvider({ children }: { children: ReactNode }) {
  const [password, setPassword] = useState<string | null>(null);

  useEffect(() => {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) setPassword(stored);
  }, []);

  const login = useCallback((pw: string) => {
    localStorage.setItem(STORAGE_KEY, pw);
    setPassword(pw);
  }, []);

  const logout = useCallback(() => {
    localStorage.removeItem(STORAGE_KEY);
    setPassword(null);
  }, []);

  return (
    <AuthContext.Provider value={{ isAdmin: !!password, password, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}

export function authHeaders(password: string | null): Record<string, string> {
  if (!password) return {};
  const encoded = btoa(`:${password}`);
  return { Authorization: `Basic ${encoded}` };
}
