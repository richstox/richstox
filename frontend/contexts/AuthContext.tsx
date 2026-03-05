/**
 * RICHSTOX Auth Context
 * =====================
 * Google OAuth via direct Google APIs (no Emergent dependency).
 * Web-compatible storage using localStorage.
 */

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL || '';

const SESSION_TOKEN_KEY = 'richstox_session_token';
const USER_DATA_KEY = 'richstox_user_data';

export interface User {
  user_id: string;
  email: string;
  name: string;
  picture?: string | null;
  role: 'admin' | 'user';
  timezone?: string | null;
  country?: string | null;
  created_at?: string;
  subscription_tier?: 'free' | 'pro' | 'pro_plus';
}

interface AuthContextType {
  user: User | null;
  isLoading: boolean;
  isAuthenticated: boolean;
  isAdmin: boolean;
  login: () => void;
  logout: () => Promise<void>;
  processSessionId: (sessionId: string) => Promise<boolean>;
  updateTimezone: (timezone: string, country?: string) => Promise<void>;
  sessionToken: string | null;
  refreshSession: () => Promise<void>;
  devLogin: () => Promise<void>;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

function getStorage(key: string): string | null {
  try { return localStorage.getItem(key); } catch { return null; }
}

function setStorage(key: string, value: string): void {
  try { localStorage.setItem(key, value); } catch {}
}

function removeStorage(key: string): void {
  try { localStorage.removeItem(key); } catch {}
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [sessionToken, setSessionToken] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    checkExistingSession();
  }, []);

  const checkExistingSession = async () => {
    try {
      const storedToken = getStorage(SESSION_TOKEN_KEY);
      if (storedToken) {
        const response = await fetch(`${API_URL}/api/auth/me`, {
          headers: { 'Authorization': `Bearer ${storedToken}` },
          credentials: 'include',
        });
        if (response.ok) {
          const userData = await response.json();
          setUser(userData);
          setSessionToken(storedToken);
          setStorage(USER_DATA_KEY, JSON.stringify(userData));
        } else {
          removeStorage(SESSION_TOKEN_KEY);
          removeStorage(USER_DATA_KEY);
        }
      }
    } catch (error) {
      console.error('Error checking session:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const refreshSession = async () => {
    setIsLoading(true);
    await checkExistingSession();
  };

  const login = () => {
    window.location.href = `${API_URL}/api/auth/google`;
  };

  const processSessionId = async (sessionId: string): Promise<boolean> => {
    try {
      setIsLoading(true);
      const response = await fetch(`${API_URL}/api/auth/session`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ session_id: sessionId }),
      });
      if (!response.ok) return false;
      const data = await response.json();
      setStorage(SESSION_TOKEN_KEY, data.session_token);
      setStorage(USER_DATA_KEY, JSON.stringify(data.user));
      setUser(data.user);
      setSessionToken(data.session_token);
      return true;
    } catch (error) {
      console.error('Error processing session:', error);
      return false;
    } finally {
      setIsLoading(false);
    }
  };

  const logout = async () => {
    try {
      await fetch(`${API_URL}/api/auth/logout`, {
        method: 'POST',
        headers: sessionToken ? { 'Authorization': `Bearer ${sessionToken}` } : {},
        credentials: 'include',
      });
    } catch (error) {
      console.error('Error logging out:', error);
    }
    removeStorage(SESSION_TOKEN_KEY);
    removeStorage(USER_DATA_KEY);
    setUser(null);
    setSessionToken(null);
    window.location.href = '/login';
  };

  const updateTimezone = async (timezone: string, country?: string) => {
    if (!sessionToken) return;
    try {
      const response = await fetch(`${API_URL}/api/auth/timezone`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${sessionToken}`,
        },
        credentials: 'include',
        body: JSON.stringify({ timezone, country }),
      });
      if (response.ok) {
        const updatedUser = await response.json();
        setUser(updatedUser);
        setStorage(USER_DATA_KEY, JSON.stringify(updatedUser));
      }
    } catch (error) {
      console.error('Error updating timezone:', error);
    }
  };

  const devLogin = async () => {
    try {
      setIsLoading(true);
      const response = await fetch(`${API_URL}/api/auth/dev-login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });
      if (!response.ok) throw new Error('Dev login failed');
      const data = await response.json();
      setStorage(SESSION_TOKEN_KEY, data.session_token);
      setStorage(USER_DATA_KEY, JSON.stringify(data.user));
      setUser(data.user);
      setSessionToken(data.session_token);
    } catch (error) {
      console.error('Dev login error:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const value: AuthContextType = {
    user,
    isLoading,
    isAuthenticated: !!user,
    isAdmin: user?.role === 'admin',
    login,
    logout,
    processSessionId,
    updateTimezone,
    sessionToken,
    refreshSession,
    devLogin,
  };

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
