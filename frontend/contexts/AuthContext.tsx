/**
 * RICHSTOX Auth Context
 * =====================
 * Google OAuth via direct Google APIs (no Emergent dependency).
 */

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';
import AsyncStorage from '@react-native-async-storage/async-storage';
import { Platform } from 'react-native';

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL || '';

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

const SESSION_TOKEN_KEY = 'richstox_session_token';
const USER_DATA_KEY = 'richstox_user_data';

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [sessionToken, setSessionToken] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    checkExistingSession();
  }, []);

  const checkExistingSession = async () => {
    try {
      let storedToken: string | null = null;
      storedToken = await AsyncStorage.getItem(SESSION_TOKEN_KEY);

      if (!storedToken && Platform.OS === 'web' && typeof document !== 'undefined') {
        const cookies = document.cookie.split(';');
        for (const cookie of cookies) {
          const [name, value] = cookie.trim().split('=');
          if (name === 'session_token') {
            storedToken = value;
            break;
          }
        }
      }

      if (storedToken) {
        const response = await fetch(`${API_URL}/api/auth/me`, {
          headers: { 'Authorization': `Bearer ${storedToken}` },
          credentials: 'include',
        });

        if (response.ok) {
          const userData = await response.json();
          setUser(userData);
          setSessionToken(storedToken);
          await AsyncStorage.setItem(SESSION_TOKEN_KEY, storedToken);
          await AsyncStorage.setItem(USER_DATA_KEY, JSON.stringify(userData));
        } else {
          await AsyncStorage.removeItem(SESSION_TOKEN_KEY);
          await AsyncStorage.removeItem(USER_DATA_KEY);
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
    if (Platform.OS === 'web' && typeof window !== 'undefined') {
      // Redirect to backend Google OAuth
      window.location.href = `${API_URL}/api/auth/google`;
    }
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
      await AsyncStorage.setItem(SESSION_TOKEN_KEY, data.session_token);
      await AsyncStorage.setItem(USER_DATA_KEY, JSON.stringify(data.user));
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
    await AsyncStorage.removeItem(SESSION_TOKEN_KEY);
    await AsyncStorage.removeItem(USER_DATA_KEY);
    setUser(null);
    setSessionToken(null);
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
        await AsyncStorage.setItem(USER_DATA_KEY, JSON.stringify(updatedUser));
      }
    } catch (error) {
      console.error('Error updating timezone:', error);
    }
  };

  const devLogin = async () => {
    setIsLoading(true);
    try {
      const response = await fetch(`${API_URL}/api/auth/dev-login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
      });
      if (!response.ok) return;
      const data = await response.json();
      await AsyncStorage.setItem(SESSION_TOKEN_KEY, data.session_token);
      await AsyncStorage.setItem(USER_DATA_KEY, JSON.stringify(data.user));
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
