/**
 * RICHSTOX Auth Context
 * =====================
 * Global authentication state management using React Context.
 * 
 * Features:
 * - Google OAuth via Emergent Auth
 * - Session persistence
 * - Admin role detection
 * - Timezone onboarding
 * 
 * REMINDER: DO NOT HARDCODE THE URL, OR ADD ANY FALLBACKS OR REDIRECT URLS, THIS BREAKS THE AUTH
 */

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';
import AsyncStorage from '@react-native-async-storage/async-storage';
import { Platform } from 'react-native';
import * as WebBrowser from 'expo-web-browser';
import * as AuthSession from 'expo-auth-session';

// Warm up browser for faster OAuth
WebBrowser.maybeCompleteAuthSession();

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL || '';

// User type
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

// Auth context type
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
  devLogin: () => Promise<void>;  // Dev login for testing
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

// Storage keys
const SESSION_TOKEN_KEY = 'richstox_session_token';
const USER_DATA_KEY = 'richstox_user_data';

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [sessionToken, setSessionToken] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  // Check for existing session on mount
  useEffect(() => {
    checkExistingSession();
  }, []);

  // Auto-login in dev mode
  useEffect(() => {
    const autoDevLogin = async () => {
      // If no user after initial check, auto-login as admin in dev
      if (!isLoading && !user) {
        console.log('No user found, attempting dev auto-login...');
        await devLogin();
      }
    };
    autoDevLogin();
  }, [isLoading, user]);

  const checkExistingSession = async () => {
    console.log('Checking existing session...');
    try {
      let storedToken: string | null = null;
      let storedUser: string | null = null;
      
      // Try AsyncStorage first (works on mobile and web)
      storedToken = await AsyncStorage.getItem(SESSION_TOKEN_KEY);
      storedUser = await AsyncStorage.getItem(USER_DATA_KEY);
      
      // On web, also check for cookie-based session
      if (!storedToken && Platform.OS === 'web' && typeof document !== 'undefined') {
        // Try to get session from cookie (set by backend)
        const cookies = document.cookie.split(';');
        for (const cookie of cookies) {
          const [name, value] = cookie.trim().split('=');
          if (name === 'session_token') {
            storedToken = value;
            console.log('Found session token in cookie');
            break;
          }
        }
      }
      
      console.log('Stored token exists:', !!storedToken);
      console.log('Stored user exists:', !!storedUser);
      
      if (storedToken) {
        // Validate session with backend
        const response = await fetch(`${API_URL}/api/auth/me`, {
          headers: {
            'Authorization': `Bearer ${storedToken}`,
          },
          credentials: 'include',
        });
        
        console.log('Auth/me response status:', response.status);
        
        if (response.ok) {
          const userData = await response.json();
          console.log('User data loaded:', userData.email);
          setUser(userData);
          setSessionToken(storedToken);
          // Also save to AsyncStorage for next time
          await AsyncStorage.setItem(SESSION_TOKEN_KEY, storedToken);
          await AsyncStorage.setItem(USER_DATA_KEY, JSON.stringify(userData));
        } else {
          // Session expired, clear storage
          console.log('Session expired, clearing...');
          await AsyncStorage.removeItem(SESSION_TOKEN_KEY);
          await AsyncStorage.removeItem(USER_DATA_KEY);
        }
      } else if (storedUser) {
        // Try to parse stored user (fallback)
        console.log('Using stored user data');
        setUser(JSON.parse(storedUser));
      }
    } catch (error) {
      console.error('Error checking session:', error);
    } finally {
      setIsLoading(false);
    }
  };
  
  // Expose refresh function
  const refreshSession = async () => {
    setIsLoading(true);
    await checkExistingSession();
  };

  // REMINDER: DO NOT HARDCODE THE URL, OR ADD ANY FALLBACKS OR REDIRECT URLS, THIS BREAKS THE AUTH
  const login = async () => {
    if (Platform.OS === 'web' && typeof window !== 'undefined') {
      // Web: use window.location.origin
      const redirectUrl = window.location.origin + '/auth/callback';
      const authUrl = `https://auth.emergentagent.com/?redirect=${encodeURIComponent(redirectUrl)}`;
      console.log('Auth URL:', authUrl);
      window.location.href = authUrl;
    } else {
      // Mobile: use Expo's redirect URI which returns to the app
      const redirectUri = AuthSession.makeRedirectUri({
        scheme: 'richstox',
        path: 'auth/callback',
      });
      
      console.log('Redirect URI:', redirectUri);
      
      const authUrl = `https://auth.emergentagent.com/?redirect=${encodeURIComponent(redirectUri)}`;
      console.log('Auth URL:', authUrl);
      
      try {
        const result = await WebBrowser.openAuthSessionAsync(authUrl, redirectUri);
        console.log('Auth result type:', result.type);
        console.log('Auth result:', JSON.stringify(result));
        
        if (result.type === 'success' && result.url) {
          // Extract session_id from the returned URL
          const url = result.url;
          console.log('Returned URL:', url);
          
          // session_id can be in hash (#session_id=...) or query (?session_id=...)
          let sessionId = null;
          
          if (url.includes('#session_id=')) {
            const match = url.match(/#session_id=([^&]+)/);
            sessionId = match ? match[1] : null;
          } else if (url.includes('session_id=')) {
            const match = url.match(/session_id=([^&]+)/);
            sessionId = match ? match[1] : null;
          }
          
          if (sessionId) {
            console.log('Got session_id, processing...');
            const success = await processSessionId(sessionId);
            console.log('processSessionId result:', success);
          } else {
            console.log('No session_id in URL:', url);
          }
        } else if (result.type === 'dismiss') {
          console.log('User dismissed the auth flow');
        } else {
          console.log('Auth result was not success:', result.type);
        }
      } catch (error) {
        console.error('WebBrowser error:', error);
      }
    }
  };

  const processSessionId = async (sessionId: string): Promise<boolean> => {
    try {
      setIsLoading(true);
      
      const response = await fetch(`${API_URL}/api/auth/session`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({ session_id: sessionId }),
      });
      
      if (!response.ok) {
        throw new Error('Failed to exchange session ID');
      }
      
      const data = await response.json();
      
      // Store session token
      await AsyncStorage.setItem(SESSION_TOKEN_KEY, data.session_token);
      await AsyncStorage.setItem(USER_DATA_KEY, JSON.stringify(data.user));
      
      setUser(data.user);
      setSessionToken(data.session_token);
      
      return true;
    } catch (error) {
      console.error('Error processing session ID:', error);
      return false;
    } finally {
      setIsLoading(false);
    }
  };

  const logout = async () => {
    try {
      // Call logout endpoint
      await fetch(`${API_URL}/api/auth/logout`, {
        method: 'POST',
        headers: sessionToken ? { 'Authorization': `Bearer ${sessionToken}` } : {},
        credentials: 'include',
      });
    } catch (error) {
      console.error('Error logging out:', error);
    }
    
    // Clear local storage
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

  // Dev login - directly sets admin session for testing
  const devLogin = async () => {
    console.log('Dev login - calling API...');
    setIsLoading(true);
    
    try {
      const response = await fetch(`${API_URL}/api/auth/dev-login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
      });
      
      if (!response.ok) {
        throw new Error('Dev login failed');
      }
      
      const data = await response.json();
      console.log('Dev login response:', data);
      
      // Save to storage
      await AsyncStorage.setItem(SESSION_TOKEN_KEY, data.session_token);
      await AsyncStorage.setItem(USER_DATA_KEY, JSON.stringify(data.user));
      
      // Update state
      setUser(data.user);
      setSessionToken(data.session_token);
      
      console.log('Dev login complete - logged in as admin');
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
