/**
 * Shared app configuration
 * ========================
 * Centralises the backend API base URL so every file reads from one place.
 *
 * Resolution order:
 *  1. EXPO_PUBLIC_BACKEND_URL env-var (set at build-time by Expo / Railway)
 *  2. Hard-coded production backend on Railway
 */

export const API_URL: string =
  process.env.EXPO_PUBLIC_BACKEND_URL || 'https://richstox-backend.up.railway.app';
