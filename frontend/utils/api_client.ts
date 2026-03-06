/**
 * RICHSTOX Authenticated Fetch Client
 * ====================================
 * Drop-in wrapper around the native `fetch` that handles 401 Unauthorized
 * responses by silently refreshing the session via POST /api/auth/refresh.
 *
 * Key features:
 *  - Single-flight refresh lock: if N concurrent requests all receive a 401,
 *    only ONE refresh call is made; the others wait in a promise queue.
 *  - Token rotation: on a successful refresh the new session token is persisted
 *    to localStorage and all queued callers receive it.
 *  - Hard logout: if the refresh itself fails (expired/invalid refresh token),
 *    localStorage is cleared and the user is redirected to /login.
 *  - HttpOnly cookie: the refresh token is stored in an HttpOnly cookie set by
 *    the backend — it is never accessible from JS (XSS-safe). The `credentials:
 *    'include'` option on the refresh call is the only requirement here.
 */

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL || '';
const SESSION_TOKEN_KEY = 'richstox_session_token';
const USER_DATA_KEY = 'richstox_user_data';

// ---------------------------------------------------------------------------
// Single-flight state (module-level singletons — survive React re-renders)
// ---------------------------------------------------------------------------

type RefreshSubscriber = {
  resolve: (newToken: string) => void;
  reject: (err: unknown) => void;
};

let _isRefreshing = false;
let _refreshQueue: RefreshSubscriber[] = [];

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function _readToken(): string | null {
  try {
    return localStorage.getItem(SESSION_TOKEN_KEY);
  } catch {
    return null;
  }
}

function _persistToken(token: string): void {
  try {
    localStorage.setItem(SESSION_TOKEN_KEY, token);
  } catch {}
}

function _clearAuth(): void {
  try {
    localStorage.removeItem(SESSION_TOKEN_KEY);
    localStorage.removeItem(USER_DATA_KEY);
  } catch {}
}

function _forceLogout(): void {
  _clearAuth();
  if (typeof window !== 'undefined') {
    window.location.href = '/login';
  }
}

/**
 * Trigger a token refresh.  Only one in-flight refresh is allowed at a time.
 * Additional callers are enqueued and resolved/rejected once the single refresh
 * completes.
 */
function _refreshSession(): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    if (_isRefreshing) {
      _refreshQueue.push({ resolve, reject });
      return;
    }

    _isRefreshing = true;

    fetch(`${API_URL}/api/auth/refresh`, {
      method: 'POST',
      credentials: 'include', // sends HttpOnly refresh_token cookie
    })
      .then(async (res) => {
        if (!res.ok) {
          throw new Error(`refresh_failed:${res.status}`);
        }
        const data: { token: string } = await res.json();
        const newToken = data.token;

        _persistToken(newToken);

        // Resolve all waiting callers
        _refreshQueue.forEach((s) => s.resolve(newToken));
        _refreshQueue = [];
        resolve(newToken);
      })
      .catch((err) => {
        // Reject all waiting callers, then force logout
        _refreshQueue.forEach((s) => s.reject(err));
        _refreshQueue = [];
        reject(err);
        _forceLogout();
      })
      .finally(() => {
        _isRefreshing = false;
      });
  });
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Authenticated fetch wrapper.
 *
 * Usage (drop-in replacement for fetch):
 *   const res = await authenticatedFetch('/api/admin/overview', {}, sessionToken);
 *
 * @param url     Absolute or relative URL.
 * @param options Standard RequestInit options (headers will be merged).
 * @param token   The current Bearer session token (from AuthContext / props).
 *                If null the request is sent without an Authorization header,
 *                which will likely yield a 401 that triggers a refresh attempt.
 */
export async function authenticatedFetch(
  url: string,
  options: RequestInit = {},
  token: string | null = _readToken(),
): Promise<Response> {
  const authHeaders: HeadersInit = token
    ? { Authorization: `Bearer ${token}` }
    : {};

  const mergedOptions: RequestInit = {
    ...options,
    credentials: 'include',
    headers: { ...options.headers, ...authHeaders },
  };

  const response = await fetch(url, mergedOptions);

  if (response.status !== 401) {
    return response;
  }

  // Never intercept 401s from the refresh endpoint itself — that would cause
  // an infinite loop and must hard-logout immediately instead.
  if (url.includes('/api/auth/refresh')) {
    _forceLogout();
    return response;
  }

  // --- 401 received — attempt silent refresh ---
  try {
    const newToken = await _refreshSession();

    // Retry the original request with the new token
    const retryOptions: RequestInit = {
      ...options,
      credentials: 'include',
      headers: {
        ...options.headers,
        Authorization: `Bearer ${newToken}`,
      },
    };
    return fetch(url, retryOptions);
  } catch {
    // Refresh failed — _forceLogout() was already called inside _refreshSession.
    // Return the original 401 response so callers can handle it if they want.
    return response;
  }
}
