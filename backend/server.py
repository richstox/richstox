# ⚠️ BINDING RULE: VISIBLE UNIVERSE FILTER
# ============================================
# The ONLY runtime filter for ticker visibility is: is_visible == True
# NO ad-hoc filters allowed (exchange, suffix, sector, industry, asset_type, etc.)
# Violation = data integrity breach = users see wrong tickers
# 
# Use VISIBLE_UNIVERSE_QUERY constant defined in server.py line 60
# or whitelist_service.py line 710
#
# If you need to filter tickers, add a NEW field to tracked_tickers schema
# and get explicit user approval FIRST.
# ============================================

"""
RICHSTOX Backend API
====================
Complete backend with:
- EODHD API integration for live stock data
- Financial calculations (benchmark, buy & hold, portfolio value)
- Stock detail pages with all metrics
- Admin dashboard

=============================================================================
VISIBLE UNIVERSE RULE (PERMANENT)
=============================================================================
Only tickers with is_visible=true may appear anywhere in the app.

is_visible = is_seeded && has_price_data && has_classification

Where:
- is_seeded: NYSE/NASDAQ + Common Stock
- has_price_data: appears in daily bulk prices
- has_classification: sector AND industry are non-empty

All app queries MUST use VISIBLE_UNIVERSE_QUERY, never is_active alone.
=============================================================================
"""

from fastapi import FastAPI, APIRouter, HTTPException, BackgroundTasks, Query
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from admin_middleware import AdminAuthMiddleware
from auth_guard import UserAuthMiddleware
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime, timedelta, timezone
import asyncio
import httpx
import json
import hashlib
import re

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# =============================================================================
# A3/A5: UNIFIED CONFIG + HARD STARTUP GUARD
# =============================================================================
from config import get_mongo_url, get_db_name, validate_env_db_match, get_db_host, get_env

# Validate ENV/DB_NAME match BEFORE connecting (logs to stdout/stderr)
validate_env_db_match()

# MongoDB connection (using unified config)
mongo_url = get_mongo_url()
client = AsyncIOMotorClient(mongo_url)
db = client[get_db_name()]

# Log to richstox logger (now available after app setup)
logger_startup = logging.getLogger("richstox")
logger_startup.info(f"✅ Database connected: ENV={get_env()}, DB={get_db_name()}, Host={get_db_host()}")

# ==============================================================================
# SINGLE SOURCE OF TRUTH: VISIBLE UNIVERSE QUERY
# ==============================================================================
# This is the ONLY filter that should be used to query tickers visible in the app.
# Do NOT use is_active, exchange in ["NYSE","NASDAQ"], or asset_type alone.
# ==============================================================================
VISIBLE_UNIVERSE_QUERY = {"is_visible": True}

# Create the main app
app = FastAPI(title="RICHSTOX API")

# Create router with /api prefix
api_router = APIRouter(prefix="/api")

# =============================================================================
# RUNTIME EODHD GUARD
# =============================================================================
# App runtime NEVER calls EODHD. All data comes from MongoDB only.
# Only scheduler/backfill jobs may call EODHD.
#
# Any runtime EODHD call is a BUG. Fix it immediately.
# =============================================================================

# ========== CONFIGURATION ==========
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")

# S&P 500 benchmark ticker
SP500_TICKER = "GSPC.INDX"  # S&P 500 Index
SP500_TR_TICKER = "SP500TR.INDX"  # S&P 500 Total Return Index

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("richstox")

# =============================================================================
# STARTUP: VERIFY SP500TR BENCHMARK DATA EXISTS (DB-ONLY, NO API CALLS)
# =============================================================================
@app.on_event("startup")
async def verify_sp500tr_benchmark():
    """Verify SP500TR.INDX benchmark data exists. DB-only, no EODHD calls."""
    ticker = "SP500TR.INDX"
    
    count = await db.stock_prices.count_documents({"ticker": ticker})
    if count >= 9000:
        logger.info(f"SP500TR.INDX benchmark data OK: {count} records")
    else:
        logger.warning(f"SP500TR.INDX benchmark data incomplete: {count} records. Run scheduler job to backfill.")


# =============================================================================
# LAYER 2: STARTUP API CALL GUARD
# =============================================================================
# Runs /app/scripts/audit_external_calls.py at startup.
# Stores result to ops_audit_runs collection for Admin Panel.
# =============================================================================
@app.on_event("startup")
async def startup_api_call_guard():
    """
    LAYER 2: Run audit script at startup. Log result to ops_audit_runs.
    SCHEDULER-ONLY RULE: No runtime endpoints may call external APIs.
    """
    import subprocess
    from datetime import datetime, timezone
    
    try:
        result = subprocess.run(
            ["python", "/app/scripts/audit_external_calls.py"],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        passed = result.returncode == 0
        
        audit_result = {
            "audit_type": "api_call_guard",
            "ran_at": datetime.now(timezone.utc),
            "exit_code": result.returncode,
            "passed": passed,
            "stdout": result.stdout[:2000] if result.stdout else None,
            "stderr": result.stderr[:500] if result.stderr else None,
        }
        
        # Store to ops collection for Admin Panel
        await db.ops_audit_runs.update_one(
            {"audit_type": "api_call_guard"},
            {"$set": audit_result},
            upsert=True
        )
        
        if passed:
            logger.info("✅ API Call Guard: PASS - All EODHD calls in allowlist")
        else:
            logger.critical(f"🚨 API Call Guard: FAIL - Violations found!")
            logger.critical(result.stdout[:500] if result.stdout else "No output")
            
    except subprocess.TimeoutExpired:
        logger.warning("⚠️ API Call Guard: Timeout (script took >30s)")
    except FileNotFoundError:
        logger.warning("⚠️ API Call Guard: Script not found at /app/scripts/audit_external_calls.py")
    except Exception as e:
        logger.warning(f"⚠️ API Call Guard: Error running audit - {e}")


# =============================================================================
# LAYER 3: STARTUP VISIBILITY GUARD (DATA SUPREMACY MANIFESTO v1.0)
# =============================================================================
# BINDING: Startup Guard fails if canonical sieve count != is_visible count
# =============================================================================
@app.on_event("startup")
async def startup_visibility_guard():
    """
    BINDING: Startup Guard must fail if is_visible count != canonical sieve count.
    Canonical sieve:
    - SEEDING: exchange ∈ {NYSE, NASDAQ} AND asset_type == "Common Stock"
    - ACTIVITY: has_price_data == true
    - QUALITY: sector AND industry present
    - STATUS: is_delisted != true
    """
    from visibility_rules import get_canonical_sieve_query, VISIBLE_TICKERS_QUERY
    
    canonical_count = await db.tracked_tickers.count_documents(get_canonical_sieve_query())
    is_visible_count = await db.tracked_tickers.count_documents(VISIBLE_TICKERS_QUERY)
    
    mismatch = abs(canonical_count - is_visible_count)
    
    if mismatch > 0:
        error_msg = f"🚨 VISIBILITY MISMATCH: canonical_sieve={canonical_count}, is_visible={is_visible_count}, diff={mismatch}"
        logger.error(error_msg)
        
        # Log audit failure
        await db.ops_audit_runs.update_one(
            {"audit_type": "visibility_guard"},
            {"$set": {
                "audit_type": "visibility_guard",
                "status": "FAIL",
                "canonical_count": canonical_count,
                "is_visible_count": is_visible_count,
                "mismatch": mismatch,
                "timestamp": datetime.now(timezone.utc),
                "action_required": "Run recompute_visibility_all job from Admin Panel"
            }},
            upsert=True
        )
        
        # WARNING: Do not fail startup - just log critical error
        # User must run visibility cleanup job to fix
        logger.warning("⚠️ Run 'recompute_visibility_all' job from Admin Panel to fix mismatch")
    else:
        logger.info(f"✅ Visibility Guard: {is_visible_count} tickers (canonical sieve match)")
        
        # Log audit success
        await db.ops_audit_runs.update_one(
            {"audit_type": "visibility_guard"},
            {"$set": {
                "audit_type": "visibility_guard",
                "status": "PASS",
                "canonical_count": canonical_count,
                "is_visible_count": is_visible_count,
                "mismatch": 0,
                "timestamp": datetime.now(timezone.utc),
            }},
            upsert=True
        )


# =============================================================================

# ========== MODELS ==========

class TrackedTicker(BaseModel):
    ticker: str
    name: Optional[str] = ""
    exchange: str = "US"
    sector: Optional[str] = None
    industry: Optional[str] = None
    is_active: bool = True
    is_visible: bool = True  # VISIBLE UNIVERSE FLAG
    added_at: datetime = Field(default_factory=datetime.utcnow)

class StockPrice(BaseModel):
    ticker: str
    date: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    adjusted_close: float
    volume: int

class PortfolioCreate(BaseModel):
    name: str

class PositionCreate(BaseModel):
    portfolio_id: str
    ticker: str
    shares: float
    buy_price: float
    buy_date: str

# ========== MONGO READ HELPERS (replace runtime EODHD calls) ==========

def _normalize_ticker(ticker: str) -> str:
    """Normalize ticker to the canonical DB format (e.g. 'AAPL' -> 'AAPL.US')."""
    t = ticker.upper().strip()
    if not t.endswith(".US") and "." not in t:
        return f"{t}.US"
    return t


async def _get_prices_from_db(ticker: str, days: int = 365, from_date: str = None) -> List[dict]:
    """
    Read historical EOD prices from MongoDB stock_prices collection.
    Returns list of dicts sorted by date ascending, matching the shape
    previously returned by the inline EODHDService.
    """
    ticker_full = _normalize_ticker(ticker)
    query: dict = {"ticker": ticker_full}
    if from_date:
        query["date"] = {"$gte": from_date}
    else:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        query["date"] = {"$gte": cutoff}

    cursor = db.stock_prices.find(
        query,
        {"_id": 0}
    ).sort("date", 1).limit(days + 60)  # margin covers weekends/holidays in the date window

    return await cursor.to_list(length=days + 60)


async def _get_latest_price_from_db(ticker: str) -> Optional[dict]:
    """Get the most recent price record for a ticker from stock_prices."""
    ticker_full = _normalize_ticker(ticker)
    return await db.stock_prices.find_one(
        {"ticker": ticker_full},
        {"_id": 0},
        sort=[("date", -1)]
    )


async def _get_fundamentals_from_db(ticker: str) -> dict:
    """
    Read fundamentals from MongoDB.
    Tries company_fundamentals_cache first, falls back to tracked_tickers.fundamentals.
    Returns a dict compatible with EODHD fundamentals shape (General, Highlights, Technicals, etc.)
    or an empty dict if nothing is available.
    """
    ticker_full = _normalize_ticker(ticker)

    # 1. Try company_fundamentals_cache
    cache_doc = await db.company_fundamentals_cache.find_one(
        {"ticker": ticker_full}, {"_id": 0}
    )
    if cache_doc:
        # If the cache stores EODHD-shaped data, return as-is
        if cache_doc.get("General"):
            return cache_doc
        # Otherwise build a compat dict from flat fields
        return {
            "General": {
                "Name": cache_doc.get("name"),
                "Exchange": cache_doc.get("exchange"),
                "Sector": cache_doc.get("sector"),
                "Industry": cache_doc.get("industry"),
                "Description": cache_doc.get("description"),
            },
            "Highlights": {},
            "Technicals": {},
        }

    # 2. Fall back to tracked_tickers.fundamentals
    tracked = await db.tracked_tickers.find_one(
        {"ticker": ticker_full}, {"_id": 0, "fundamentals": 1, "name": 1, "sector": 1, "industry": 1}
    )
    if tracked and tracked.get("fundamentals"):
        return tracked["fundamentals"]
    if tracked:
        return {
            "General": {
                "Name": tracked.get("name"),
                "Sector": tracked.get("sector"),
                "Industry": tracked.get("industry"),
            },
            "Highlights": {},
            "Technicals": {},
        }

    return {}


async def _get_dividends_from_db(ticker: str, from_date: str = None) -> List[dict]:
    """
    Read dividend history from MongoDB dividend_history collection.
    Returns list of dicts with date/value keys.
    """
    ticker_full = _normalize_ticker(ticker)
    query: dict = {"ticker": ticker_full}
    if from_date:
        query["$or"] = [
            {"ex_date": {"$gte": from_date}},
            {"date": {"$gte": from_date}}
        ]

    cursor = db.dividend_history.find(query, {"_id": 0}).sort(
        [("ex_date", -1), ("date", -1)]
    )
    raw = await cursor.to_list(length=500)

    # Normalize to consistent shape expected by callers
    result = []
    for d in raw:
        div_date = d.get("ex_date") or d.get("date")
        amount = d.get("amount") or d.get("value") or 0
        result.append({"date": div_date, "value": amount, "dividend": amount})
    return result


# ========== CALCULATION HELPERS (imported from calculators_service) ==========

from calculators_service import (
    calculate_cagr,
    calculate_max_drawdown,
    calculate_pain_details,
    calculate_volatility,
    calculate_52w_high_low,
)

# ========== API ENDPOINTS ==========

@api_router.get("/")
async def root():
    return {
        "message": "RICHSTOX API",
        "version": "2.0",
        "mode": "DB_ONLY",
        "api_calls": 0
    }

# ----- Health Check -----
@api_router.get("/health")
async def health_check():
    return {"status": "healthy", "mode": "DB_ONLY"}

# ----- Authentication Endpoints -----

from auth_service import (
    exchange_session_id,
    exchange_google_code,
    create_or_update_user,
    create_session,
    create_refresh_token,
    consume_refresh_token,
    delete_refresh_tokens_for_user,
    validate_session,
    delete_session,
    is_admin,
    update_user_timezone,
    seed_admin_user,
    get_session_token_from_request,
    User,
    serialize_user,
    GOOGLE_CLIENT_ID,
    REFRESH_TOKEN_EXPIRY_DAYS,
)
from fastapi import Request, Response

class SessionRequest(BaseModel):
    """Request to exchange session_id for session_token."""
    session_id: str

class TimezoneUpdate(BaseModel):
    """Request to update user timezone."""
    timezone: str
    country: Optional[str] = None

@api_router.get("/auth/me")
async def auth_me(request: Request):
    """
    Get current user data from session.
    
    Validates session token from cookie or Authorization header.
    """
    session_token = get_session_token_from_request(request)
    
    if not session_token:
        raise HTTPException(401, "Not authenticated")
    
    user = await validate_session(db, session_token)
    
    if not user:
        raise HTTPException(401, "Session expired or invalid")
    
    user_data = serialize_user(user)
    return User(**user_data).dict()

@api_router.post("/auth/logout")
async def auth_logout(request: Request, response: Response):
    """Logout: delete session + refresh token, clear both cookies."""
    session_token = get_session_token_from_request(request)
    if session_token:
        user = await validate_session(db, session_token)
        if user:
            await delete_refresh_tokens_for_user(db, user["user_id"])
        await delete_session(db, session_token)

    response.delete_cookie(key="session_token", path="/", secure=True, samesite="none")
    response.delete_cookie(key="refresh_token", path="/api/auth/refresh", secure=True, samesite="none")
    return {"message": "Logged out"}


@api_router.post("/auth/refresh")
async def auth_refresh(request: Request, response: Response):
    """
    Silently refresh a session using the long-lived HttpOnly refresh token cookie.

    Flow:
      1. Read refresh token from HttpOnly cookie (never from body/header — XSS safe).
      2. Validate + consume the token (one-time use; a new one is issued on success).
      3. Create a new session token (7-day) and a new refresh token (30-day).
      4. Return {"token": "<new_session_token>"} as JSON.
         The new refresh token is set as an HttpOnly cookie.
      5. On failure: 401 (frontend must redirect to login).
    """
    refresh_token_value = request.cookies.get("refresh_token")
    if not refresh_token_value:
        raise HTTPException(status_code=401, detail="No refresh token provided.")

    user = await consume_refresh_token(db, refresh_token_value)
    if not user:
        response.delete_cookie(key="refresh_token", path="/api/auth/refresh", secure=True, samesite="none")
        raise HTTPException(status_code=401, detail="Refresh token invalid or expired. Please sign in again.")

    user_id = user["user_id"]
    new_session_token = f"session_{uuid.uuid4().hex}"
    await create_session(db, user_id, new_session_token)

    new_refresh_token = await create_refresh_token(db, user_id)

    response.set_cookie(
        key="session_token",
        value=new_session_token,
        httponly=True,
        secure=True,
        samesite="none",
        path="/",
        max_age=7 * 24 * 60 * 60,
    )
    response.set_cookie(
        key="refresh_token",
        value=new_refresh_token,
        httponly=True,
        secure=True,
        samesite="none",
        path="/api/auth/refresh",
        max_age=REFRESH_TOKEN_EXPIRY_DAYS * 24 * 60 * 60,
    )

    return {"token": new_session_token}

@api_router.get("/auth/google")
async def auth_google_login(request: Request):
    """Redirect to Google OAuth."""
    from urllib.parse import urlencode
    base_url = str(request.base_url).rstrip("/").replace("http://", "https://")
    redirect_uri = f"{base_url}/api/auth/google/callback"
    frontend_url = os.environ.get("FRONTEND_URL", "https://jocular-faun-27ea7b.netlify.app")
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "offline",
    }
    google_url = f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url=google_url)

@api_router.get("/auth/google/callback")
async def auth_google_callback(code: str, request: Request, response: Response):
    """Handle Google OAuth callback."""
    from fastapi.responses import RedirectResponse
    base_url = str(request.base_url).rstrip("/").replace("http://", "https://")
    redirect_uri = f"{base_url}/api/auth/google/callback"
    frontend_url = os.environ.get("FRONTEND_URL", "https://jocular-faun-27ea7b.netlify.app")
    
    auth_data = await exchange_google_code(code, redirect_uri)
    if not auth_data:
        return RedirectResponse(url=f"{frontend_url}/?error=auth_failed")

    user = await create_or_update_user(db, auth_data)
    user_data = serialize_user(user)
    session_token = auth_data.get("session_token")
    await create_session(db, user_data["user_id"], session_token)
    refresh_token_value = await create_refresh_token(db, user_data["user_id"])

    response_redirect = RedirectResponse(url=f"{frontend_url}/auth/callback?session_id={session_token}")
    response_redirect.set_cookie(
        key="session_token",
        value=session_token,
        httponly=True,
        secure=True,
        samesite="none",
        path="/",
        max_age=7 * 24 * 60 * 60,
    )
    response_redirect.set_cookie(
        key="refresh_token",
        value=refresh_token_value,
        httponly=True,
        secure=True,
        samesite="none",
        path="/api/auth/refresh",
        max_age=REFRESH_TOKEN_EXPIRY_DAYS * 24 * 60 * 60,
    )
    return response_redirect

@api_router.post("/auth/session")
async def auth_session_token(body: dict, response: Response):
    """Exchange session_id (which is now session_token) for user data."""
    session_id = body.get("session_id", "")
    user = await validate_session(db, session_id)
    if not user:
        raise HTTPException(401, "Invalid session")
    user_data = serialize_user(user)
    response.set_cookie(
        key="session_token",
        value=session_id,
        httponly=True,
        secure=True,
        samesite="none",
        path="/",
        max_age=7 * 24 * 60 * 60
    )
    return {"user": User(**user_data).dict(), "session_token": session_id}

@api_router.put("/auth/timezone")
async def auth_update_timezone(request: Request, data: TimezoneUpdate):
    """
    Update user's timezone (onboarding step).
    """
    session_token = get_session_token_from_request(request)
    
    if not session_token:
        raise HTTPException(401, "Not authenticated")
    
    user = await validate_session(db, session_token)
    
    if not user:
        raise HTTPException(401, "Session expired or invalid")
    
    updated_user = await update_user_timezone(db, user["user_id"], data.timezone, data.country)
    
    user_data = serialize_user(updated_user)
    return User(**user_data).dict()

@api_router.post("/admin/auth/seed-admin")
async def admin_seed_admin():
    """
    Seed admin user (kurtarichard@gmail.com).
    
    This creates or updates the admin user with role='admin'.
    """
    admin = await seed_admin_user(db)
    return serialize_user(admin)

@api_router.post("/auth/dev-login")
async def auth_dev_login(response: Response):
    """
    Dev Login - Quick admin login for development/testing.
    
    Bypasses OAuth to log in as admin user directly.
    Creates session and returns user data.
    
    SECURITY: Only enabled when DEV_LOGIN_ENABLED=1 in environment.
    Must be OFF in production.
    """
    import uuid
    
    # Check if dev login is enabled via environment variable
    dev_login_enabled = os.environ.get("DEV_LOGIN_ENABLED", "0") == "1"
    if not dev_login_enabled:
        raise HTTPException(403, "Dev login is disabled. Set DEV_LOGIN_ENABLED=1 to enable.")
    
    # Seed or get admin user
    admin = await seed_admin_user(db)
    
    if not admin:
        raise HTTPException(500, "Failed to get admin user")

    session_token = f"dev_session_{uuid.uuid4().hex}"
    await create_session(db, admin["user_id"], session_token)
    refresh_token_value = await create_refresh_token(db, admin["user_id"])
    user_data = serialize_user(admin)

    response.set_cookie(
        key="session_token",
        value=session_token,
        httponly=True,
        secure=True,
        samesite="none",
        path="/",
        max_age=7 * 24 * 60 * 60,
    )
    response.set_cookie(
        key="refresh_token",
        value=refresh_token_value,
        httponly=True,
        secure=True,
        samesite="none",
        path="/api/auth/refresh",
        max_age=REFRESH_TOKEN_EXPIRY_DAYS * 24 * 60 * 60,
    )

    return {
        "user": User(**user_data).dict(),
        "session_token": session_token,
        "message": "Dev login successful - logged in as admin",
    }

# ----- Stock Search -----
@api_router.get("/search")
async def search_stocks(q: str = Query(..., min_length=1)):
    """
    Search for stocks by ticker or company name.
    Searches in local database (tracked_tickers) for visible tickers.
    
    Priority:
    1. Exact ticker match (e.g., "BRK" matches "BRK-A.US", "BRK-B.US")
    2. Ticker starts with query
    3. Company name contains query
    """
    query = q.upper().strip()
    
    # Build regex for flexible matching
    # Remove .US suffix if user included it
    search_term = query.replace(".US", "").replace("-", "[-.]?")
    
    # Search in tracked_tickers for visible stocks
    # Match: ticker contains query OR name contains query (case-insensitive)
    pipeline = [
        {
            "$match": {
                "is_visible": True,
                "$or": [
                    # Ticker matches (with flexible hyphen/dot)
                    {"ticker": {"$regex": f"^{search_term}", "$options": "i"}},
                    {"ticker": {"$regex": search_term, "$options": "i"}},
                    # Company name contains query
                    {"name": {"$regex": q, "$options": "i"}},
                    {"fundamentals.General.Name": {"$regex": q, "$options": "i"}}
                ]
            }
        },
        {
            "$addFields": {
                # Priority scoring for sorting
                "sort_score": {
                    "$cond": [
                        # Exact ticker match (highest priority)
                        {"$regexMatch": {"input": "$ticker", "regex": f"^{search_term}[-.]", "options": "i"}},
                        1,
                        {
                            "$cond": [
                                # Ticker starts with query
                                {"$regexMatch": {"input": "$ticker", "regex": f"^{search_term}", "options": "i"}},
                                2,
                                3  # Name match (lowest priority)
                            ]
                        }
                    ]
                }
            }
        },
        {"$sort": {"sort_score": 1, "ticker": 1}},
        {"$limit": 30},
        {
            "$project": {
                "_id": 0,
                "ticker": {"$replaceAll": {"input": "$ticker", "find": ".US", "replacement": ""}},
                "full_ticker": "$ticker",
                "name": {"$ifNull": ["$fundamentals.General.Name", "$name"]},
                "exchange": {"$ifNull": ["$fundamentals.General.Exchange", "$exchange"]}
            }
        }
    ]
    
    results = await db.tracked_tickers.aggregate(pipeline).to_list(length=30)
    
    return {"query": q, "count": len(results), "results": results}

# ----- Stock Detail (Vizitka) -----
@api_router.get("/stock/{ticker}")
async def get_stock_detail(ticker: str):
    """
    Get complete stock detail - "vizitka akcie".
    Includes: current price, fundamentals, performance metrics, dividend info.
    """
    ticker = ticker.upper()
    
    # Get price history (1 year) from MongoDB
    prices = await _get_prices_from_db(ticker, days=365)
    
    if not prices:
        raise HTTPException(404, f"No data found for {ticker}")
    
    # Get fundamentals from MongoDB
    fundamentals = await _get_fundamentals_from_db(ticker)
    
    # Get dividends from MongoDB
    dividends = await _get_dividends_from_db(ticker)
    
    # Calculate metrics
    latest_price = prices[-1] if prices else {}
    current_price = latest_price.get("adjusted_close", latest_price.get("close", 0))
    
    # 1-year return
    if len(prices) > 1:
        start_price = prices[0].get("adjusted_close", prices[0].get("close", 1))
        year_return = ((current_price - start_price) / start_price * 100) if start_price > 0 else 0
    else:
        year_return = 0
    
    # 52-week high/low
    high_52w, low_52w = calculate_52w_high_low(prices)
    
    # Max drawdown
    max_drawdown = calculate_max_drawdown(prices)
    
    # Volatility
    volatility = calculate_volatility(prices)
    
    # CAGR (based on available data)
    years = len(prices) / 252  # Trading days to years
    if years > 0 and len(prices) > 1:
        start_price = prices[0].get("adjusted_close", prices[0].get("close", 1))
        cagr = calculate_cagr(start_price, current_price, years)
    else:
        cagr = 0
    
    # Dividend yield
    total_dividends = sum(d.get("value", d.get("dividend", 0)) for d in dividends[:4])  # Last 4 quarters
    dividend_yield = (total_dividends / current_price * 100) if current_price > 0 else 0
    
    # General info from fundamentals
    general = fundamentals.get("General", {})
    highlights = fundamentals.get("Highlights", {})
    technicals = fundamentals.get("Technicals", {})
    
    return {
        "ticker": ticker,
        "name": general.get("Name", ticker),
        "exchange": general.get("Exchange", "US"),
        "sector": general.get("Sector", "Unknown"),
        "industry": general.get("Industry", "Unknown"),
        "description": general.get("Description", "")[:500] if general.get("Description") else "",
        
        # Price data
        "current_price": round(current_price, 2),
        "price_date": latest_price.get("date", ""),
        "previous_close": round(prices[-2].get("close", current_price), 2) if len(prices) > 1 else current_price,
        "daily_change": round(current_price - (prices[-2].get("close", current_price) if len(prices) > 1 else current_price), 2),
        "daily_change_pct": round(((current_price / prices[-2].get("close", current_price)) - 1) * 100, 2) if len(prices) > 1 and prices[-2].get("close", 0) > 0 else 0,
        
        # Performance metrics
        "return_1y": round(year_return, 2),
        "cagr": round(cagr, 2),
        "max_drawdown": round(max_drawdown, 2),
        "volatility": round(volatility, 2),
        "high_52w": round(high_52w, 2),
        "low_52w": round(low_52w, 2),
        
        # Fundamentals
        "market_cap": highlights.get("MarketCapitalization", 0),
        "pe_ratio": highlights.get("PERatio", 0),
        "eps": highlights.get("EarningsShare", 0),
        "beta": technicals.get("Beta", 0),
        
        # Dividends
        "dividend_yield": round(dividend_yield, 2),
        "last_dividend": dividends[0].get("value", 0) if dividends else 0,
        "dividend_count_year": len([d for d in dividends if d.get("date", "")[:4] == str(datetime.now().year)]),
        
        # Meta
        "data_source": "MongoDB",
        "last_updated": datetime.utcnow().isoformat()
    }

# ----- Price History -----
@api_router.get("/stock/{ticker}/prices")
async def get_stock_prices(
    ticker: str,
    days: int = Query(365, ge=1, le=3650),
    from_date: str = None
):
    """Get historical prices for a stock."""
    ticker = ticker.upper()
    prices = await _get_prices_from_db(ticker, days=days, from_date=from_date)
    
    return {
        "ticker": ticker,
        "count": len(prices),
        "from_date": prices[0].get("date") if prices else None,
        "to_date": prices[-1].get("date") if prices else None,
        "prices": prices
    }

# ----- Benchmark Data (S&P 500) -----
@api_router.get("/benchmark")
async def get_benchmark(days: int = Query(365, ge=1, le=3650)):
    """Get S&P 500 benchmark data for comparison."""
    prices = await _get_prices_from_db(SP500_TR_TICKER, days=days)
    
    if not prices:
        raise HTTPException(404, "Benchmark data not available")
    
    # Calculate benchmark metrics
    start_price = prices[0].get("adjusted_close", prices[0].get("close", 1))
    end_price = prices[-1].get("adjusted_close", prices[-1].get("close", 1))
    
    total_return = ((end_price - start_price) / start_price * 100) if start_price > 0 else 0
    max_dd = calculate_max_drawdown(prices)
    volatility = calculate_volatility(prices)
    
    years = len(prices) / 252
    cagr = calculate_cagr(start_price, end_price, years) if years > 0 else 0
    
    return {
        "name": "S&P 500 (SPY)",
        "ticker": SP500_TR_TICKER,
        "current_value": round(end_price, 2),
        "start_value": round(start_price, 2),
        "total_return": round(total_return, 2),
        "cagr": round(cagr, 2),
        "max_drawdown": round(max_dd, 2),
        "volatility": round(volatility, 2),
        "days": len(prices),
        "prices": prices
    }

# ----- Buy & Hold Calculator -----
@api_router.get("/calculator/buy-hold")
async def calculate_buy_hold(
    ticker: str,
    initial_investment: float = Query(10000, gt=0),
    start_date: str = None,
    end_date: str = None,
    days: int = Query(365, ge=1, le=3650)
):
    """
    Calculate Buy & Hold performance.
    Shows what would happen if you invested a lump sum and held.
    """
    ticker = ticker.upper()
    
    # Get price history from MongoDB
    prices = await _get_prices_from_db(ticker, days=days, from_date=start_date)
    
    if not prices:
        raise HTTPException(404, f"No price data for {ticker}")
    
    # Get benchmark for comparison from MongoDB
    benchmark_prices = await _get_prices_from_db(SP500_TR_TICKER, days=days, from_date=start_date)
    
    # Calculate for stock
    start_price = prices[0].get("adjusted_close", prices[0].get("close", 1))
    end_price = prices[-1].get("adjusted_close", prices[-1].get("close", 1))
    
    shares_bought = initial_investment / start_price if start_price > 0 else 0
    final_value = shares_bought * end_price
    total_return = final_value - initial_investment
    total_return_pct = (total_return / initial_investment * 100) if initial_investment > 0 else 0
    
    # Calculate for benchmark
    if benchmark_prices:
        bench_start = benchmark_prices[0].get("adjusted_close", benchmark_prices[0].get("close", 1))
        bench_end = benchmark_prices[-1].get("adjusted_close", benchmark_prices[-1].get("close", 1))
        bench_return_pct = ((bench_end - bench_start) / bench_start * 100) if bench_start > 0 else 0
    else:
        bench_return_pct = 0
    
    # Calculate CAGR
    years = len(prices) / 252
    cagr = calculate_cagr(start_price, end_price, years) if years > 0 else 0
    
    # Build equity curve
    equity_curve = []
    for p in prices:
        price = p.get("adjusted_close", p.get("close", 0))
        value = shares_bought * price
        equity_curve.append({
            "date": p.get("date"),
            "value": round(value, 2)
        })
    
    return {
        "ticker": ticker,
        "strategy": "Buy & Hold",
        "initial_investment": initial_investment,
        "shares_bought": round(shares_bought, 4),
        "buy_price": round(start_price, 2),
        "buy_date": prices[0].get("date"),
        "current_price": round(end_price, 2),
        "current_date": prices[-1].get("date"),
        "final_value": round(final_value, 2),
        "total_return": round(total_return, 2),
        "total_return_pct": round(total_return_pct, 2),
        "cagr": round(cagr, 2),
        "max_drawdown": round(calculate_max_drawdown(prices), 2),
        "benchmark_return_pct": round(bench_return_pct, 2),
        "vs_benchmark": round(total_return_pct - bench_return_pct, 2),
        "days_held": len(prices),
        "equity_curve": equity_curve
    }

# ----- DCA (Dollar Cost Averaging) Calculator -----
@api_router.get("/calculator/dca")
async def calculate_dca(
    ticker: str,
    monthly_investment: float = Query(500, gt=0),
    days: int = Query(365, ge=1, le=3650)
):
    """
    Calculate Dollar Cost Averaging (DCA) performance.
    Shows what would happen with regular monthly investments.
    """
    ticker = ticker.upper()
    
    # Get price history from MongoDB
    prices = await _get_prices_from_db(ticker, days=days)
    
    if not prices:
        raise HTTPException(404, f"No price data for {ticker}")
    
    # Group prices by month and take first trading day of each month
    monthly_prices = {}
    for p in prices:
        month_key = p.get("date", "")[:7]  # YYYY-MM
        if month_key not in monthly_prices:
            monthly_prices[month_key] = p
    
    # Calculate DCA
    total_invested = 0
    total_shares = 0
    investment_history = []
    
    for month_key in sorted(monthly_prices.keys()):
        price_data = monthly_prices[month_key]
        price = price_data.get("adjusted_close", price_data.get("close", 0))
        
        if price > 0:
            shares_this_month = monthly_investment / price
            total_shares += shares_this_month
            total_invested += monthly_investment
            
            investment_history.append({
                "date": price_data.get("date"),
                "price": round(price, 2),
                "shares_bought": round(shares_this_month, 4),
                "total_shares": round(total_shares, 4),
                "total_invested": round(total_invested, 2),
                "current_value": round(total_shares * price, 2)
            })
    
    # Final calculations
    final_price = prices[-1].get("adjusted_close", prices[-1].get("close", 0))
    final_value = total_shares * final_price
    total_return = final_value - total_invested
    total_return_pct = (total_return / total_invested * 100) if total_invested > 0 else 0
    
    # Average cost basis
    avg_cost = total_invested / total_shares if total_shares > 0 else 0
    
    return {
        "ticker": ticker,
        "strategy": "Dollar Cost Averaging (DCA)",
        "monthly_investment": monthly_investment,
        "months_invested": len(investment_history),
        "total_invested": round(total_invested, 2),
        "total_shares": round(total_shares, 4),
        "average_cost": round(avg_cost, 2),
        "current_price": round(final_price, 2),
        "final_value": round(final_value, 2),
        "total_return": round(total_return, 2),
        "total_return_pct": round(total_return_pct, 2),
        "investment_history": investment_history
    }

# ----- Portfolio Value Calculator -----
@api_router.get("/calculator/portfolio-value")
async def calculate_portfolio_value(
    target_value: float = Query(100000, gt=0),
    tickers: str = Query("AAPL,MSFT,GOOGL"),
    weights: str = Query("40,30,30"),
    days: int = Query(365, ge=1, le=3650)
):
    """
    Calculate historical portfolio value.
    Shows what the portfolio would be worth if current value was target_value.
    Useful for: "If my portfolio is worth $100,000 today, what was it worth 1 year ago?"
    """
    ticker_list = [t.strip().upper() for t in tickers.split(",")]
    weight_list = [float(w.strip()) for w in weights.split(",")]
    
    if len(ticker_list) != len(weight_list):
        raise HTTPException(400, "Number of tickers must match number of weights")
    
    # Normalize weights to sum to 100
    total_weight = sum(weight_list)
    weight_list = [w / total_weight * 100 for w in weight_list]
    
    # Get prices for each ticker from MongoDB
    all_prices = {}
    for ticker in ticker_list:
        prices = await _get_prices_from_db(ticker, days=days)
        if prices:
            for p in prices:
                date = p.get("date")
                if date not in all_prices:
                    all_prices[date] = {}
                all_prices[date][ticker] = p.get("adjusted_close", p.get("close", 0))
    
    if not all_prices:
        raise HTTPException(404, "No price data found")
    
    # Get current (latest) prices
    latest_date = max(all_prices.keys())
    current_prices = all_prices[latest_date]
    
    # Calculate shares for each ticker based on target value
    shares = {}
    composition = []
    for ticker, weight in zip(ticker_list, weight_list):
        allocation = target_value * (weight / 100)
        current_price = current_prices.get(ticker, 0)
        shares[ticker] = allocation / current_price if current_price > 0 else 0
        composition.append({
            "ticker": ticker,
            "weight": round(weight, 2),
            "allocation": round(allocation, 2),
            "shares": round(shares[ticker], 4),
            "current_price": round(current_price, 2)
        })
    
    # Calculate historical portfolio values
    equity_curve = []
    for date in sorted(all_prices.keys()):
        day_prices = all_prices[date]
        portfolio_value = sum(
            shares[ticker] * day_prices.get(ticker, 0)
            for ticker in ticker_list
        )
        equity_curve.append({
            "date": date,
            "value": round(portfolio_value, 2)
        })
    
    # Calculate metrics
    start_value = equity_curve[0]["value"] if equity_curve else target_value
    total_return_pct = ((target_value - start_value) / start_value * 100) if start_value > 0 else 0
    
    # Max drawdown on portfolio
    peak = equity_curve[0]["value"]
    max_dd = 0
    for point in equity_curve:
        if point["value"] > peak:
            peak = point["value"]
        dd = (peak - point["value"]) / peak if peak > 0 else 0
        max_dd = max(max_dd, dd)
    
    return {
        "target_value": target_value,
        "tickers": ticker_list,
        "composition": composition,
        "start_date": equity_curve[0]["date"] if equity_curve else None,
        "start_value": start_value,
        "end_date": latest_date,
        "end_value": target_value,
        "total_return_pct": round(total_return_pct, 2),
        "max_drawdown": round(max_dd * 100, 2),
        "equity_curve": equity_curve
    }

# ----- Portfolio Endpoints -----

@api_router.post("/portfolios")
async def create_portfolio(request: Request, portfolio: PortfolioCreate):
    """Create a new portfolio. Auth: UserAuthMiddleware (user_id from session)."""
    user = request.state.user
    portfolio_id = str(uuid.uuid4())
    doc = {
        "id": portfolio_id,
        "name": portfolio.name,
        "user_id": user["user_id"],
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    }
    await db.portfolios.insert_one(doc)
    return {"id": portfolio_id, "name": portfolio.name}

@api_router.get("/portfolios")
async def list_portfolios(request: Request):
    """List all portfolios for the authenticated user."""
    user = request.state.user
    portfolios = await db.portfolios.find(
        {"user_id": user["user_id"]},
        {"_id": 0}
    ).to_list(100)
    return {"count": len(portfolios), "portfolios": portfolios}

@api_router.get("/portfolios/{portfolio_id}")
async def get_portfolio(portfolio_id: str, request: Request):
    """Get portfolio with positions and live valuations. Scoped to authenticated user."""
    user = request.state.user
    portfolio = await db.portfolios.find_one(
        {"id": portfolio_id, "user_id": user["user_id"]}, {"_id": 0}
    )
    if not portfolio:
        raise HTTPException(404, "Portfolio not found")

    positions = await db.positions.find(
        {"portfolio_id": portfolio_id},
        {"_id": 0}
    ).to_list(100)

    total_value = 0
    total_cost = 0
    enriched_positions = []

    for pos in positions:
        ticker = pos.get("ticker", "")
        shares = pos.get("shares", 0)
        buy_price = pos.get("buy_price", 0)

        latest = await _get_latest_price_from_db(ticker)
        current_price = (latest.get("adjusted_close") or latest.get("close", buy_price)) if latest else buy_price

        market_value = shares * current_price
        cost_basis = shares * buy_price
        gain_loss = market_value - cost_basis
        gain_loss_pct = (gain_loss / cost_basis * 100) if cost_basis > 0 else 0

        total_value += market_value
        total_cost += cost_basis

        enriched_positions.append({
            **pos,
            "current_price": round(current_price, 2),
            "market_value": round(market_value, 2),
            "cost_basis": round(cost_basis, 2),
            "gain_loss": round(gain_loss, 2),
            "gain_loss_pct": round(gain_loss_pct, 2)
        })

    portfolio["positions"] = enriched_positions
    portfolio["total_value"] = round(total_value, 2)
    portfolio["total_cost"] = round(total_cost, 2)
    portfolio["total_gain_loss"] = round(total_value - total_cost, 2)
    portfolio["total_gain_loss_pct"] = round(((total_value - total_cost) / total_cost * 100) if total_cost > 0 else 0, 2)

    return portfolio

@api_router.post("/positions")
async def create_position(request: Request, position: PositionCreate):
    """Add a position to a portfolio. Anti-enumeration: 404 if not owned."""
    user = request.state.user
    portfolio = await db.portfolios.find_one(
        {"id": position.portfolio_id, "user_id": user["user_id"]}
    )
    if not portfolio:
        raise HTTPException(404, "Portfolio not found")

    position_id = str(uuid.uuid4())
    doc = {
        "id": position_id,
        **position.dict(),
        "created_at": datetime.utcnow()
    }
    await db.positions.insert_one(doc)
    return {"id": position_id, "ticker": position.ticker}


# =============================================================================
# P33 (BINDING): Watchlist (Follow ⭐) + Portfolio — SEPARATE DATA
# =============================================================================
# DO NOT CHANGE WATCHLIST/PORTFOLIO SEMANTICS WITHOUT RICHARD APPROVAL
# (kurtarichard@gmail.com)
#
# RULES:
# - user_watchlist: stores followed tickers (admin user: kurtarichard@gmail.com)
# - positions: stores portfolio holdings (shares > 0 ONLY)
# - Star ⭐ on ticker detail reads from user_watchlist ONLY
# - Homepage "My Stocks" = union(Watchlist, Portfolio)
# - Each row shows pill: Watchlist / Portfolio / Both
# - "See all" opens Watchlist-only page
# =============================================================================

from zoneinfo import ZoneInfo
PRAGUE_TZ = ZoneInfo("Europe/Prague")


def get_prague_now():
    """Get current datetime in Europe/Prague timezone."""
    return datetime.now(PRAGUE_TZ)


def is_before_market_close_prague():
    """
    Check if current Prague time is before 21:00 (market close cutoff).
    If before 21:00, use today's close price.
    If after 21:00, use next day's close price (when available).
    """
    prague_now = get_prague_now()
    return prague_now.hour < 21


@api_router.get("/v1/watchlist/check/{ticker}")
async def check_if_followed(ticker: str, request: Request):
    """P33: Check if a ticker is in the authenticated user's watchlist."""
    user = request.state.user
    ticker_clean = ticker.upper().replace(".US", "")
    followed = await db.user_watchlist.find_one(
        {"ticker": ticker_clean, "user_id": user["user_id"]}
    )
    return {"ticker": ticker_clean, "is_followed": bool(followed)}


@api_router.get("/v1/positions/check/{ticker}")
async def check_if_followed_legacy(ticker: str, request: Request):
    """Legacy alias for watchlist check."""
    return await check_if_followed(ticker, request)


@api_router.post("/v1/watchlist/{ticker}")
async def follow_ticker(ticker: str, request: Request):
    """P33: Add a ticker to the authenticated user's watchlist."""
    user = request.state.user
    user_id = user["user_id"]
    ticker_clean = ticker.upper().replace(".US", "")

    existing = await db.user_watchlist.find_one(
        {"ticker": ticker_clean, "user_id": user_id}
    )
    if existing:
        return {"ticker": ticker_clean, "status": "already_followed"}

    ticker_full = f"{ticker_clean}.US"
    tracked = await db.tracked_tickers.find_one(
        {"ticker": ticker_full, "is_visible": True}
    )
    if not tracked:
        raise HTTPException(400, f"Ticker {ticker_clean} is not visible or doesn't exist")

    prague_now = get_prague_now()
    follow_price_close = None
    follow_price_date = None

    latest_price = await db.stock_prices.find_one(
        {"ticker": ticker_full},
        sort=[("date", -1)]
    )
    if latest_price:
        follow_price_close = latest_price.get("adjusted_close") or latest_price.get("close")
        follow_price_date = latest_price.get("date")

    doc = {
        "id": str(uuid.uuid4()),
        "user_id": user_id,
        "ticker": ticker_clean,
        "followed_at": prague_now.isoformat(),
        "follow_price_close": follow_price_close,
        "follow_price_date": follow_price_date,
        "created_at": datetime.utcnow()
    }
    await db.user_watchlist.insert_one(doc)

    return {
        "ticker": ticker_clean,
        "status": "followed",
        "followed_at": prague_now.strftime("%d/%m/%Y %H:%M"),
        "follow_price_close": follow_price_close,
        "follow_price_date": follow_price_date,
    }


@api_router.post("/v1/positions/follow/{ticker}")
async def follow_ticker_legacy(ticker: str, request: Request):
    """Legacy alias for watchlist follow."""
    return await follow_ticker(ticker, request)


@api_router.delete("/v1/watchlist/{ticker}")
async def unfollow_ticker(ticker: str, request: Request):
    """P33: Remove a ticker from the authenticated user's watchlist. 404 = anti-enum."""
    user = request.state.user
    ticker_clean = ticker.upper().replace(".US", "")

    result = await db.user_watchlist.delete_one(
        {"ticker": ticker_clean, "user_id": user["user_id"]}
    )
    if result.deleted_count == 0:
        raise HTTPException(404, "Ticker not found in your watchlist")

    return {"ticker": ticker_clean, "status": "unfollowed"}


@api_router.delete("/v1/positions/unfollow/{ticker}")
async def unfollow_ticker_legacy(ticker: str, request: Request):
    """Legacy alias for watchlist unfollow."""
    return await unfollow_ticker(ticker, request)


@api_router.get("/v1/watchlist")
async def get_watchlist(request: Request):
    """
    P33: Get full watchlist with follow details for "See all" page.
    Scoped to the authenticated user.
    """
    user = request.state.user
    watchlist_docs = await db.user_watchlist.find(
        {"user_id": user["user_id"]},
        {"_id": 0}
    ).sort("followed_at", -1).to_list(length=None)
    
    enriched = []
    
    for doc in watchlist_docs:
        ticker = doc.get("ticker")
        ticker_full = f"{ticker}.US"
        
        # Get fundamentals for name/logo
        fundamentals = await db.company_fundamentals_cache.find_one({"ticker": ticker_full})
        
        # Get current price
        latest_price = await db.stock_prices.find_one(
            {"ticker": ticker_full},
            sort=[("date", -1)]
        )
        
        current_price = None
        change_since_follow = None
        
        if latest_price:
            current_price = latest_price.get("adjusted_close") or latest_price.get("close")
            follow_price = doc.get("follow_price_close")
            if follow_price and current_price and follow_price > 0:
                change_since_follow = round(((current_price - follow_price) / follow_price) * 100, 2)
        
        # Build logo URL
        logo_url = None
        if fundamentals and fundamentals.get("logo_url"):
            logo_path = fundamentals.get("logo_url")
            if logo_path.startswith("/"):
                logo_url = f"https://eodhistoricaldata.com{logo_path}"
            else:
                logo_url = logo_path
        
        # Format followed_at to DD/MM/YYYY
        followed_at_str = doc.get("followed_at", "")
        followed_at_display = None
        if followed_at_str:
            try:
                if "T" in followed_at_str:
                    dt = datetime.fromisoformat(followed_at_str.replace("Z", "+00:00"))
                    followed_at_display = dt.strftime("%d/%m/%Y")
                else:
                    followed_at_display = followed_at_str
            except:
                followed_at_display = followed_at_str
        
        enriched.append({
            "ticker": ticker,
            "name": fundamentals.get("name", ticker) if fundamentals else ticker,
            "logo_url": logo_url,
            "followed_at": followed_at_display,
            "follow_price_close": doc.get("follow_price_close"),
            "follow_price_date": doc.get("follow_price_date"),
            "current_price": round(current_price, 2) if current_price else None,
            "change_since_follow": change_since_follow
        })
    
    return {
        "count": len(enriched),
        "watchlist": enriched
    }


# ----- Homepage Data -----
@api_router.get("/homepage")
async def get_homepage_data():
    """
    P33 (BINDING): Homepage data with union of Watchlist + Portfolio.
    
    ==========================================================================
    P33: MY STOCKS = union(Watchlist, Portfolio)
    DO NOT CHANGE WITHOUT RICHARD APPROVAL (kurtarichard@gmail.com)
    ==========================================================================
    
    Rules:
    - Watchlist: from user_watchlist collection
    - Portfolio: from positions with shares > 0
    - Each row shows pill: "Watchlist" / "Portfolio" / "Both"
    - Sorted: Portfolio first, then Watchlist-only
    ==========================================================================
    """
    # --- Phase 1: Parallel fetch of independent setup data ---
    benchmark_task = _get_prices_from_db(SP500_TR_TICKER, days=30)
    watchlist_raw_task = db.user_watchlist.find({}, {"_id": 0}).to_list(length=None)
    portfolio_docs_task = db.positions.find(
        {"shares": {"$gt": 0}},
        {"_id": 0, "ticker": 1}
    ).to_list(length=None)

    benchmark, watchlist_all_docs, portfolio_docs = await asyncio.gather(
        benchmark_task, watchlist_raw_task, portfolio_docs_task
    )

    if benchmark:
        sp_current = benchmark[-1].get("adjusted_close", 0)
        sp_prev = benchmark[-2].get("adjusted_close", sp_current) if len(benchmark) > 1 else sp_current
        sp_change = sp_current - sp_prev
        sp_change_pct = (sp_change / sp_prev * 100) if sp_prev > 0 else 0
        sp_ytd_start = benchmark[0].get("adjusted_close", sp_current)
        sp_ytd_return = ((sp_current - sp_ytd_start) / sp_ytd_start * 100) if sp_ytd_start > 0 else 0
    else:
        sp_current = sp_change = sp_change_pct = sp_ytd_return = 0

    # P33: Build watchlist ticker set + docs lookup from single query
    watchlist_tickers = set()
    watchlist_docs = {}
    for doc in watchlist_all_docs:
        ticker = doc.get("ticker", "").upper()
        if ticker:
            watchlist_tickers.add(ticker.replace(".US", ""))
            watchlist_docs[ticker.replace(".US", "")] = doc

    # P33: Get PORTFOLIO tickers from positions (shares > 0 ONLY)
    portfolio_tickers = set(
        doc["ticker"].upper().replace(".US", "")
        for doc in portfolio_docs
        if doc.get("ticker")
    )

    # P33: Union of both sets
    all_tickers = watchlist_tickers | portfolio_tickers

    # Filter to only visible tickers
    if all_tickers:
        all_full = [f"{t}.US" for t in all_tickers]
        visible_docs = await db.tracked_tickers.find(
            {"ticker": {"$in": all_full}, "is_visible": True},
            {"_id": 0, "ticker": 1}
        ).to_list(length=None)
        visible_set = set(doc["ticker"].replace(".US", "") for doc in visible_docs)
    else:
        visible_set = set()

    # P33: Build enriched list with pill classification
    # Sort: Portfolio first (sorted by ticker), then Watchlist-only (sorted by ticker)
    portfolio_visible = sorted(portfolio_tickers & visible_set)
    watchlist_only_visible = sorted((watchlist_tickers - portfolio_tickers) & visible_set)
    ordered_tickers = portfolio_visible + watchlist_only_visible

    stocks = []

    # --- Phase 2: Batch fetch fundamentals + prices in parallel (no N+1) ---
    all_ticker_dbs = [f"{t}.US" for t in ordered_tickers[:20]]

    if all_ticker_dbs:
        # Single batch query for all fundamentals
        fundamentals_task = db.company_fundamentals_cache.find(
            {"ticker": {"$in": all_ticker_dbs}},
            {"_id": 0, "ticker": 1, "name": 1, "logo_url": 1}
        ).to_list(length=None)

        # Single aggregation for latest 2 prices per ticker (gives us current + previous)
        # 14-day lookback is enough to cover weekends/holidays and guarantee ≥2 trading days
        PRICE_LOOKBACK_DAYS = 14
        price_cutoff = (datetime.now() - timedelta(days=PRICE_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        prices_pipeline = [
            {"$match": {"ticker": {"$in": all_ticker_dbs}, "date": {"$gte": price_cutoff}}},
            {"$sort": {"ticker": 1, "date": -1}},
            {"$group": {
                "_id": "$ticker",
                "prices": {"$push": {
                    "date": "$date",
                    "close": "$close",
                    "adjusted_close": "$adjusted_close"
                }}
            }},
            {"$project": {
                "_id": 1,
                "prices": {"$slice": ["$prices", 2]}
            }}
        ]
        prices_task = db.stock_prices.aggregate(prices_pipeline).to_list(length=None)

        fundamentals_list, prices_agg = await asyncio.gather(
            fundamentals_task, prices_task
        )
    else:
        fundamentals_list = []
        prices_agg = []

    # Build lookup dicts
    fund_map = {doc["ticker"]: doc for doc in fundamentals_list}
    price_map = {}  # ticker_db -> {"current": ..., "prev": ..., "date": ...}
    for doc in prices_agg:
        ticker_db = doc["_id"]
        prices = doc.get("prices", [])
        entry = {"current": 0, "prev": 0, "date": None}
        if prices:
            latest = prices[0]
            entry["current"] = latest.get("adjusted_close") or latest.get("close", 0)
            entry["date"] = latest.get("date")
            if len(prices) > 1:
                prev = prices[1]
                entry["prev"] = prev.get("adjusted_close") or prev.get("close", 0)
        price_map[ticker_db] = entry

    # --- Phase 3: Build response from lookup dicts (no DB calls) ---
    for ticker in ordered_tickers[:20]:
        ticker_db = f"{ticker}.US"
        fundamentals = fund_map.get(ticker_db)
        price_data = price_map.get(ticker_db, {"current": 0, "prev": 0, "date": None})

        current = price_data["current"]
        change_1d_pct = 0
        if current and price_data["prev"] and price_data["prev"] > 0:
            change_1d_pct = ((current - price_data["prev"]) / price_data["prev"]) * 100

        # Build logo URL from EODHD
        logo_url = None
        if fundamentals and fundamentals.get("logo_url"):
            logo_path = fundamentals.get("logo_url")
            if logo_path.startswith("/"):
                logo_url = f"https://eodhistoricaldata.com{logo_path}"
            else:
                logo_url = logo_path

        # P33: Determine pill type
        in_watchlist = ticker in watchlist_tickers
        in_portfolio = ticker in portfolio_tickers

        if in_watchlist and in_portfolio:
            pill = "Both"
        elif in_portfolio:
            pill = "Portfolio"
        else:
            pill = "Watchlist"

        # P37+ Part 3 (G): Add change_since_added for watchlist items
        added_at = None
        change_since_added = None
        follow_price = None

        if ticker in watchlist_docs:
            wl_doc = watchlist_docs[ticker]
            follow_price = wl_doc.get("follow_price_close")

            # Format added_at as DD/MM/YYYY
            followed_at_str = wl_doc.get("followed_at", "")
            if followed_at_str:
                try:
                    if "T" in followed_at_str:
                        dt = datetime.fromisoformat(followed_at_str.replace("Z", "+00:00"))
                        added_at = dt.strftime("%d/%m/%Y")
                    else:
                        added_at = followed_at_str
                except:
                    added_at = None

            # Calculate change since added
            if follow_price and current and follow_price > 0:
                change_since_added = round(((current - follow_price) / follow_price) * 100, 2)

        stocks.append({
            "ticker": ticker,
            "name": fundamentals.get("name", ticker) if fundamentals else ticker,
            "logo_url": logo_url,
            "price": round(current, 2) if current else None,
            "change_1d_pct": round(change_1d_pct, 2),  # P37+: 1D change
            "pill": pill,
            # P37+ Part 3 (G): Added date and change since added
            "added_at": added_at,
            "change_since_added": change_since_added,
            "follow_price": round(follow_price, 2) if follow_price else None
        })
    
    return {
        "last_updated": datetime.utcnow().isoformat(),
        "market_status": "closed" if datetime.utcnow().hour < 14 or datetime.utcnow().hour >= 21 else "open",
        "benchmark": {
            "name": "S&P 500",
            "ticker": SP500_TR_TICKER,
            "value": round(sp_current, 2),
            "change": round(sp_change, 2),
            "change_pct": round(sp_change_pct, 2),
            "ytd_return": round(sp_ytd_return, 2)
        },
        "my_stocks": stocks,
        "watchlist_count": len(watchlist_tickers & visible_set),
        "portfolio_count": len(portfolio_tickers & visible_set),
        "total_count": len(ordered_tickers),
        "data_source": "MongoDB",
        "api_mode": "DB_ONLY"
    }

# ----- Admin Endpoints -----

@api_router.get("/admin/stats")
async def get_admin_stats():
    """Get admin dashboard statistics.
    PERF: Single $facet aggregation replaces 4x sequential count_documents.
    """
    import asyncio as _asyncio

    facet_task = db.users.aggregate([{"$facet": {
        "users":      [{"$count": "n"}],
    }}]).to_list(1)

    other_facet_task = db.portfolios.aggregate([{"$facet": {
        "portfolios": [{"$count": "n"}],
    }}]).to_list(1)

    positions_task = db.positions.aggregate([{"$facet": {
        "positions":  [{"$count": "n"}],
    }}]).to_list(1)

    tracked_task = db.tracked_tickers.count_documents(VISIBLE_UNIVERSE_QUERY)

    users_r, port_r, pos_r, tracked = await _asyncio.gather(
        facet_task, other_facet_task, positions_task, tracked_task
    )

    def _n(r, key): return (r[0].get(key) or [{}])[0].get("n", 0) if r else 0

    return {
        "users": _n(users_r, "users"),
        "portfolios": _n(port_r, "portfolios"),
        "positions": _n(pos_r, "positions"),
        "tracked_tickers": tracked,
        "api_mode": "DB_ONLY",
        "api_calls": 0,
        "cache_files": 0
    }

@api_router.post("/admin/cache/clear")
async def clear_cache():
    """Clear API cache (legacy — file cache no longer used at runtime)."""
    cache_dir = ROOT_DIR / "cache"
    count = 0
    if cache_dir.exists():
        for f in cache_dir.glob("*.json"):
            f.unlink()
            count += 1
    return {"message": f"Cleared {count} cache files"}

@api_router.get("/admin/api-status")
async def api_status():
    """Check EODHD API status."""
    return {
        "mode": "DB_ONLY",
        "api_key_configured": bool(EODHD_API_KEY),
        "requests_made": 0,
        "cache_files": 0
    }

# ----- Admin Report Endpoints (P45) -----

from services.admin_report_service import (
    generate_daily_report as generate_admin_daily_report,
    save_daily_report,
    get_today_report,
    get_report_by_date,
    get_recent_reports,
    run_admin_report_job,
)

@api_router.get("/admin/report/today")
async def admin_report_today():
    """
    P45: Get today's admin report.
    Returns the daily report generated at 06:00 Prague time.
    """
    report = await get_today_report(db)
    
    if not report:
        return {"status": "not_generated", "message": "Today's report not yet generated. Runs at 06:00 Prague."}
    
    return report

@api_router.get("/admin/report/{date}")
async def admin_report_by_date(date: str):
    """
    P45: Get admin report for a specific date.
    
    Args:
        date: Date in YYYY-MM-DD format
    """
    report = await get_report_by_date(db, date)
    
    if not report:
        raise HTTPException(404, f"No report found for {date}")
    
    return report

@api_router.get("/admin/reports/recent")
async def admin_reports_recent(days: int = Query(7, ge=1, le=30)):
    """
    P45: Get recent admin reports (last N days).
    """
    reports = await get_recent_reports(db, days)
    return {"count": len(reports), "reports": reports}

@api_router.post("/admin/report/generate")
async def admin_report_generate_now():
    """
    P45: Manually trigger admin report generation.
    Useful for testing or immediate refresh.
    Sets generation_source = "manual"
    """
    import traceback
    try:
        logger.info("Starting admin report generation (manual)")
        report = await generate_admin_daily_report(db, source="manual")
        logger.info(f"Report generated in {report.get('generation_duration_ms', 0)}ms")
        
        if not report or "report_date" not in report:
            raise HTTPException(500, "Failed to generate report - empty result")
        
        save_result = await save_daily_report(db, report)
        logger.info(f"Report saved: {save_result}")
        
        return {
            "status": "generated",
            "report_date": report["report_date"],
            "generation_source": report.get("generation_source"),
            "generation_duration_ms": report.get("generation_duration_ms"),
            "warnings_count": len(report.get("warnings", [])),
            "save_result": save_result,
            "report": report
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Admin report generation failed: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(500, f"Report generation failed: {str(e)}")

# ----- Whitelist Endpoints -----

from whitelist_service import (
    sync_ticker_whitelist,
    get_whitelist_stats,
    search_whitelist,
    is_ticker_in_whitelist,
    process_fundamentals_events,
    DUPLICATE_REASON_PREFIX,
)

from industry_benchmarks_service import (
    compute_industry_benchmarks,
    get_industry_benchmark,
    get_benchmark_stats,
    compute_valuation_score,
    compute_gradient_color,
)

from dividend_history_service import (
    sync_ticker_dividends,
    sync_batch_dividends,
    calculate_dividend_yield_ttm,
    get_dividend_history_for_ticker,
    get_dividend_stats,
)

from ttm_calculations_service import (
    calculate_ttm_metrics,
    calculate_local_pe_ratio,
    batch_update_ttm_metrics,
    get_enhanced_stock_metrics,
)

from data_gaps_service import (
    DataField,
    log_data_gap,
    check_and_log_gaps,
    get_data_gaps_by_field,
    get_data_gaps_summary,
    scan_all_tickers_for_gaps,
    generate_daily_report,
)

from price_ingestion_service import (
    backfill_ticker_prices,
    backfill_batch_prices,
    sync_daily_prices,
    compute_52w_high_low,
    get_latest_price,
    get_price_stats,
)

@api_router.get("/whitelist/stats")
async def whitelist_stats():
    """
    Get whitelist statistics.
    
    Returns counts by status:
    - active: Tickers with fundamentals (usable in app)
    - pending_fundamentals: Waiting for fundamentals fetch
    - no_fundamentals: EODHD has no fundamentals for this ticker
    - delisted: Removed from exchange
    """
    stats = await get_whitelist_stats(db)
    return stats

@api_router.get("/whitelist/search")
async def whitelist_search(q: str = Query(..., min_length=1), limit: int = Query(20, ge=1, le=100)):
    """
    Search the whitelist for tickers.
    Only returns ACTIVE tickers (those with fundamentals).
    """
    results = await search_whitelist(db, q, limit)
    return {"query": q, "count": len(results), "results": results}

@api_router.get("/whitelist/check/{ticker}")
async def whitelist_check(ticker: str):
    """Check if a ticker is in the active whitelist (has fundamentals)."""
    in_whitelist = await is_ticker_in_whitelist(db, ticker)
    return {"ticker": ticker, "in_whitelist": in_whitelist}

@api_router.post("/admin/backfill-asset-type")
async def admin_backfill_asset_type():
    """
    Backfill asset_type field for all tickers in company_fundamentals_cache.
    Sets asset_type = 'Common Stock' for all existing records.
    """
    # Update all records that don't have asset_type
    result = await db.company_fundamentals_cache.update_many(
        {},
        {"$set": {"asset_type": "Common Stock"}}
    )
    
    # Verify
    total = await db.company_fundamentals_cache.count_documents({})
    with_asset_type = await db.company_fundamentals_cache.count_documents({"asset_type": "Common Stock"})
    
    return {
        "modified_count": result.modified_count,
        "total_tickers": total,
        "with_asset_type": with_asset_type,
        "message": f"Backfilled asset_type for {result.modified_count} tickers"
    }


@api_router.post("/admin/backfill-full-price-history")
async def admin_backfill_full_price_history():
    """
    Recompute process-truth price completeness fields for all visible tickers.

    Canonical truth model (strict proof regime):
      history_download_completed  = history_download_proven_at IS NOT NULL
                                    AND history_download_proven_anchor IS NOT NULL
      history_download_completed_at = history_download_proven_anchor (date string)
      history_download_min_date   = min(stock_prices.date)
      missing_bulk_dates_since_history_download = gaps after anchor
      gap_free_since_history_download = no gaps since anchor

    Also preserves legacy heuristic fields (full_price_history*).
    Idempotent: safe to re-run at any time.
    """
    from services.admin_overview_service import backfill_full_price_history
    return await backfill_full_price_history(db)


@api_router.post("/admin/whitelist/sync")
async def admin_whitelist_sync(dry_run: bool = Query(True)):
    """
    Sync the whitelist with EODHD exchange-symbol-list.
    
    This creates CANDIDATES with status='pending_fundamentals'.
    Tickers become 'active' only after fundamentals are fetched.
    
    Pipeline:
    1. This job creates candidates + queues fundamentals_events
    2. /admin/fundamentals/process fetches fundamentals and activates tickers
    """
    result = await sync_ticker_whitelist(db, dry_run=dry_run)
    return result

async def _run_universe_seed_bg(db):
    """Background task wrapper for universe seed — logs to ops_job_runs with live progress."""
    import uuid as _uuid

    job_id = f"universe_seed_{_uuid.uuid4().hex[:8]}"
    started_at = datetime.now(timezone.utc)
    logger.info(f"Universe Seed started (job_id={job_id})")

    # Insert running sentinel immediately so the pipeline polling endpoint finds
    # a "running" document and the progress bar activates without waiting for completion.
    _sentinel = await db.ops_job_runs.insert_one({
        "job_id": job_id,
        "job_name": "universe_seed",
        "status": "running",
        "source": "admin_manual",
        "triggered_by": "admin_manual",
        "started_at": started_at,
        "started_at_prague": _sched_to_prague_iso(started_at),
        "log_timezone": "Europe/Prague",
        "progress": "Fetching symbols from EODHD…",
        "progress_pct": 0,
    })
    _doc_id = _sentinel.inserted_id

    async def _s1_progress(processed: int, total: int) -> None:
        pct = round(100 * processed / total) if total else 0
        await db.ops_job_runs.update_one(
            {"_id": _doc_id},
            {"$set": {
                "progress": f"Seeding… {processed:,} / {total:,} tickers",
                "progress_processed": processed,
                "progress_total": total,
                "progress_pct": pct,
            }},
        )

    async def _s1_raw_total(raw_rows_total: int) -> None:
        """Write raw total to sentinel as soon as all exchange symbols are fetched."""
        await db.ops_job_runs.update_one(
            {"_id": _doc_id},
            {"$set": {
                "raw_rows_total": raw_rows_total,
                "details.raw_rows_total": raw_rows_total,
            }},
        )

    try:
        result = await sync_ticker_whitelist(
            db, dry_run=False, job_run_id=job_id,
            progress_callback=_s1_progress,
            raw_total_callback=_s1_raw_total,
        )
        status = "completed"
    except Exception as e:
        result = {"error": str(e)}
        status = "failed"
        logger.error(f"Universe Seed failed: {e}")

    finished_at = datetime.now(timezone.utc)
    duration = (finished_at - started_at).total_seconds()
    seeded_total = result.get("seeded_total") or 0
    if status == "completed":
        await db.ops_job_runs.update_one(
            {"_id": _doc_id},
            {"$set": {
                "status": "completed",
                "finished_at": finished_at,
                "finished_at_prague": _sched_to_prague_iso(finished_at),
                "duration_seconds": duration,
                "result": result,
                "details": {
                    "fetched": result.get("fetched") or 0,
                    "raw_rows_total": result.get("raw_rows_total") or 0,
                    "seeded_total": seeded_total,
                    "filtered_out_total_step1": result.get("filtered_out_total_step1") or 0,
                    "fetched_raw_per_exchange": result.get("fetched_raw_per_exchange") or {},
                },
                "progress": f"Completed: {seeded_total:,} seeded",
                "progress_processed": seeded_total,
                "progress_total": seeded_total,
                "progress_pct": 100,
            }},
        )
    else:
        await db.ops_job_runs.update_one(
            {"_id": _doc_id},
            {"$set": {
                "status": "failed",
                "finished_at": finished_at,
                "finished_at_prague": _sched_to_prague_iso(finished_at),
                "duration_seconds": duration,
                "error": result.get("error", "Unknown error"),
                "progress": f"Failed: {result.get('error', 'Unknown error')}",
            }},
        )
    logger.info(f"Universe Seed completed: status={status}, job_id={job_id}")


@api_router.post("/admin/jobs/universe-seed")
async def admin_run_universe_seed(background_tasks: BackgroundTasks):
    """Individually triggering pipeline steps is disabled. Use the full sequential run instead."""
    raise HTTPException(
        status_code=403,
        detail={
            "error": "per_step_run_disabled",
            "message": "Individual step execution is disabled. Use the full sequential pipeline run (Run Full Pipeline Now).",
        },
    )

@api_router.get("/admin/whitelist/preview")
async def admin_whitelist_preview():
    """Preview what would happen if we synced the whitelist now."""
    result = await sync_ticker_whitelist(db, dry_run=True)
    return result

@api_router.post("/admin/fundamentals/process")
async def admin_process_fundamentals(
    batch_size: int = Query(50, ge=1, le=200),
    dry_run: bool = Query(False)
):
    """
    Process pending fundamentals events.
    
    Fetches fundamentals for tickers with status='pending_fundamentals'.
    If successful: activates ticker, caches fundamentals.
    If no data: marks ticker as 'no_fundamentals' (not active).
    
    NOTE: Each ticker costs 10 EODHD API calls!
    batch_size=50 = 500 API calls
    """
    result = await process_fundamentals_events(db, batch_size=batch_size, dry_run=dry_run)
    return result

@api_router.get("/admin/fundamentals/pending")
async def admin_fundamentals_pending():
    """Get count of pending fundamentals events."""
    pending = await db.fundamentals_events.count_documents({"status": "pending"})
    completed = await db.fundamentals_events.count_documents({"status": "completed"})
    no_data = await db.fundamentals_events.count_documents({"status": "no_data"})
    return {
        "pending": pending,
        "completed": completed,
        "no_data": no_data,
        "estimated_api_calls_needed": pending * 10
    }

# ----- Pilot Fundamentals Sync (Normalized Tables) -----

from fundamentals_service import (
    sync_pilot_fundamentals,
    sync_ticker_fundamentals,
    get_fundamentals_stats,
    PILOT_TICKERS
)

@api_router.get("/admin/fundamentals/stats")
async def admin_fundamentals_stats():
    """Get statistics about fundamentals data in normalized tables."""
    stats = await get_fundamentals_stats(db)
    return stats

@api_router.post("/admin/fundamentals/sync-pilot")
async def admin_sync_pilot(dry_run: bool = Query(False)):
    """
    Sync fundamentals for pilot batch (15 tickers).
    
    Tickers: AAPL, MSFT, JNJ, NVDA, TSLA, GOOGL, AMZN, META, NFLX, 
             AVGO, COST, ADBE, ASML, LRCX, CDNS
    
    Creates normalized data in:
    - company_fundamentals_cache (identity + metrics)
    - financials_cache (income/balance/cashflow per period)
    - earnings_history_cache (quarterly EPS)
    - insider_activity_cache (6m aggregated)
    
    Cost: 150 EODHD credits (15 × 10)
    """
    result = await sync_pilot_fundamentals(db, dry_run=dry_run)
    return result

@api_router.post("/admin/fundamentals/sync-ticker/{ticker}")
async def admin_sync_single_ticker(ticker: str):
    """
    Sync fundamentals for a single ticker.
    Cost: 10 EODHD credits
    """
    result = await sync_ticker_fundamentals(db, ticker)
    return result


@api_router.get("/admin/ticker/{ticker}/fundamentals-audit")
async def admin_fundamentals_audit(
    ticker: str,
    live: int = Query(0, ge=0, le=1, description="1 = make a live EODHD call (costs 10 credits); 0 = DB-only check"),
):
    """
    Audit what Step 3 fetched from EODHD and what is persisted in MongoDB.

    live=0 (default): DB-only — zero credits, instant.
    live=1:           One live EODHD call (10 credits) + fetched block + mismatch.
    """
    from fundamentals_service import fetch_fundamentals_from_eodhd

    # Preserve ticker identity; no hyphen stripping
    t = ticker.upper().strip()
    ticker_full = t if t.endswith(".US") else f"{t}.US"

    REQUIRED_TOP_LEVEL = ["General", "SharesStats", "Financials", "Earnings"]

    fetched: Dict[str, Any] = {}
    required_missing: list = []   # blocks visibility gate
    warnings: list = []           # cosmetic — never cause FAIL
    integrity_failures: list = [] # data corruption — causes FAIL
    mismatch: list = []
    raw_data = None
    credits_used = 0

    # ── 1. Optional live EODHD fetch (live=1 only) ───────────────────────────
    if live == 1:
        if not os.getenv("EODHD_API_KEY", ""):
            fetched["error"] = "EODHD_API_KEY not configured — running in MOCK mode"
        else:
            ticker_plain = ticker_full.replace(".US", "")
            raw_data = await fetch_fundamentals_from_eodhd(ticker_plain)
            if not raw_data:
                fetched["error"] = f"EODHD returned no data for {ticker_full}"
            else:
                credits_used = 10
                general    = raw_data.get("General")    or {}
                shares     = raw_data.get("SharesStats") or {}
                financials = raw_data.get("Financials")  or {}
                earnings   = raw_data.get("Earnings")    or {}

                fetched = {
                    "top_level_keys":            sorted(raw_data.keys()),
                    "general_name":              general.get("Name"),
                    "general_sector":            (general.get("Sector")   or "").strip() or None,
                    "general_industry":          (general.get("Industry") or "").strip() or None,
                    "general_currency_code":     general.get("CurrencyCode"),
                    "shares_outstanding_raw":    shares.get("SharesOutstanding"),
                    "financials_sections":       sorted(financials.keys()),
                    "earnings_history_quarters": len(earnings.get("History") or {}),
                }

                for key in REQUIRED_TOP_LEVEL:
                    if not raw_data.get(key):
                        required_missing.append(f"EODHD missing top-level key: {key}")

                if not fetched["general_sector"]:
                    required_missing.append("EODHD General.Sector is empty or null")
                if not fetched["general_industry"]:
                    required_missing.append("EODHD General.Industry is empty or null")
                if not fetched["shares_outstanding_raw"]:
                    required_missing.append("EODHD SharesStats.SharesOutstanding is empty or null")

    # ── 2. MongoDB state ─────────────────────────────────────────────────────
    tracked   = await db.tracked_tickers.find_one({"ticker": ticker_full}, {"_id": 0})
    cache_doc = await db.company_fundamentals_cache.find_one(
        {"ticker": ticker_full},
        {
            "_id": 0,
            "ticker": 1, "code": 1, "name": 1, "exchange": 1,
            "currency_code": 1, "country_iso": 1,
            "website": 1, "logo_url": 1,
            "description": 1,
            "sector": 1, "industry": 1,
            "full_time_employees": 1,
            "ipo_date": 1, "fiscal_year_end": 1, "is_delisted": 1,
        },
    )
    fin_count  = await db.company_financials.count_documents({"ticker": ticker_full})
    earn_count = await db.company_earnings_history.count_documents({"ticker": ticker_full})

    persisted = {
        "company_fundamentals_cache": cache_doc is not None,
        "tracked_ticker_exists":      tracked is not None,
        "financial_rows":             fin_count,
        "earnings_rows":              earn_count,
    }

    # ── 2b. Cache snapshot from company_fundamentals_cache ───────────────────
    if cache_doc:
        desc = cache_doc.get("description") or ""
        cache_snapshot: Dict[str, Any] = {
            "ticker":              cache_doc.get("ticker"),
            "code":                cache_doc.get("code"),
            "name":                cache_doc.get("name"),
            "exchange":            cache_doc.get("exchange"),
            "currency_code":       cache_doc.get("currency_code"),
            "country_iso":         cache_doc.get("country_iso"),
            "sector":              cache_doc.get("sector"),
            "industry":            cache_doc.get("industry"),
            "website":             cache_doc.get("website"),
            "logo_url":            cache_doc.get("logo_url"),
            "description_preview": desc[:200] if desc else None,
            "description_length":  len(desc),
            "full_time_employees": cache_doc.get("full_time_employees"),
            "ipo_date":            cache_doc.get("ipo_date"),
            "fiscal_year_end":     cache_doc.get("fiscal_year_end"),
            "is_delisted":         cache_doc.get("is_delisted"),
        }
    else:
        cache_snapshot = None

    # ── 3. Values check — read directly from tracked_tickers ─────────────────
    values_check: Dict[str, Any] = {
        "sector":                (tracked or {}).get("sector"),
        "industry":              (tracked or {}).get("industry"),
        "shares_outstanding":    (tracked or {}).get("shares_outstanding"),
        "financial_currency":    (tracked or {}).get("financial_currency"),
        "has_classification":    (tracked or {}).get("has_classification"),
        "fundamentals_complete": (tracked or {}).get("fundamentals_complete"),
        "fundamentals_status":   (tracked or {}).get("fundamentals_status"),
    }

    # ── 4. Classify missing fields ────────────────────────────────────────────
    # Visibility-gate fields → required_missing (never FAIL by themselves)
    if not values_check["sector"]:
        required_missing.append("DB tracked_tickers.sector is null/empty")
    if not values_check["industry"]:
        required_missing.append("DB tracked_tickers.industry is null/empty")
    if not values_check["shares_outstanding"]:
        required_missing.append("DB tracked_tickers.shares_outstanding is null/empty")
    if not values_check["financial_currency"]:
        required_missing.append("DB tracked_tickers.financial_currency is null/empty")
    if not values_check["has_classification"]:
        required_missing.append("DB tracked_tickers.has_classification is false or null")
    snap = cache_snapshot or {}
    if not snap.get("name"):
        required_missing.append("company_fundamentals_cache.name is null/empty")
    if not snap.get("sector"):
        required_missing.append("company_fundamentals_cache.sector is null/empty")
    if not snap.get("industry"):
        required_missing.append("company_fundamentals_cache.industry is null/empty")

    # Cosmetic fields → warnings only (never FAIL)
    if not snap.get("website"):
        warnings.append("company_fundamentals_cache.website is null/empty")
    if not snap.get("description_length"):
        warnings.append("company_fundamentals_cache.description is null/empty")
    if not snap.get("logo_url"):
        warnings.append("company_fundamentals_cache.logo_url is null/empty (optional)")

    # ── 5. Data integrity check (live=1): provider-has-data but DB-missing ───
    if raw_data:
        financials_live = raw_data.get("Financials") or {}
        provider_has_fin_periods = any(
            isinstance((financials_live.get(stmt) or {}).get(period), dict)
            and bool((financials_live[stmt])[period])
            for stmt in ("Income_Statement", "Balance_Sheet", "Cash_Flow")
            for period in ("yearly", "quarterly")
            if isinstance(financials_live.get(stmt), dict)
        )
        provider_has_earnings = bool(
            (raw_data.get("Earnings") or {}).get("History")
        )
        if provider_has_fin_periods and fin_count == 0:
            integrity_failures.append(
                "CORRUPTION: provider has financial periods but company_financials has 0 rows"
            )
        if provider_has_earnings and earn_count == 0:
            integrity_failures.append(
                "CORRUPTION: provider has earnings history but company_earnings_history has 0 rows"
            )

    # ── 6. Mismatch (live=1 only) ─────────────────────────────────────────────
    if raw_data and tracked:
        eodhd_sector   = fetched.get("general_sector")
        eodhd_industry = fetched.get("general_industry")
        if eodhd_sector and eodhd_sector != values_check["sector"]:
            mismatch.append(
                f"sector: EODHD='{eodhd_sector}' vs DB='{values_check['sector']}'"
            )
        if eodhd_industry and eodhd_industry != values_check["industry"]:
            mismatch.append(
                f"industry: EODHD='{eodhd_industry}' vs DB='{values_check['industry']}'"
            )
        eodhd_so = fetched.get("shares_outstanding_raw")
        if eodhd_so is not None and eodhd_so != values_check["shares_outstanding"]:
            mismatch.append(
                f"shares_outstanding: EODHD={eodhd_so} vs DB={values_check['shares_outstanding']}"
            )

    # verdict: FAIL only on data integrity problems
    verdict = "FAIL" if (integrity_failures or mismatch) else "PASS"

    # ── Visibility badge and primary funnel reason ────────────────────────────
    is_visible = bool(tracked and tracked.get("is_visible"))
    is_seeded = bool(tracked and tracked.get("exchange") in ("NYSE", "NASDAQ")
                     and tracked.get("asset_type") == "Common Stock")
    has_price = bool(tracked and tracked.get("has_price_data"))
    fund_complete = tracked.get("fundamentals_status") == "complete" if tracked else False

    # Determine primary funnel reason — exactly one label per ticker
    if not tracked:
        funnel_step = "Step 1 Excluded"
        primary_reason = "Ticker not found in tracked_tickers"
    elif not is_seeded:
        funnel_step = "Step 1 Excluded"
        primary_reason = f"exchange={tracked.get('exchange')}, asset_type={tracked.get('asset_type')}"
    elif not has_price:
        funnel_step = "Step 2 No Price"
        primary_reason = "has_price_data is false or missing"
    elif not fund_complete:
        funnel_step = "Step 3 Fundamentals Blocker"
        primary_reason = "fundamentals_status != complete"
    elif not is_visible:
        funnel_step = "Step 3 Visibility Rule"
        primary_reason = "is_visible is false (visibility gate)"
    else:
        funnel_step = "Passed All Steps"
        primary_reason = "Visible ticker — all pipeline gates passed"

    return {
        "ticker":              ticker_full,
        "audit_at":            datetime.now(timezone.utc).isoformat(),
        "live_mode":           live == 1,
        "credits_used":        credits_used,
        "is_visible":          is_visible,
        "funnel_step":         funnel_step,
        "primary_reason":      primary_reason,
        "fetched":             fetched,
        "persisted":           persisted,
        "cache_snapshot":      cache_snapshot,
        "values_check":        values_check,
        "required_missing":    required_missing,
        "warnings":            warnings,
        "integrity_failures":  integrity_failures,
        "missing":             required_missing,   # backwards compat
        "mismatch":            mismatch,
        "verdict":             verdict,
    }


@api_router.post("/admin/ticker/{ticker}/run-fundamentals-sync")
async def admin_run_single_fundamentals_sync(ticker: str):
    """
    Run the full Step 3 persistence pipeline for exactly ONE ticker.
    Delegates to sync_single_ticker_fundamentals (batch_jobs_service) —
    the canonical single-ticker worker — so all fields including
    no_financials_available / no_earnings_available are sourced from there.
    Costs 10 EODHD credits.
    """
    from batch_jobs_service import sync_single_ticker_fundamentals

    if not os.getenv("EODHD_API_KEY", ""):
        raise HTTPException(status_code=503, detail="EODHD_API_KEY not configured — MOCK mode, cannot fetch.")

    t = ticker.upper().strip()
    ticker_full = t if t.endswith(".US") else f"{t}.US"

    r = await sync_single_ticker_fundamentals(db, ticker_full, source_job="manual_single_sync")

    if not r.get("success"):
        raise HTTPException(
            status_code=404 if r.get("error_type") == "no_data" else 500,
            detail=r.get("error") or "Sync failed",
        )

    fin_bw  = r.get("fin_bulk_write")  or {}
    earn_bw = r.get("earn_bulk_write") or {}

    return {
        "ticker":                    ticker_full,
        "synced_at":                 datetime.now(timezone.utc).isoformat(),
        "credits_used":              10,
        "no_financials_available":   r.get("no_financials_available", False),
        "no_earnings_available":     r.get("no_earnings_available",   False),
        "company_fundamentals_cache": {"upserted": 1 if r.get("has_fundamentals") else 0},
        "company_financials": {
            "rows_parsed":    fin_bw.get("rows_parsed",    0),
            "upserted_count": fin_bw.get("upserted_count", 0),
            "modified_count": fin_bw.get("modified_count", 0),
            "matched_count":  fin_bw.get("matched_count",  0),
        },
        "company_earnings_history": {
            "rows_parsed":    earn_bw.get("rows_parsed",    0),
            "upserted_count": earn_bw.get("upserted_count", 0),
            "modified_count": earn_bw.get("modified_count", 0),
            "matched_count":  earn_bw.get("matched_count",  0),
        },
        "insider_activity": {"written": 1 if r.get("has_insider") else 0},
        "tracked_tickers": {
            "fundamentals_status": "complete",
        },
    }


@api_router.get("/admin/fundamentals/pilot-tickers")
async def admin_pilot_tickers():
    """Get list of pilot tickers."""
    return {"tickers": PILOT_TICKERS, "count": len(PILOT_TICKERS)}

# ----- Batch Job Management -----

from batch_jobs_service import (
    run_fundamentals_batch_job,
    get_tickers_for_sync,
    get_job_status,
    get_kill_switch,
    set_kill_switch,
)

@api_router.get("/admin/batch/status")
async def admin_batch_status():
    """Get current batch job status and kill switch state."""
    status = await get_job_status(db)
    return status

@api_router.post("/admin/batch/kill-switch")
async def admin_batch_kill_switch(enabled: bool = Query(...)):
    """Enable or disable kill switch for batch jobs."""
    set_kill_switch(enabled)
    return {"kill_switch_enabled": enabled}

@api_router.get("/admin/batch/pending-tickers")
async def admin_pending_tickers(limit: int = Query(100, ge=1, le=1000)):
    """Get list of tickers pending fundamentals sync."""
    tickers = await get_tickers_for_sync(db, limit=limit)
    return {"count": len(tickers), "tickers": tickers[:100]}  # Show first 100

@api_router.post("/admin/batch/sync-fundamentals")
async def admin_batch_sync_fundamentals(
    limit: int = Query(500, ge=1, le=2000),
    batch_size: int = Query(50, ge=10, le=100),
    background_tasks: BackgroundTasks = None
):
    """
    Start batch fundamentals sync for specified number of tickers.
    
    Args:
        limit: Number of tickers to sync (default 500, max 2000)
        batch_size: Tickers per batch (default 50)
    
    Returns immediately with job info. Check /admin/batch/status for progress.
    """
    # Get tickers to sync (excluding already synced)
    tickers = await get_tickers_for_sync(db, limit=limit)
    
    if not tickers:
        return {
            "message": "No tickers pending sync",
            "already_synced": (await get_fundamentals_stats(db))["company_fundamentals_cache"]["tickers"]
        }
    
    # Run sync (this will take time)
    result = await run_fundamentals_batch_job(
        db,
        tickers=tickers,
        batch_size=batch_size,
        delay_between_batches=0.5,
        job_name=f"fundamentals_batch_{len(tickers)}"
    )
    
    return result

# ----- Industry Benchmarks Endpoints -----

@api_router.post("/admin/benchmarks/compute")
async def admin_compute_benchmarks():
    """
    Compute industry benchmarks from all company fundamentals.
    
    Creates/updates industry_benchmarks collection with median values
    for P/E, P/S, P/B, EV/EBITDA, Net Margin, etc. per industry.
    
    Requires at least 5 companies per industry.
    """
    result = await compute_industry_benchmarks(db)
    return result

@api_router.get("/admin/benchmarks/stats")
async def admin_benchmark_stats():
    """Get statistics about industry benchmarks."""
    stats = await get_benchmark_stats(db)
    return stats

@api_router.get("/benchmarks/{industry}")
async def get_industry_benchmark_detail(industry: str):
    """Get benchmark data for a specific industry."""
    benchmark = await get_industry_benchmark(db, industry)
    if not benchmark:
        raise HTTPException(404, f"No benchmark data for industry: {industry}")
    return benchmark

# ----- Dividend History Endpoints -----

@api_router.post("/admin/dividends/sync-ticker/{ticker}")
async def admin_sync_ticker_dividends(ticker: str):
    """Sync dividend history for a single ticker."""
    result = await sync_ticker_dividends(db, ticker)
    return result

@api_router.post("/admin/dividends/sync-batch")
async def admin_sync_batch_dividends(limit: int = Query(100, ge=1, le=500)):
    """
    Sync dividend history for multiple tickers.
    
    Syncs dividends for tickers that have fundamentals but no dividend history yet.
    Cost: 1 EODHD credit per ticker.
    """
    # Get tickers with fundamentals but no dividend history yet
    all_tickers = await db.company_fundamentals_cache.distinct("ticker")
    synced_tickers = await db.dividend_history.distinct("ticker")
    
    pending = [t for t in all_tickers if t not in synced_tickers][:limit]
    
    if not pending:
        # Try to get any tickers
        pending = all_tickers[:limit]
    
    result = await sync_batch_dividends(db, pending)
    return result

@api_router.get("/admin/dividends/stats")
async def admin_dividend_stats():
    """Get statistics about dividend history collection."""
    stats = await get_dividend_stats(db)
    return stats

@api_router.get("/dividends/{ticker}")
async def get_ticker_dividends(ticker: str):
    """Get dividend history for a ticker with annual aggregation."""
    result = await get_dividend_history_for_ticker(db, ticker)
    return result

# ----- TTM Calculations Endpoints -----

@api_router.post("/admin/ttm/update-batch")
async def admin_update_ttm_batch(limit: int = Query(500, ge=1, le=2000)):
    """
    Update TTM metrics (Net Margin TTM, etc.) for all tickers.
    
    Calculates from quarterly financials and updates company_fundamentals_cache.
    """
    result = await batch_update_ttm_metrics(db, limit=limit)
    return result

@api_router.get("/ttm/{ticker}")
async def get_ttm_metrics(ticker: str):
    """Get TTM metrics for a ticker."""
    result = await calculate_ttm_metrics(db, ticker)
    return result

# ----- Price Ingestion Endpoints -----

@api_router.post("/admin/prices/backfill-ticker/{ticker}")
async def admin_backfill_ticker_prices(ticker: str):
    """
    Backfill full price history for a single ticker.
    Fetches IPO-to-present EOD data from EODHD.
    Cost: 1 API credit.
    """
    result = await backfill_ticker_prices(db, ticker)
    return result

@api_router.post("/admin/prices/backfill-batch")
async def admin_backfill_batch_prices(
    limit: int = Query(100, ge=1, le=500),
    background_tasks: BackgroundTasks = None
):
    """
    Backfill price history for multiple tickers.
    
    Processes tickers that have fundamentals but no price data.
    Cost: 1 API credit per ticker.
    """
    # Get tickers with fundamentals but no/few prices
    all_tickers = await db.company_fundamentals_cache.distinct("ticker")
    
    # Check which already have prices
    tickers_with_prices = await db.stock_prices.distinct("ticker")
    tickers_with_prices_set = set(tickers_with_prices)
    
    # Find tickers missing prices
    missing_prices = [t for t in all_tickers if t not in tickers_with_prices_set][:limit]
    
    if not missing_prices:
        return {"message": "All tickers have price data", "tickers_checked": len(all_tickers)}
    
    result = await backfill_batch_prices(db, missing_prices)
    return result

@api_router.post("/admin/prices/sync-daily")
async def admin_sync_daily_prices():
    """
    Sync latest day prices using EODHD bulk endpoint.
    Cost-efficient: 1 API call for entire exchange.
    """
    result = await sync_daily_prices(db)
    return result

@api_router.post("/admin/prices/backfill-parallel")
async def admin_backfill_parallel(
    limit: int = 100,
    concurrency: int = 10,
    sync: bool = False,
    background_tasks: BackgroundTasks = None
):
    """
    Run parallel price backfill for tickers without full price history.
    
    Args:
        limit: Max tickers to process (default 100, max 500 for sync, unlimited for background)
        concurrency: Concurrent API requests (default 10)
        sync: If True, run synchronously and return results (use for small batches)
    
    Runs in background by default. Check /api/admin/scheduler/status for results.
    """
    from parallel_batch_service import (
        get_tickers_without_full_prices,
        run_parallel_price_backfill,
    )
    
    # Validate params
    concurrency = min(max(concurrency, 1), 10)
    
    if sync:
        limit = min(limit, 500)  # Cap for sync mode
    
    # Get tickers needing backfill
    tickers = await get_tickers_without_full_prices(db, limit=limit if limit > 0 else None)
    
    if not tickers:
        return {
            "status": "no_work",
            "message": "All tickers have full price history",
            "tickers_checked": 0,
        }
    
    # Run synchronously if requested or small batch
    if sync or len(tickers) <= 20:
        result = await run_parallel_price_backfill(
            db,
            tickers,
            concurrency=concurrency,
            job_name="manual_parallel_backfill",
        )
        return result.to_dict()
    
    # Run in background for larger batches
    async def run_backfill():
        await run_parallel_price_backfill(
            db,
            tickers,
            concurrency=concurrency,
            job_name="manual_parallel_backfill",
        )
    
    background_tasks.add_task(run_backfill)
    return {
        "status": "started",
        "message": f"Started parallel backfill for {len(tickers)} tickers with concurrency={concurrency}",
        "tickers_to_process": len(tickers),
        "concurrency": concurrency,
        "safety_stops": {
            "max_runtime_hours": 4,
            "max_error_rate_pct": 5,
            "max_backoff_threshold_sec": 30,
        },
        "check_status": "/api/admin/scheduler/status",
    }


@api_router.post("/admin/prices/backfill-full")
async def admin_backfill_full(
    background_tasks: BackgroundTasks = None
):
    """
    Run FULL price backfill - all remaining tickers until completion.
    
    Safety stops:
    - Rate-limit backoff >30s
    - Error rate >5%
    - Max runtime: 4 hours
    
    Use for nightly full runs. Progress logged every 500 tickers.
    """
    from parallel_batch_service import (
        get_tickers_without_full_prices,
        run_parallel_price_backfill,
    )
    
    CONCURRENCY = 10
    
    # Get ALL tickers needing backfill (no limit)
    tickers = await get_tickers_without_full_prices(db, limit=None)
    
    if not tickers:
        return {
            "status": "no_work",
            "message": "All tickers have full price history ✅",
            "tickers_remaining": 0,
        }
    
    # Run in background
    async def run_full_backfill():
        await run_parallel_price_backfill(
            db,
            tickers,
            concurrency=CONCURRENCY,
            job_name="manual_full_backfill",
            max_runtime_hours=4,
        )
    
    background_tasks.add_task(run_full_backfill)
    return {
        "status": "started",
        "message": f"Started FULL backfill for ALL {len(tickers)} remaining tickers",
        "tickers_to_process": len(tickers),
        "concurrency": CONCURRENCY,
        "safety_stops": {
            "max_runtime_hours": 4,
            "max_error_rate_pct": 5,
            "max_backoff_threshold_sec": 30,
        },
        "progress_log_interval": 500,
        "check_status": "/api/admin/scheduler/status",
    }

@api_router.get("/admin/prices/stats")
async def admin_price_stats():
    """Get statistics about stock_prices collection."""
    stats = await get_price_stats(db)
    return stats

@api_router.get("/prices/{ticker}/52w")
async def get_ticker_52w(ticker: str):
    """
    Get 52-week high/low computed from stock_prices.
    Uses last 252 trading days.
    """
    result = await compute_52w_high_low(db, ticker)
    return result

@api_router.get("/prices/{ticker}/latest")
async def get_ticker_latest_price(ticker: str):
    """Get latest price for a ticker from stock_prices."""
    result = await get_latest_price(db, ticker)
    if not result:
        raise HTTPException(404, f"No price data for {ticker}")
    return result

# ----- Data Gaps Tracking Endpoints -----

@api_router.get("/admin/data-gaps")
async def admin_get_data_gaps(
    field: str = Query(None, description="Filter by field: fundamentals|price|dividends|financials|earnings|benchmark"),
    limit: int = Query(100, ge=1, le=500)
):
    """
    Get data gaps report.
    
    Args:
        field: Optional filter by specific field
        limit: Maximum tickers to return per field
    
    Returns:
        If field specified: gaps for that field
        Otherwise: complete summary across all fields
    """
    if field:
        return await get_data_gaps_by_field(db, field, limit)
    return await get_data_gaps_summary(db)

@api_router.post("/admin/data-gaps/scan")
async def admin_scan_data_gaps(
    limit: int = Query(None, ge=1, le=10000, description="Limit tickers to scan (None = all)")
):
    """
    Scan all tickers and identify data gaps.
    
    This is a comprehensive scan that checks all tickers against all data sources.
    Use sparingly as it can be slow for large datasets.
    """
    result = await scan_all_tickers_for_gaps(db, limit)
    return result

@api_router.post("/admin/data-gaps/daily-report")
async def admin_daily_data_gaps_report():
    """Generate and store daily data gaps report."""
    result = await generate_daily_report(db)
    return result

# ----- Stock Overview (DB-only reads - ZERO OUTBOUND) -----

@api_router.get("/stock-overview/{ticker}")
async def get_stock_overview(ticker: str, lite: bool = Query(True)):
    """
    Get complete stock overview from cached data (no EODHD calls).
    
    Includes:
    - Company fundamentals (identity, metrics)
    - TTM calculations (Net Margin TTM, local P/E)
    - Valuation Score (0-100) with peer comparison
    - Industry benchmark data
    - Gradient colors for UI
    - Financials, earnings, insider data (if lite=False)
    
    Args:
        ticker: Stock ticker (e.g., AAPL)
        lite: If True (default), skip financials/earnings/insiders for faster load
    
    Returns:
        Complete stock overview with all P0 metrics.
    """
    ticker_upper = ticker.upper()
    ticker_full = ticker_upper if ticker_upper.endswith(".US") else f"{ticker_upper}.US"
    
    # LOVABLE LOGIC: Validate ticker visibility before showing data
    # Use the is_visible field which is set by the universe cleanup script
    tracked = await db.tracked_tickers.find_one(
        {"ticker": ticker_full},
        {"_id": 0}
    )
    
    if tracked:
        # Check visibility using the persisted is_visible field
        if not tracked.get("is_visible", False):
            raise HTTPException(404, f"Ticker {ticker} is not available")
    
    # 1. Company fundamentals - BINDING: Read from EMBEDDED tracked_tickers.fundamentals
    # NOT from company_fundamentals_cache (which is empty)
    embedded_fundamentals = tracked.get("fundamentals", {}) if tracked else {}
    general = embedded_fundamentals.get("General", {}) if embedded_fundamentals else {}
    
    # Build company dict from embedded fundamentals
    fundamentals_pending = False
    if general:
        company = {
            "ticker": ticker_full,
            "code": ticker_upper,
            "name": general.get("Name") or (tracked.get("name") if tracked else ticker_upper),
            "exchange": general.get("Exchange") or (tracked.get("exchange") if tracked else None),
            "sector": general.get("Sector") or (tracked.get("sector") if tracked else None),
            "industry": general.get("Industry") or (tracked.get("industry") if tracked else None),
            "asset_type": general.get("Type") or "Common Stock",
            "description": general.get("Description"),
            "website": general.get("WebURL"),
            "logo_url": general.get("LogoURL") or (tracked.get("logo_url") if tracked else None),
            "full_time_employees": general.get("FullTimeEmployees"),
            "ipo_date": general.get("IPODate") or (tracked.get("ipo_date") if tracked else None),
            "city": general.get("City"),
            "state": general.get("State"),
            "country_name": general.get("CountryName", "USA"),
            "isin": general.get("ISIN"),
            "cusip": general.get("CUSIP"),
            # Note: computed metrics (market_cap, pe_ratio, etc.) are NOT stored per whitelist rules
            "market_cap": None,  # Computed locally if needed
            "pe_ratio": None,
            "eps_ttm": None,
            "beta": None,
            "dividend_yield": None,
            "fifty_two_week_high": None,
            "fifty_two_week_low": None,
            "pct_insiders": None,
            "pct_institutions": None,
            "profit_margin": None,
            "roe": None,
            "revenue_ttm": None,
        }
    else:
        # No embedded fundamentals - check if ticker has price data
        has_prices = await db.stock_prices.count_documents({"ticker": ticker_full}) > 0
        
        if not tracked and not has_prices:
            raise HTTPException(404, f"No data for {ticker}")
        
        # Create placeholder company data from tracked_tickers
        fundamentals_pending = True
        company = {
            "ticker": ticker_full,
            "code": ticker_upper,
            "name": tracked.get("name") if tracked else ticker_upper,
            "exchange": tracked.get("exchange") if tracked else None,
            "sector": tracked.get("sector") if tracked else None,
            "industry": tracked.get("industry") if tracked else None,
            "asset_type": tracked.get("asset_type") if tracked else "Common Stock",
            "description": None,
            "website": None,
            "logo_url": None,
            "full_time_employees": None,
            "ipo_date": None,
            "city": None,
            "state": None,
            "country_name": "USA",
            "market_cap": None,
            "pe_ratio": None,
            "eps_ttm": None,
            "beta": None,
            "dividend_yield": None,
            "fifty_two_week_high": None,
            "fifty_two_week_low": None,
            "pct_insiders": None,
            "pct_institutions": None,
            "profit_margin": None,
            "roe": None,
            "revenue_ttm": None,
            "_fundamentals_pending": True,
        }
    
    # 2. Get latest price from stock_prices (DB-only, no EODHD fallback)
    latest_price = await db.stock_prices.find_one(
        {"ticker": ticker_full},
        {"_id": 0},
        sort=[("date", -1)]
    )
    
    current_price = None
    price_data = None
    
    if latest_price:
        prev_price = await db.stock_prices.find_one(
            {"ticker": ticker_full, "date": {"$lt": latest_price.get("date")}},
            {"_id": 0},
            sort=[("date", -1)]
        )
        
        current_price = latest_price.get("adjusted_close") or latest_price.get("close_price") or 0
        previous = (prev_price.get("adjusted_close") or prev_price.get("close_price") or current_price) if prev_price else current_price
        
        price_data = {
            "last_close": round(current_price, 2) if current_price else None,
            "previous_close": round(previous, 2) if previous else None,
            "change": round(current_price - previous, 2) if current_price and previous else None,
            "change_pct": round(((current_price - previous) / previous * 100), 2) if previous and previous != 0 else 0,
            "date": latest_price.get("date"),
        }
    
    # ZERO OUTBOUND RULE: No live EODHD calls - use only cached data
    # If no price in DB, log as data gap and use company fundamentals data if available
    if not current_price:
        # Try to use 52-week high as reference price (stored in fundamentals)
        company_price = company.get("fifty_two_week_high") or company.get("price_last_close")
        if company_price:
            current_price = company_price
            price_data = {
                "last_close": round(current_price, 2) if current_price else None,
                "previous_close": None,
                "change": None,
                "change_pct": None,
                "date": None,
                "source": "fundamentals_cache",
                "note": "Using cached price from fundamentals"
            }
        # Log data gap for missing price
        await log_data_gap(db, ticker_full, DataField.PRICE, "No price in stock_prices collection")
    
    # 3. Calculate TTM metrics
    ttm_metrics = await calculate_ttm_metrics(db, ticker_full)
    
    # Use TTM values for calculations
    eps_ttm = ttm_metrics.get("eps_ttm") or company.get("eps_ttm")
    net_margin_ttm = ttm_metrics.get("net_margin_ttm") or company.get("net_margin_ttm")
    
    # Calculate local P/E ratio
    local_pe = None
    if current_price and eps_ttm and eps_ttm > 0:
        local_pe = round(current_price / eps_ttm, 2)
    
    # 4. Get industry benchmark
    industry = company.get("industry")
    benchmark = None
    if industry:
        benchmark = await db.industry_benchmarks.find_one(
            {"industry": industry},
            {"_id": 0}
        )
    
    # 5. Calculate Dividend Yield TTM
    dividend_yield_ttm = None
    if current_price:
        dividend_yield_ttm = await calculate_dividend_yield_ttm(db, ticker_full, current_price)
    
    # 6. Compute Valuation Score if we have benchmark
    valuation_result = None
    if benchmark:
        company_metrics = {
            "pe_ratio": local_pe or company.get("pe_ratio"),
            "ps_ratio": company.get("ps_ratio"),
            "pb_ratio": company.get("pb_ratio"),
            "ev_ebitda": company.get("ev_ebitda"),
            "ev_revenue": company.get("ev_revenue"),
            "dividend_yield": dividend_yield_ttm or company.get("dividend_yield"),
            "net_margin_ttm": net_margin_ttm,
            "profit_margin": company.get("profit_margin"),
        }
        
        benchmark_metrics = {
            "pe_ratio_median": benchmark.get("pe_ratio_median"),
            "ps_ratio_median": benchmark.get("ps_ratio_median"),
            "pb_ratio_median": benchmark.get("pb_ratio_median"),
            "ev_ebitda_median": benchmark.get("ev_ebitda_median"),
            "ev_revenue_median": benchmark.get("ev_revenue_median"),
            "dividend_yield_median": benchmark.get("dividend_yield_median"),
            "net_margin_ttm_median": benchmark.get("net_margin_ttm_median"),
            "profit_margin_median": benchmark.get("profit_margin_median"),
        }
        
        valuation_result = compute_valuation_score(company_metrics, benchmark_metrics)
    
    # 7. Compute gradient colors for key metrics
    gradient_colors = {}
    if benchmark:
        metrics_for_gradient = [
            ("pe_ratio", local_pe or company.get("pe_ratio"), benchmark.get("pe_ratio_median"), "lower_better"),
            ("ps_ratio", company.get("ps_ratio"), benchmark.get("ps_ratio_median"), "lower_better"),
            ("pb_ratio", company.get("pb_ratio"), benchmark.get("pb_ratio_median"), "lower_better"),
            ("ev_ebitda", company.get("ev_ebitda"), benchmark.get("ev_ebitda_median"), "lower_better"),
            ("ev_revenue", company.get("ev_revenue"), benchmark.get("ev_revenue_median"), "lower_better"),
            ("dividend_yield", dividend_yield_ttm or company.get("dividend_yield"), benchmark.get("dividend_yield_median"), "higher_better"),
            ("net_margin_ttm", net_margin_ttm, benchmark.get("net_margin_ttm_median"), "higher_better"),
            ("profit_margin", company.get("profit_margin"), benchmark.get("profit_margin_median"), "higher_better"),
        ]
        
        for metric_name, company_val, benchmark_val, direction in metrics_for_gradient:
            if company_val is not None:
                gradient_colors[metric_name] = compute_gradient_color(company_val, benchmark_val, direction)
    
    # 8. Compute 52W high/low from stock_prices (on-demand)
    week_52_data = await compute_52w_high_low(db, ticker_full)
    
    # 9. Build key metrics with peer comparison
    key_metrics = {
        "market_cap": company.get("market_cap"),
        "enterprise_value": company.get("enterprise_value"),
        
        # P/E (local calculation preferred)
        "pe_ratio": local_pe or company.get("pe_ratio"),
        "pe_ratio_source": "local" if local_pe else "eodhd",
        "pe_benchmark": benchmark.get("pe_ratio_median") if benchmark else None,
        
        # EPS
        "eps_ttm": round(eps_ttm, 2) if eps_ttm else None,
        
        # Other valuation ratios
        "ps_ratio": company.get("ps_ratio"),
        "ps_benchmark": benchmark.get("ps_ratio_median") if benchmark else None,
        
        "pb_ratio": company.get("pb_ratio"),
        "pb_benchmark": benchmark.get("pb_ratio_median") if benchmark else None,
        
        "ev_ebitda": company.get("ev_ebitda"),
        "ev_ebitda_benchmark": benchmark.get("ev_ebitda_median") if benchmark else None,
        
        "ev_revenue": company.get("ev_revenue"),
        "ev_revenue_benchmark": benchmark.get("ev_revenue_median") if benchmark else None,
        
        # Profitability
        "net_margin_ttm": round(net_margin_ttm, 2) if net_margin_ttm else None,
        "net_margin_benchmark": benchmark.get("net_margin_ttm_median") if benchmark else None,
        
        "profit_margin": company.get("profit_margin"),
        "profit_margin_benchmark": benchmark.get("profit_margin_median") if benchmark else None,
        
        "roe": company.get("roe"),
        "roa": company.get("roa"),
        
        # Dividends
        "dividend_yield": company.get("dividend_yield"),
        "dividend_yield_ttm": round(dividend_yield_ttm, 2) if dividend_yield_ttm else None,
        "dividend_benchmark": benchmark.get("dividend_yield_median") if benchmark else None,
        "payout_ratio": company.get("payout_ratio"),
        "ex_dividend_date": company.get("ex_dividend_date"),
        
        # Risk - 52W computed on-demand from stock_prices
        "beta": company.get("beta"),
        "fifty_two_week_high": week_52_data.get("fifty_two_week_high"),
        "fifty_two_week_low": week_52_data.get("fifty_two_week_low"),
        "fifty_two_week_high_date": week_52_data.get("high_date"),
        "fifty_two_week_low_date": week_52_data.get("low_date"),
        "fifty_two_week_source": week_52_data.get("source"),
        "fifty_two_week_days_of_data": week_52_data.get("days_of_data"),
        
        # Ownership
        "pct_insiders": company.get("pct_insiders"),
        "pct_institutions": company.get("pct_institutions"),
    }
    
    # 10. Build peer comparison context
    peer_context = None
    if benchmark:
        peer_context = {
            "industry": benchmark.get("industry"),
            "sector": benchmark.get("sector"),
            "company_count": benchmark.get("company_count"),
            "has_sufficient_peers": (benchmark.get("company_count") or 0) >= 5,
        }
    
    # 10. Financials (if full mode)
    financials = None
    if not lite:
        # Get Income_Statement data for financials display
        annual_raw = await db.financials_cache.find(
            {"ticker": ticker_full, "period_type": "annual", "statement_type": "Income_Statement"},
            {"_id": 0}
        ).sort("date", -1).limit(5).to_list(5)
        
        quarterly_raw = await db.financials_cache.find(
            {"ticker": ticker_full, "period_type": "quarterly", "statement_type": "Income_Statement"},
            {"_id": 0}
        ).sort("date", -1).limit(8).to_list(8)
        
        # Transform to frontend expected format
        def transform_financial(f):
            data = f.get("data", {})
            return {
                "period_date": f.get("date"),
                "statement_type": f.get("statement_type"),
                "revenue": float(data.get("totalRevenue") or 0) if data.get("totalRevenue") else None,
                "net_income": float(data.get("netIncome") or 0) if data.get("netIncome") else None,
                "gross_profit": float(data.get("grossProfit") or 0) if data.get("grossProfit") else None,
                "operating_income": float(data.get("operatingIncome") or 0) if data.get("operatingIncome") else None,
                "ebitda": float(data.get("ebitda") or 0) if data.get("ebitda") else None,
            }
        
        annual = [transform_financial(f) for f in annual_raw if f.get("data")]
        quarterly = [transform_financial(f) for f in quarterly_raw if f.get("data")]
        
        # Filter out records with no meaningful data
        annual = [a for a in annual if a.get("revenue") or a.get("net_income")]
        quarterly = [q for q in quarterly if q.get("revenue") or q.get("net_income")]
        
        if annual or quarterly:
            financials = {
                "annual": annual,
                "quarterly": quarterly,
                "ttm": {
                    "revenue": ttm_metrics.get("revenue_ttm"),
                    "net_income": ttm_metrics.get("net_income_ttm"),
                    "ebitda": ttm_metrics.get("ebitda_ttm"),
                    "operating_income": ttm_metrics.get("operating_income_ttm"),
                    "gross_profit": ttm_metrics.get("gross_profit_ttm"),
                    "free_cash_flow": ttm_metrics.get("free_cash_flow_ttm"),
                    "quarters_used": ttm_metrics.get("quarters_used", []),
                }
            }
    
    # 11. Earnings history (if full mode)
    earnings = None
    if not lite:
        earnings = await db.earnings_history_cache.find(
            {"ticker": ticker_full},
            {"_id": 0}
        ).sort("quarter_date", -1).limit(12).to_list(12)
    
    # 12. Insider activity (if full mode)
    insider = None
    if not lite:
        insider = await db.insider_activity_cache.find_one(
            {"ticker": ticker_full},
            {"_id": 0}
        )
    
    # 13. Dividend history (if full mode)
    dividends = None
    if not lite:
        dividends = await get_dividend_history_for_ticker(db, ticker_full)
    
    return {
        "ticker": ticker_full,
        
        # Company identity
        "company": {
            "name": company.get("name"),
            "code": company.get("code"),
            "exchange": company.get("exchange"),
            "sector": company.get("sector"),
            "industry": company.get("industry"),
            "description": company.get("description"),
            "website": company.get("website"),
            "logo_url": company.get("logo_url"),
            "full_time_employees": company.get("full_time_employees"),
            "ipo_date": company.get("ipo_date"),
            "city": company.get("city"),
            "state": company.get("state"),
            "country_name": company.get("country_name"),
        },
        
        # Price data
        "price": price_data,
        
        # Key metrics with benchmark comparison
        "key_metrics": key_metrics,
        
        # Valuation Score (0-100)
        "valuation": valuation_result,
        
        # Gradient colors for UI
        "gradient_colors": gradient_colors,
        
        # Peer comparison context
        "peer_context": peer_context,
        
        # Detailed data (null if lite mode)
        "financials": financials,
        "earnings": earnings,
        "insider_activity": insider,
        "dividends": dividends,
        
        # Metadata
        "lite_mode": lite,
        "data_source": "cache",
        "has_benchmark": benchmark is not None,
        "fundamentals_pending": fundamentals_pending,  # VARIANT C: True if no fundamentals yet
    }


# =============================================================================
# TICKER DETAIL MOBILE API (NEW DESIGN)
# =============================================================================
# RAW FACTS ONLY. All metrics computed locally from raw financial statements.
# =============================================================================

def _is_local_valuation_cache_doc(doc: Optional[Dict[str, Any]]) -> bool:
    """
    M01 guard: Runtime valuation may use cache only when it was
    materialized from local valuation time-series (raw facts pipeline).
    """
    if not isinstance(doc, dict):
        return False
    source = doc.get("source")
    timeseries_source = doc.get("timeseries_source") or {}
    return (
        source == "materialized_from_timeseries"
        and timeseries_source.get("collection") == "ticker_valuation_timeseries"
    )

@api_router.get("/v1/ticker/{ticker}/detail")
async def get_ticker_detail_mobile(
    ticker: str,
    period: str = Query("1Y", description="Period for chart stats: 3M, 6M, YTD, 1Y, 3Y, 5Y")
):
    """
    Get ticker detail for mobile app redesign.
    
    Returns:
    - company: Basic company info (name, sector, industry, logo, etc.)
    - price: Current price + daily change
    - reality_check: ALL-TIME metrics (fixed, never changes with period)
    - period_stats: Metrics for selected chart period
    - valuation: Peer + Self comparison (locally computed)
    - key_metrics: Hybrid 7 metrics (Market Cap, Shares, Net Margin, FCF Yield, etc.)
    - peer_transparency: Total peers + valid metric counts
    - company_details: Collapsible section data
    
    RAW FACTS ONLY - all metrics computed locally from raw financial statements.
    NO 52W High/Low (removed per P0 spec).
    """
    from local_metrics_service import (
        calculate_reality_check_max,
        calculate_period_stats,
        get_valuation_overview_v2,  # BINDING: Use V2 that reads from embedded fundamentals
        calculate_hybrid_7_metrics_v2,  # BINDING: Use V2 for Key Metrics
        get_peer_transparency,
    )
    
    ticker_upper = ticker.upper()
    ticker_full = ticker_upper if ticker_upper.endswith(".US") else f"{ticker_upper}.US"
    symbol = ticker_upper.replace(".US", "")
    
    # Validate ticker visibility
    tracked = await db.tracked_tickers.find_one(
        {"ticker": ticker_full, "is_visible": True},
        {"_id": 0}
    )
    
    if not tracked:
        raise HTTPException(404, f"Ticker {ticker} is not available")
    
    # Get company fundamentals from EMBEDDED tracked_tickers.fundamentals
    # BINDING: Fundamentals are embedded, NOT in separate cache collection
    embedded_fundamentals = tracked.get("fundamentals", {})
    general = embedded_fundamentals.get("General", {}) if embedded_fundamentals else {}
    
    # Build company dict from embedded fundamentals - ALWAYS return a dict, never None
    company = {
        "name": general.get("Name") or tracked.get("name"),
        "sector": general.get("Sector") or tracked.get("sector"),
        "industry": general.get("Industry") or tracked.get("industry"),
        "exchange": general.get("Exchange") or tracked.get("exchange"),
        "description": general.get("Description"),
        "website": general.get("WebURL"),
        "logo_url": general.get("LogoURL") or tracked.get("logo_url"),
        "employees": general.get("FullTimeEmployees"),
        "ipo_date": general.get("IPODate") or tracked.get("ipo_date"),
        "address": general.get("Address"),
        "phone": general.get("Phone"),
        "country": general.get("CountryName"),
        "isin": general.get("ISIN"),
        "cusip": general.get("CUSIP"),
    }
    
    # Phase 1: Parallel lightweight cache/index lookups
    (
        latest_price,
        prev_price,
        pain_cache,
        valuation_cache_raw,
    ) = await asyncio.gather(
        db.stock_prices.find_one(
            {"ticker": ticker_full}, {"_id": 0}, sort=[("date", -1)]
        ),
        db.stock_prices.find_one(
            {"ticker": ticker_full}, {"_id": 0}, sort=[("date", -1)], skip=1
        ),
        db.ticker_pain_cache.find_one(
            {"ticker": ticker_full},
            {"_id": 0, "ticker": 0, "cached_at": 0, "data_points_used": 0}
        ),
        db.ticker_valuations_cache.find_one(
            {"ticker": ticker_full}, {"_id": 0}
        ),
    )
    
    valuation_cache = valuation_cache_raw if _is_local_valuation_cache_doc(valuation_cache_raw) else None
    
    # Phase 2: Heavy full-history queries run sequentially to avoid
    # resource contention (both scan entire stock_prices for MAX period)
    reality_check = await calculate_reality_check_max(db, ticker_full)
    period_stats = await calculate_period_stats(db, ticker_full, period)
    
    current_price = latest_price["close"] if latest_price else None
    prev_close = prev_price["close"] if prev_price else current_price
    
    # Calculate daily change
    daily_change = None
    daily_change_pct = None
    if current_price and prev_close:
        daily_change = current_price - prev_close
        daily_change_pct = (daily_change / prev_close) * 100 if prev_close > 0 else 0
    
    # Also get peer benchmarks for the ticker's industry
    peer_bench_doc = None
    if tracked.get("industry"):
        peer_bench_doc = await db.peer_benchmarks.find_one(
            {"industry": tracked.get("industry")},
            {"_id": 0}
        )
    if not peer_bench_doc and tracked.get("sector"):
        peer_bench_doc = await db.peer_benchmarks.find_one(
            {"sector": tracked.get("sector"), "industry": None},
            {"_id": 0}
        )
    
    # Build valuation response from cache
    if valuation_cache:
        cached_metrics = valuation_cache.get("current_metrics", {})
        
        # BINDING: Use peer_count_used (USD-only) for vs_peers comparison
        peer_count_used = peer_bench_doc.get("peer_count_used", peer_bench_doc.get("peer_count", 0)) if peer_bench_doc else 0
        peer_count_total = peer_bench_doc.get("peer_count_total", peer_bench_doc.get("peer_count", 0)) if peer_bench_doc else 0
        currency_filter = peer_bench_doc.get("currency_filter") if peer_bench_doc else None
        
        # =====================================================================
        # P0 FIX: EXCLUDE-SELF SIMPLE MEDIAN (not weighted)
        # For each metric, exclude the current ticker from peer list,
        # then compute simple median from remaining peers
        # =====================================================================
        def compute_exclude_self_median(metric_values: dict, exclude_ticker: str) -> tuple:
            """Compute simple median excluding self ticker."""
            if not metric_values:
                return None, 0, None
            
            tickers = metric_values.get("tickers", [])
            values = metric_values.get("values", [])
            
            if not tickers or not values or len(tickers) != len(values):
                return None, 0, None
            
            # Exclude self
            filtered = [(t, v) for t, v in zip(tickers, values) if t != exclude_ticker]
            
            if len(filtered) < 3:
                return None, len(filtered), "insufficient_peers"
            
            # Values are pre-sorted, compute simple median
            filtered_values = [v for _, v in filtered]
            n = len(filtered_values)
            if n % 2 == 1:
                median_val = filtered_values[n // 2]
            else:
                median_val = (filtered_values[n // 2 - 1] + filtered_values[n // 2]) / 2
            
            return round(median_val, 2), n, "industry"
        
        # Get sector fallback doc
        sector_bench_doc = None
        if tracked.get("sector"):
            sector_bench_doc = await db.peer_benchmarks.find_one(
                {"sector": tracked.get("sector"), "industry": None},
                {"_id": 0, "metric_values": 1, "peer_count": 1}
            )
        
        # Compute exclude-self medians for each metric
        metric_medians = {}
        for metric in ["pe", "ps", "pb", "ev_ebitda", "ev_revenue"]:
            median_val, peer_count, source = None, 0, None
            
            # Try industry first
            if peer_bench_doc:
                industry_metrics = peer_bench_doc.get("metric_values", {}).get(metric, {})
                median_val, peer_count, source = compute_exclude_self_median(industry_metrics, ticker_full)
            
            # Fallback to sector if insufficient industry peers
            if median_val is None and sector_bench_doc:
                sector_metrics = sector_bench_doc.get("metric_values", {}).get(metric, {})
                median_val, peer_count, source = compute_exclude_self_median(sector_metrics, ticker_full)
                if source:
                    source = "sector"
            
            metric_medians[metric] = {"median": median_val, "count": peer_count, "source": source}
        
        # Reclassify vs_peers using new medians
        def classify_vs(current, median):
            if current is None or median is None:
                return None
            ratio = current / median if median > 0 else 1
            if ratio < 0.85:
                return "cheaper"
            elif ratio > 1.15:
                return "more_expensive"
            return "around"
        
        # P0 FIX: Recompute overall_vs_peers from metric-level vs_peers (60% majority rule)
        def compute_overall_from_metrics(metrics_dict):
            """Recompute header status from metric-level vs_peers values."""
            classifications = []
            for metric_key in ["pe", "ps", "pb", "ev_ebitda", "ev_revenue"]:
                metric_data = metrics_dict.get(metric_key, {})
                current = cached_metrics.get(metric_key)
                median = metric_data.get("median")
                # Only count metrics where both current and peer_median exist
                if current is not None and median is not None:
                    vs = classify_vs(current, median)
                    if vs:
                        classifications.append(vs)
            
            if not classifications:
                return None
            
            total = len(classifications)
            cheaper_count = sum(1 for c in classifications if c == "cheaper")
            expensive_count = sum(1 for c in classifications if c == "more_expensive")
            
            # 60% majority rule
            if expensive_count / total >= 0.6:
                return "more_expensive"
            elif cheaper_count / total >= 0.6:
                return "cheaper"
            else:
                return "around"
        
        recomputed_overall = compute_overall_from_metrics(metric_medians)
        
        # =====================================================================
        # P0 UX TRANSPARENCY: Evidence-based N/A reason codes
        # =====================================================================
        # Reason codes:
        # - missing_raw_data: field absent or not enough quarters
        # - non_positive_value: value present but <= 0 (e.g., EPS <= 0)
        # - insufficient_peers: peer_count < 3 after exclusion
        # =====================================================================
        
        NA_REASONS_DISPLAY = {
            "missing_raw_data": "Missing financial data",
            "non_positive_value_earnings": "Negative earnings (loss-making)",
            "non_positive_value_ebitda": "Negative EBITDA",
            "non_positive_value_revenue": "No revenue data",
            "non_positive_value_book": "Negative book value",
            "insufficient_peers": "Insufficient peer data (<3)",
        }
        
        def get_na_reason(metric_key, current_val, peer_count, valuation_cache_metrics):
            """
            Determine N/A reason based on evidence.
            Returns (na_reason_code, na_reason_display) or (None, None) if valid.
            """
            if current_val is not None:
                return None, None
            
            # Check if metric is in cache - if not, it's missing raw data OR non-positive
            if metric_key not in valuation_cache_metrics:
                # For PE/EV_EBITDA, check if it's due to negative values
                if metric_key == "pe":
                    return "non_positive_value_earnings", NA_REASONS_DISPLAY["non_positive_value_earnings"]
                elif metric_key == "ev_ebitda":
                    return "non_positive_value_ebitda", NA_REASONS_DISPLAY["non_positive_value_ebitda"]
                elif metric_key in ["ps", "ev_revenue"]:
                    return "non_positive_value_revenue", NA_REASONS_DISPLAY["non_positive_value_revenue"]
                elif metric_key == "pb":
                    return "non_positive_value_book", NA_REASONS_DISPLAY["non_positive_value_book"]
                else:
                    return "missing_raw_data", NA_REASONS_DISPLAY["missing_raw_data"]
            
            # Check peer count
            if peer_count is not None and peer_count < 3:
                return "insufficient_peers", NA_REASONS_DISPLAY["insufficient_peers"]
            
            return "missing_raw_data", NA_REASONS_DISPLAY["missing_raw_data"]
        
        # Build excluded metrics list and summary
        excluded_metrics = []
        excluded_reasons_map = {}
        metrics_used_count = 0
        
        for metric_key in ["pe", "ps", "pb", "ev_ebitda", "ev_revenue"]:
            current = cached_metrics.get(metric_key)
            peer_count = metric_medians[metric_key]["count"]
            
            if current is not None:
                metrics_used_count += 1
            else:
                metric_display = metric_key.upper().replace("_", "/")
                na_code, na_display = get_na_reason(metric_key, current, peer_count, cached_metrics)
                excluded_metrics.append(metric_display)
                if na_code:
                    if na_code not in excluded_reasons_map:
                        excluded_reasons_map[na_code] = {"display": na_display, "metrics": []}
                    excluded_reasons_map[na_code]["metrics"].append(metric_display)
        
        # Generate human-readable excluded summary
        excluded_summary = None
        if excluded_metrics:
            parts = []
            for na_code, info in excluded_reasons_map.items():
                metrics_str = " & ".join(info["metrics"])
                parts.append(f"{metrics_str}: {info['display']}")
            excluded_summary = " • ".join(parts)
        
        valuation = {
            "available": True,
            "source": "local_raw_facts_timeseries",
            "overall_vs_peers": recomputed_overall,  # P0 FIX: Use recomputed value
            "peer_count": peer_count_used,  # BINDING: Use peer_count_used
            "peer_count_total": peer_count_total,
            "peer_count_used": peer_count_used,
            "currency_filter": currency_filter,
            "peer_type": "industry" if peer_bench_doc and peer_bench_doc.get("industry") else "sector",
            "overall_vs_5y_avg": valuation_cache.get("eval_vs_5y"),
            "history_5y": {"available": valuation_cache.get("eval_vs_5y") is not None},
            "metrics_used": metrics_used_count,
            "excluded_metrics": excluded_metrics,
            "excluded_summary": excluded_summary,
            "metrics": {
                "pe": {
                    "name": "P/E",
                    "current": cached_metrics.get("pe"),
                    "peer_median": metric_medians["pe"]["median"],
                    "peer_count": metric_medians["pe"]["count"],
                    "peer_source": metric_medians["pe"]["source"],
                    "vs_peers": classify_vs(cached_metrics.get("pe"), metric_medians["pe"]["median"]),
                    "na_reason_code": get_na_reason("pe", cached_metrics.get("pe"), metric_medians["pe"]["count"], cached_metrics)[0],
                    "na_reason_display": get_na_reason("pe", cached_metrics.get("pe"), metric_medians["pe"]["count"], cached_metrics)[1],
                },
                "ps": {
                    "name": "P/S",
                    "current": cached_metrics.get("ps"),
                    "peer_median": metric_medians["ps"]["median"],
                    "peer_count": metric_medians["ps"]["count"],
                    "peer_source": metric_medians["ps"]["source"],
                    "vs_peers": classify_vs(cached_metrics.get("ps"), metric_medians["ps"]["median"]),
                    "na_reason_code": get_na_reason("ps", cached_metrics.get("ps"), metric_medians["ps"]["count"], cached_metrics)[0],
                    "na_reason_display": get_na_reason("ps", cached_metrics.get("ps"), metric_medians["ps"]["count"], cached_metrics)[1],
                },
                "pb": {
                    "name": "P/B",
                    "current": cached_metrics.get("pb"),
                    "peer_median": metric_medians["pb"]["median"],
                    "peer_count": metric_medians["pb"]["count"],
                    "peer_source": metric_medians["pb"]["source"],
                    "vs_peers": classify_vs(cached_metrics.get("pb"), metric_medians["pb"]["median"]),
                    "na_reason_code": get_na_reason("pb", cached_metrics.get("pb"), metric_medians["pb"]["count"], cached_metrics)[0],
                    "na_reason_display": get_na_reason("pb", cached_metrics.get("pb"), metric_medians["pb"]["count"], cached_metrics)[1],
                },
                "ev_ebitda": {
                    "name": "EV/EBITDA",
                    "current": cached_metrics.get("ev_ebitda"),
                    "peer_median": metric_medians["ev_ebitda"]["median"],
                    "peer_count": metric_medians["ev_ebitda"]["count"],
                    "peer_source": metric_medians["ev_ebitda"]["source"],
                    "vs_peers": classify_vs(cached_metrics.get("ev_ebitda"), metric_medians["ev_ebitda"]["median"]),
                    "na_reason_code": get_na_reason("ev_ebitda", cached_metrics.get("ev_ebitda"), metric_medians["ev_ebitda"]["count"], cached_metrics)[0],
                    "na_reason_display": get_na_reason("ev_ebitda", cached_metrics.get("ev_ebitda"), metric_medians["ev_ebitda"]["count"], cached_metrics)[1],
                },
                "ev_revenue": {
                    "name": "EV/Revenue",
                    "current": cached_metrics.get("ev_revenue"),
                    "peer_median": metric_medians["ev_revenue"]["median"],
                    "peer_count": metric_medians["ev_revenue"]["count"],
                    "peer_source": metric_medians["ev_revenue"]["source"],
                    "vs_peers": classify_vs(cached_metrics.get("ev_revenue"), metric_medians["ev_revenue"]["median"]),
                    "na_reason_code": get_na_reason("ev_revenue", cached_metrics.get("ev_revenue"), metric_medians["ev_revenue"]["count"], cached_metrics)[0],
                    "na_reason_display": get_na_reason("ev_revenue", cached_metrics.get("ev_revenue"), metric_medians["ev_revenue"]["count"], cached_metrics)[1],
                },
            },
            "disclaimer": "Context only, not advice.",
            "cache_timestamp": valuation_cache.get("computed_at")
        }
    else:
        valuation = {
            "available": False,
            "reason": "Local valuation cache not pre-computed yet",
            "local_only_enforced": True,
        }
    
    # ==========================================================================
    # P0 BLOCKER FIX: Hybrid 7 Key Metrics - Full implementation with all fields
    # ==========================================================================
    
    # Get shares from embedded fundamentals
    shares_stats = embedded_fundamentals.get("SharesStats", {})
    shares_outstanding = shares_stats.get("SharesOutstanding")
    
    # Get current price for market cap calculation
    current_price_val = None
    if latest_price:
        current_price_val = latest_price.get("close") or latest_price.get("adjusted_close")
    
    # Calculate market cap
    market_cap = None
    if shares_outstanding and current_price_val and current_price_val > 0:
        market_cap = shares_outstanding * current_price_val
    
    # Format helpers for large numbers
    def format_large_num(val):
        if val is None: return None
        if val >= 1e12: return f"${val/1e12:.2f}T"
        if val >= 1e9: return f"${val/1e9:.2f}B"
        if val >= 1e6: return f"${val/1e6:.2f}M"
        return f"${val:,.0f}"
    
    def format_shares(val):
        if val is None: return None
        if val >= 1e9: return f"{val/1e9:.2f}B"
        if val >= 1e6: return f"{val/1e6:.1f}M"
        return f"{val:,.0f}"
    
    # Extract embedded Financials for TTM calculations
    embedded_financials_data = embedded_fundamentals.get("Financials", {})
    
    # Get TTM data from Income Statement (need to calculate from quarterly)
    income_stmt = embedded_financials_data.get("Income_Statement", {})
    quarterly_income = income_stmt.get("quarterly", {})
    
    # Calculate TTM Revenue and Net Income from last 4 quarters
    ttm_revenue = None
    ttm_net_income = None
    ttm_ebitda = None  # P0 FIX: Calculate EBITDA TTM from quarterly data
    if quarterly_income:
        # Sort quarters by date descending
        sorted_quarters = sorted(quarterly_income.items(), key=lambda x: x[0], reverse=True)[:4]
        if len(sorted_quarters) >= 4:
            revenues = [q[1].get("totalRevenue") for q in sorted_quarters if q[1].get("totalRevenue")]
            net_incomes = [q[1].get("netIncome") for q in sorted_quarters if q[1].get("netIncome") is not None]
            ebitdas = [q[1].get("ebitda") for q in sorted_quarters if q[1].get("ebitda") is not None]
            if len(revenues) >= 4:
                ttm_revenue = sum(float(r) for r in revenues[:4])
            if len(net_incomes) >= 4:
                ttm_net_income = sum(float(ni) for ni in net_incomes[:4])
            if len(ebitdas) >= 4:
                ttm_ebitda = sum(float(e) for e in ebitdas[:4])
    
    # Get TTM FCF from Cash Flow Statement
    ttm_fcf = None
    cash_flow = embedded_financials_data.get("Cash_Flow", {})
    quarterly_cf = cash_flow.get("quarterly", {})
    if quarterly_cf:
        sorted_cf = sorted(quarterly_cf.items(), key=lambda x: x[0], reverse=True)[:4]
        if len(sorted_cf) >= 4:
            fcfs = []
            for q in sorted_cf[:4]:
                ocf = float(q[1].get("totalCashFromOperatingActivities") or q[1].get("operatingCashflow") or 0)
                capex = abs(float(q[1].get("capitalExpenditures") or 0))
                fcfs.append(ocf - capex)
            if fcfs:
                ttm_fcf = sum(fcfs)
    
    # Get Cash and Debt from latest Balance Sheet
    ttm_cash = None
    ttm_debt = None
    balance_sheet = embedded_financials_data.get("Balance_Sheet", {})
    quarterly_bs = balance_sheet.get("quarterly", {})
    if quarterly_bs:
        sorted_bs = sorted(quarterly_bs.items(), key=lambda x: x[0], reverse=True)
        if sorted_bs:
            latest_bs = sorted_bs[0][1]
            ttm_cash = float(latest_bs.get("cash") or latest_bs.get("cashAndCashEquivalentsAtCarryingValue") or 0)
            short_debt = float(latest_bs.get("shortLongTermDebt") or latest_bs.get("shortTermDebt") or 0)
            long_debt = float(latest_bs.get("longTermDebt") or 0)
            ttm_debt = short_debt + long_debt
    
    # Calculate Net Margin TTM
    net_margin_ttm = None
    if ttm_revenue and ttm_revenue > 0 and ttm_net_income is not None:
        net_margin_ttm = (ttm_net_income / ttm_revenue) * 100
    
    # Calculate FCF Yield
    fcf_yield = None
    if ttm_fcf is not None and market_cap and market_cap > 0:
        fcf_yield = (ttm_fcf / market_cap) * 100
    
    # Calculate Net Debt/EBITDA
    # P0 FIX: Use ttm_ebitda calculated from quarterly data, fallback to valuation_cache
    net_debt_ebitda = None
    ebitda_ttm = ttm_ebitda  # Use locally calculated EBITDA TTM first
    if ebitda_ttm is None:
        # Fallback to valuation_cache if available
        ebitda_ttm = valuation_cache.get("raw_inputs", {}).get("ebitda_ttm") if valuation_cache else None
    if ttm_debt is not None and ttm_cash is not None and ebitda_ttm and ebitda_ttm > 0:
        net_debt = ttm_debt - ttm_cash
        net_debt_ebitda = net_debt / ebitda_ttm
    
    # Get Dividend Yield from fundamentals
    splits_dividends = embedded_fundamentals.get("SplitsDividends", {})
    dividend_yield_ttm = splits_dividends.get("ForwardAnnualDividendYield")
    if dividend_yield_ttm:
        try:
            dividend_yield_ttm = float(dividend_yield_ttm) * 100  # Convert to percentage
        except (ValueError, TypeError):
            dividend_yield_ttm = None
    
    # FIX-2: Read PRECOMPUTED dividend data from peer_benchmarks
    # All fallback logic is already computed by compute_peer_benchmarks_v3
    # API only reads the pre-computed values + counts + levels
    industry_dividend_data = {
        "dividend_yield_median_all": None,
        "dividend_yield_median_payers": None,
        "dividend_peer_count": 0,
        "dividend_payers_count": 0,
        "dividend_median_level_all": None,
        "dividend_median_level_payers": None
    }
    industry = general.get("Industry") if general else None
    
    if industry:
        peer_bench = await db.peer_benchmarks.find_one({"industry": industry})
        if peer_bench:
            benchmarks = peer_bench.get("benchmarks", {})
            industry_dividend_data = {
                "dividend_yield_median_all": benchmarks.get("dividend_yield_median_all"),
                "dividend_yield_median_payers": benchmarks.get("dividend_yield_median_payers"),
                "dividend_peer_count": peer_bench.get("dividend_peer_count", 0),
                "dividend_payers_count": peer_bench.get("dividend_payers_count", 0),
                "dividend_median_level_all": peer_bench.get("dividend_median_level_all"),
                "dividend_median_level_payers": peer_bench.get("dividend_median_level_payers")
            }
    
    # Build key_metrics with all 7 fields
    key_metrics = {
        "market_cap": {
            "name": "Market Cap",
            "value": market_cap,
            "formatted": format_large_num(market_cap),
            "na_reason": None if market_cap else "missing_data"
        },
        "shares_outstanding": {
            "name": "Shares Outstanding",
            "value": shares_outstanding,
            "formatted": format_shares(shares_outstanding),
            "na_reason": None if shares_outstanding else "missing_data"
        },
        "net_margin_ttm": {
            "name": "Net Margin (TTM)",
            "value": net_margin_ttm,
            "formatted": f"{net_margin_ttm:.1f}%" if net_margin_ttm is not None else None,
            "na_reason": None if net_margin_ttm is not None else ("unprofitable" if ttm_net_income and ttm_net_income < 0 else "missing_data")
        },
        "fcf_yield": {
            "name": "FCF Yield",
            "value": fcf_yield,
            "formatted": f"{fcf_yield:.1f}%" if fcf_yield is not None else None,
            "na_reason": None if fcf_yield is not None else ("negative_fcf" if ttm_fcf and ttm_fcf < 0 else "missing_data")
        },
        "net_debt_ebitda": {
            "name": "Net Debt/EBITDA",
            "value": net_debt_ebitda,
            "formatted": f"{net_debt_ebitda:.1f}x" if net_debt_ebitda is not None else None,
            # P0 FIX: Precise NA reasons for Net Debt/EBITDA
            "na_reason": None if net_debt_ebitda is not None else (
                "negative_ebitda" if ebitda_ttm is not None and ebitda_ttm <= 0 else
                "ebitda_missing" if ebitda_ttm is None else
                "missing_debt_data" if ttm_debt is None else
                "missing_cash_data" if ttm_cash is None else
                "missing_data"
            )
        },
        "revenue_growth_3y": {
            "name": "Revenue Growth (3Y CAGR)",
            "value": None,  # Will be updated after annual_income is loaded
            "formatted": None,
            "na_reason": "insufficient_annual_history"
        },
        "dividend_yield_ttm": {
            "name": "Dividend Yield",
            "value": dividend_yield_ttm,
            "formatted": f"{dividend_yield_ttm:.2f}%" if dividend_yield_ttm else "0.00%",
            "na_reason": None if dividend_yield_ttm is not None else "no_dividend",
            # BACKWARD COMPAT (keep existing fields for frontend):
            "industry_dividend_yield_median": industry_dividend_data.get("dividend_yield_median_all"),
            "industry_dividend_peer_count": industry_dividend_data.get("dividend_peer_count", 0),
            "dividend_median_level": industry_dividend_data.get("dividend_median_level_all"),
            # FIX-2: NEW FIELDS (dual medians with independent fallback levels)
            "dividend_yield_median_all": industry_dividend_data.get("dividend_yield_median_all"),
            "dividend_yield_median_payers": industry_dividend_data.get("dividend_yield_median_payers"),
            "dividend_peer_count": industry_dividend_data.get("dividend_peer_count", 0),
            "dividend_payers_count": industry_dividend_data.get("dividend_payers_count", 0),
            "dividend_median_level_all": industry_dividend_data.get("dividend_median_level_all"),
            "dividend_median_level_payers": industry_dividend_data.get("dividend_median_level_payers")
        },
    }
    
    # Peer Transparency (P0 requirement: total_industry_peers + valid_metric_peers)
    peer_transparency = await get_peer_transparency(db, ticker_full)
    
    # P8 FIX: Add Financials data (5 essential metrics: Revenue, Net Income, FCF, Cash, Debt)
    # P9: Extended with YoY comparisons and Core 5 for all periods
    # BINDING: Read from EMBEDDED tracked_tickers.fundamentals.Financials, not financials_cache
    financials = None
    
    # Extract embedded financials
    embedded_financials = embedded_fundamentals.get("Financials", {})
    
    # Helper to convert embedded format to list of {date, data} dicts
    def convert_embedded_to_list(statements_dict, limit=12):
        """Convert embedded quarterly/yearly dict to sorted list by date desc."""
        if not statements_dict or not isinstance(statements_dict, dict):
            return []
        result = []
        for date_key, data in statements_dict.items():
            if isinstance(data, dict):
                result.append({"date": date_key, "data": data})
        # Sort by date descending
        result.sort(key=lambda x: x.get("date", ""), reverse=True)
        return result[:limit]
    
    # Get Income Statement data from embedded
    income_stmt = embedded_financials.get("Income_Statement", {})
    annual_income = convert_embedded_to_list(income_stmt.get("yearly", {}), 6)
    quarterly_income = convert_embedded_to_list(income_stmt.get("quarterly", {}), 12)
    
    # Get Balance Sheet data from embedded
    balance_sheet = embedded_financials.get("Balance_Sheet", {})
    quarterly_balance = convert_embedded_to_list(balance_sheet.get("quarterly", {}), 12)
    annual_balance = convert_embedded_to_list(balance_sheet.get("yearly", {}), 6)
    
    # Get Cash Flow data from embedded
    cash_flow = embedded_financials.get("Cash_Flow", {})
    quarterly_cashflow = convert_embedded_to_list(cash_flow.get("quarterly", {}), 12)
    annual_cashflow = convert_embedded_to_list(cash_flow.get("yearly", {}), 6)
    
    # FIX 1: Calculate Revenue Growth 3Y CAGR from annual revenue history
    revenue_growth_3y_value = None
    revenue_growth_3y_na_reason = "insufficient_annual_history"
    
    # Get annual revenues from annual_income (already sorted by date desc)
    annual_revenues = []
    for inc in annual_income[:4]:
        inc_data = inc.get("data", {})
        rev = inc_data.get("totalRevenue")
        if rev is not None:
            try:
                annual_revenues.append(float(rev))
            except (ValueError, TypeError):
                pass
    
    # Need exactly 4 annual revenue points for 3-year CAGR
    if len(annual_revenues) >= 4:
        end_revenue = annual_revenues[0]      # Most recent year
        start_revenue = annual_revenues[3]    # 3 years ago
        
        if start_revenue is None or start_revenue <= 0:
            revenue_growth_3y_na_reason = "negative_or_zero_base_revenue"
        elif end_revenue is None or end_revenue <= 0:
            revenue_growth_3y_na_reason = "negative_end_revenue"
        else:
            # CAGR = (End/Start)^(1/3) - 1
            cagr = ((end_revenue / start_revenue) ** (1/3) - 1) * 100
            revenue_growth_3y_value = round(cagr, 1)
            revenue_growth_3y_na_reason = None
    
    # Update key_metrics with calculated revenue_growth_3y
    key_metrics["revenue_growth_3y"] = {
        "name": "Revenue Growth (3Y CAGR)",
        "value": revenue_growth_3y_value,
        "formatted": f"{revenue_growth_3y_value:.1f}%" if revenue_growth_3y_value is not None else None,
        "na_reason": revenue_growth_3y_na_reason
    }
    
    # P9: Helper to extract Core 5 metrics from financial statements
    def get_core5_from_period(income_data, balance_data, cashflow_data, period_date):
        """Extract Core 5 metrics for a single period"""
        revenue = None
        net_income = None
        fcf = None
        cash = None
        total_debt = None
        
        # Income statement metrics
        if income_data:
            revenue = float(income_data.get("totalRevenue") or 0) if income_data.get("totalRevenue") else None
            net_income = float(income_data.get("netIncome") or 0) if income_data.get("netIncome") else None
        
        # Balance sheet metrics
        if balance_data:
            cash = float(balance_data.get("cash") or balance_data.get("cashAndCashEquivalentsAtCarryingValue") or 0) \
                if (balance_data.get("cash") or balance_data.get("cashAndCashEquivalentsAtCarryingValue")) else None
            short_debt = float(balance_data.get("shortLongTermDebt") or balance_data.get("shortTermDebt") or 0)
            long_debt = float(balance_data.get("longTermDebt") or 0)
            total_debt = short_debt + long_debt if (short_debt or long_debt) else None
        
        # Cash flow metrics
        if cashflow_data:
            ocf = float(cashflow_data.get("totalCashFromOperatingActivities") or cashflow_data.get("operatingCashFlow") or 0)
            capex = float(cashflow_data.get("capitalExpenditures") or 0)
            fcf = ocf - abs(capex) if capex else ocf
        
        return {
            "period_date": period_date,
            "revenue": revenue,
            "net_income": net_income,
            "free_cash_flow": fcf,
            "cash": cash,
            "total_debt": total_debt,
        }
    
    # P9: Build period-indexed maps for quick lookup
    def build_period_map(statements):
        """Create dict of date -> data for quick YoY lookup"""
        return {s.get("date"): s.get("data", {}) for s in statements if s.get("date")}
    
    income_q_map = build_period_map(quarterly_income)
    income_a_map = build_period_map(annual_income)
    balance_q_map = build_period_map(quarterly_balance)
    balance_a_map = build_period_map(annual_balance)
    cashflow_q_map = build_period_map(quarterly_cashflow)
    cashflow_a_map = build_period_map(annual_cashflow)
    
    # P9: Transform quarterly data with Core 5
    quarterly = []
    for inc in quarterly_income[:8]:
        period = inc.get("date")
        if not period:
            continue
        inc_data = inc.get("data", {})
        bal_data = balance_q_map.get(period, {})
        cf_data = cashflow_q_map.get(period, {})
        
        core5 = get_core5_from_period(inc_data, bal_data, cf_data, period)
        if core5.get("revenue") or core5.get("net_income"):
            quarterly.append(core5)
    
    # P9: Transform annual data with Core 5
    annual = []
    for inc in annual_income[:5]:
        period = inc.get("date")
        if not period:
            continue
        inc_data = inc.get("data", {})
        bal_data = balance_a_map.get(period, {})
        cf_data = cashflow_a_map.get(period, {})
        
        core5 = get_core5_from_period(inc_data, bal_data, cf_data, period)
        if core5.get("revenue") or core5.get("net_income"):
            annual.append(core5)
    
    # P9: Calculate TTM (current) and Prior TTM (for YoY comparison)
    ttm_revenue = None
    ttm_net_income = None
    ttm_fcf = None
    prior_ttm_revenue = None
    prior_ttm_net_income = None
    prior_ttm_fcf = None
    
    if len(quarterly) >= 4:
        ttm_revenue = sum(q.get("revenue") or 0 for q in quarterly[:4])
        ttm_net_income = sum(q.get("net_income") or 0 for q in quarterly[:4])
        ttm_fcf = sum(q.get("free_cash_flow") or 0 for q in quarterly[:4] if q.get("free_cash_flow") is not None)
        if not any(q.get("free_cash_flow") is not None for q in quarterly[:4]):
            ttm_fcf = None
    
    # Prior TTM (quarters 5-8, i.e., the 4 quarters before current TTM)
    if len(quarterly) >= 8:
        prior_ttm_revenue = sum(q.get("revenue") or 0 for q in quarterly[4:8])
        prior_ttm_net_income = sum(q.get("net_income") or 0 for q in quarterly[4:8])
        prior_ttm_fcf = sum(q.get("free_cash_flow") or 0 for q in quarterly[4:8] if q.get("free_cash_flow") is not None)
        if not any(q.get("free_cash_flow") is not None for q in quarterly[4:8]):
            prior_ttm_fcf = None
    
    # Get Cash & Debt from latest balance sheet
    cash = None
    total_debt = None
    if quarterly and quarterly[0]:
        cash = quarterly[0].get("cash")
        total_debt = quarterly[0].get("total_debt")
    
    if annual or quarterly:
        financials = {
            "annual": annual,
            "quarterly": quarterly,
            "ttm": {
                "revenue": ttm_revenue,
                "net_income": ttm_net_income,
                "free_cash_flow": ttm_fcf,
                "cash": cash,
                "total_debt": total_debt,
            },
            # P9: Prior TTM for YoY comparison of Live TTM bar
            "prior_ttm": {
                "revenue": prior_ttm_revenue,
                "net_income": prior_ttm_net_income,
                "free_cash_flow": prior_ttm_fcf,
            } if prior_ttm_revenue is not None else None
        }
    
    # Build response
    # Get safety type for badge display
    safety_type = tracked.get("safety_type", "standard")
    
    return {
        "ticker": ticker_full,
        "symbol": symbol,
        
        # Safety badge info
        "safety": {
            "type": safety_type,  # "standard" | "spac_shell" | "recent_ipo"
            "badge_text": {
                "standard": None,
                "spac_shell": "SPAC / Shell Co",
                "recent_ipo": "Recent IPO"
            }.get(safety_type),
            "badge_color": {
                "standard": None,
                "spac_shell": "amber",
                "recent_ipo": "blue"
            }.get(safety_type),
            "tooltip": {
                "standard": None,
                "spac_shell": "This is a shell company with no active business operations, created to merge with another company.",
                "recent_ipo": "This company recently went public. Historical data and valuation metrics may be limited."
            }.get(safety_type),
        },
        
        # Company identity (header row)
        "company": {
            "name": company.get("name") if company else tracked.get("name"),
            "exchange": company.get("exchange") if company else tracked.get("exchange"),
            "sector": company.get("sector") if company else tracked.get("sector"),
            "industry": company.get("industry") if company else tracked.get("industry"),
            "logo_url": company.get("logo_url") if company else tracked.get("logo_url"),
            "country": company.get("country") if company else None,
        },
        
        # Price block
        "price": {
            "current": current_price,
            "as_of": latest_price["date"] if latest_price else None,
            "daily_change": round(daily_change, 2) if daily_change else None,
            "daily_change_pct": round(daily_change_pct, 2) if daily_change_pct else None,
        },
        
        # Reality Check (ALL-TIME, fixed)
        "reality_check": reality_check,
        
        # P25/P26: PAIN details (exact max drawdown from full daily series, cached)
        "pain": pain_cache,
        
        # Period stats (for chart period)
        "period_stats": period_stats,
        
        # Valuation (locally computed)
        "valuation": valuation,
        
        # Hybrid 7 Key Metrics (P0)
        # NO 52W High/Low - removed per P0 spec
        "key_metrics": key_metrics,
        
        # Peer Transparency (P0)
        # Format: "vs 12 industry peers / 6 with valid data"
        "peer_transparency": peer_transparency,
        
        # Company details (for collapsible accordion)
        "company_details": {
            "description": company.get("description") if company else None,
            "website": company.get("website") if company else None,
            "employees": company.get("employees") or (company.get("full_time_employees") if company else None),
            "ipo_date": company.get("ipo_date") if company else None,
            "address": company.get("address") if company else None,
            "phone": company.get("phone") if company else None,
        },
        
        # P8: Financials data (5 essential metrics)
        "financials": financials,
    }


@api_router.get("/v1/ticker/{ticker}/chart")
async def get_ticker_chart_data(
    ticker: str,
    period: str = Query("1Y", description="Period: 3M, 6M, YTD, 1Y, 3Y, 5Y"),
    include_benchmark: bool = Query(True, description="Include SP500TR benchmark data")
):
    """
    Get chart data for ticker with optional benchmark overlay.
    
    Returns price data for the specified period, normalized to 100 at start.
    If include_benchmark=true, also returns SP500TR.INDX data for comparison.
    """
    ticker_upper = ticker.upper()
    ticker_full = ticker_upper if ticker_upper.endswith(".US") else f"{ticker_upper}.US"
    SP500TR_TICKER = "SP500TR.INDX"
    
    # Calculate start date based on period
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    
    if period == "3M":
        start_dt = now - timedelta(days=90)
    elif period == "6M":
        start_dt = now - timedelta(days=180)
    elif period == "YTD":
        start_dt = datetime(now.year, 1, 1, tzinfo=timezone.utc)
    elif period == "1Y":
        start_dt = now - timedelta(days=365)
    elif period == "3Y":
        start_dt = now - timedelta(days=365 * 3)
    elif period == "5Y":
        start_dt = now - timedelta(days=365 * 5)
    elif period == "MAX":
        start_dt = datetime(1950, 1, 1, tzinfo=timezone.utc)  # All history
    else:
        start_dt = now - timedelta(days=365)
    
    start_date = start_dt.strftime("%Y-%m-%d")
    
    # B: MAX Chart Fix - Smart Downsampling
    # For MAX period, get ALL data (no artificial limit) and downsample intelligently
    if period == "MAX":
        # Get ALL prices - no limit to ensure we include the latest date
        all_prices = await db.stock_prices.find(
            {"ticker": ticker_full, "date": {"$gte": start_date}},
            {"_id": 0, "date": 1, "close": 1, "adjusted_close": 1, "volume": 1}
        ).sort("date", 1).to_list(length=None)
        
        logger.info(f"[CHART] MAX: {ticker_full} has {len(all_prices)} total records")
        
        # Smart downsample: evenly distribute across ENTIRE date range
        # FIXED: Use proper sampling to cover all years
        target_points = 2000
        if len(all_prices) > target_points:
            # Calculate step to cover the entire range
            # Use float division and round indices to ensure even coverage
            import numpy as np
            indices = np.linspace(0, len(all_prices) - 1, target_points, dtype=int)
            indices = sorted(set(indices))  # Remove any duplicates from rounding
            prices = [all_prices[i] for i in indices]
        else:
            prices = all_prices
        
        logger.info(f"[CHART] MAX: {ticker_full} downsampled to {len(prices)} points (first={prices[0]['date'] if prices else 'N/A'}, last={prices[-1]['date'] if prices else 'N/A'})")
    else:
        # For non-MAX periods, use standard limit
        data_limit = 2000
        prices = await db.stock_prices.find(
            {"ticker": ticker_full, "date": {"$gte": start_date}},
            {"_id": 0, "date": 1, "close": 1, "adjusted_close": 1, "volume": 1}
        ).sort("date", 1).to_list(length=data_limit)
        
        logger.info(f"[CHART] {period}: {ticker_full} has {len(prices)} records")
    
    # Downsample for large datasets (keep ~500 points for chart, but all for calculations)
    def downsample(data, target_points=500):
        if len(data) <= target_points:
            return data
        step = len(data) // target_points
        # Always include first, last, and evenly spaced points
        return [data[i] for i in range(0, len(data), step)] + ([data[-1]] if data else [])
    
    # Get benchmark prices if requested
    benchmark_prices = []
    if include_benchmark and prices:
        # Use the actual first date from ticker prices to ensure alignment
        actual_start_date = prices[0]["date"] if prices else start_date
        
        benchmark_raw = await db.stock_prices.find(
            {"ticker": SP500TR_TICKER, "date": {"$gte": actual_start_date}},
            {"_id": 0, "date": 1, "close": 1}
        ).sort("date", 1).to_list(length=2000)
        
        # Normalize benchmark to 100 at start
        if benchmark_raw:
            start_value = benchmark_raw[0]["close"]
            for p in benchmark_raw:
                benchmark_prices.append({
                    "date": p["date"],
                    "normalized": round((p["close"] / start_value) * 100, 2) if start_value else 100
                })
    
    # Normalize ticker prices to 100 at start
    normalized_prices = []
    if prices:
        start_value = prices[0].get("adjusted_close") or prices[0]["close"]
        for p in prices:
            price_val = p.get("adjusted_close") or p["close"]
            normalized_prices.append({
                "date": p["date"],
                "close": p["close"],
                "adjusted_close": p.get("adjusted_close"),
                "normalized": round((price_val / start_value) * 100, 2) if start_value else 100,
                "volume": p.get("volume")
            })
    
    return {
        "ticker": ticker_full,
        "period": period,
        "start_date": start_date,
        "data_points": len(prices),
        "prices": normalized_prices,
        "benchmark": {
            "ticker": SP500TR_TICKER,
            "name": "S&P 500 TR",
            "prices": benchmark_prices
        } if include_benchmark else None
    }


# ----- News Endpoints (FROM CACHE - updated daily) -----

@api_router.get("/news")
async def get_news(
    tickers: str = Query(None, description="Comma-separated tickers, e.g. AAPL,MSFT,GOOGL"),
    sector: str = Query(None, description="Filter by sector"),
    include_portfolio: bool = Query(True, description="Include portfolio tickers in news"),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100)
):
    """
    P38.4 (BINDING): News & Sentiment = 3 newest articles per ticker.
    
    ==========================================================================
    P38.4: NEWS SELECTION LOGIC — IMMUTABLE WITHOUT RICHARD APPROVAL (kurtarichard@gmail.com)
    ==========================================================================
    
    1. UNIVERSE:
       - Portfolio toggle ON: tickers = Watchlist ∪ Portfolio
       - Portfolio toggle OFF: tickers = Watchlist only
       - Always apply is_visible=true
    
    2. SELECTION RULE (HARD):
       - For each ticker: fetch up to 3 newest articles from news_article_symbols → news_articles
       - No title heuristics, no guessing
    
    3. OUTPUT SIZE:
       - Total = min(3 * N, available_articles)
       - If N=16, target up to 48 items
    
    4. ORDERING: Interleaved by recency (max 3 per ticker, sorted by published_at DESC)
    
    ==========================================================================
    LOGO GUARANTEE — DO NOT REMOVE WITHOUT RICHARD APPROVAL
    ==========================================================================
    """
    
    # ==========================================================================
    # P38.4 Step 1: BUILD TICKER UNIVERSE (portfolio toggle support)
    # ==========================================================================
    
    if tickers:
        # If explicitly provided, use those (for ticker-specific pages)
        ticker_list = [t.strip().upper().replace(".US", "") for t in tickers.split(",")]
    else:
        # P38.4: Portfolio toggle affects universe
        # Step 1: Get watchlist tickers (always included)
        watchlist_raw = await db.user_watchlist.distinct("ticker")
        watchlist_tickers = set(t.upper().replace(".US", "") for t in watchlist_raw if t)
        
        # Step 2: Get portfolio tickers (only if include_portfolio=True)
        if include_portfolio:
            portfolio_docs = await db.positions.find(
                {"shares": {"$gt": 0}},
                {"_id": 0, "ticker": 1}
            ).to_list(length=None)
            portfolio_tickers = set(
                doc["ticker"].upper().replace(".US", "") 
                for doc in portfolio_docs 
                if doc.get("ticker")
            )
        else:
            portfolio_tickers = set()
        
        # Step 3: Union based on toggle
        all_followed = watchlist_tickers | portfolio_tickers
        
        if not all_followed:
            return {
                "news": [],
                "offset": offset,
                "limit": limit,
                "total": 0,
                "has_more": False,
                "followed_tickers": [],
                "ticker_count": 0,
                "include_portfolio": include_portfolio,
                "aggregate_sentiment": {
                    "score": 0,
                    "label": "neutral",
                    "color": "#F59E0B",
                }
            }
        
        # Step 4: Filter to only is_visible=true tickers
        all_full = [f"{t}.US" for t in all_followed]
        visible_docs = await db.tracked_tickers.find(
            {"ticker": {"$in": all_full}, "is_visible": True},
            {"_id": 0, "ticker": 1}
        ).to_list(length=None)
        
        visible_and_followed = [doc["ticker"].replace(".US", "") for doc in visible_docs]
        
        # P38.4: NO FALLBACK - if no followed+visible tickers, return empty
        if not visible_and_followed:
            return {
                "news": [],
                "offset": offset,
                "limit": limit,
                "total": 0,
                "has_more": False,
                "followed_tickers": [],
                "ticker_count": 0,
                "include_portfolio": include_portfolio,
                "aggregate_sentiment": {
                    "score": 0,
                    "label": "neutral",
                    "color": "#F59E0B",
                }
            }
        
        ticker_list = visible_and_followed
    
    # ==========================================================================
    # P38.4 Step 2: FETCH COMPANY INFO FOR LOGOS
    # ==========================================================================
    ticker_full_list = [f"{t}.US" for t in ticker_list]
    company_docs = await db.company_fundamentals_cache.find(
        {"ticker": {"$in": ticker_full_list}},
        {"_id": 0, "ticker": 1, "logo_url": 1, "name": 1}
    ).to_list(length=None)
    
    ticker_info = {}
    for doc in company_docs:
        ticker = doc.get("ticker", "").replace(".US", "").upper()
        logo_url = doc.get("logo_url")
        if logo_url and not logo_url.startswith("http"):
            logo_url = f"https://eodhistoricaldata.com{logo_url}"
        ticker_info[ticker] = {
            "logo_url": logo_url,
            "name": doc.get("name", ticker),
        }
    
    # ==========================================================================
    # P53 Step 3: FETCH NEWS FROM article_ticker_mapping JOIN TABLE
    # ==========================================================================
    # P53: Use article_ticker_mapping table (ticker field, max 3 per ticker)
    # Get article_ids mapped to our followed tickers
    article_mappings = await db.article_ticker_mapping.find(
        {"ticker": {"$in": ticker_list}},
        {"_id": 0, "article_id": 1, "ticker": 1, "rank": 1}
    ).sort([("ticker", 1), ("rank", 1)]).to_list(length=None)
    
    # Fallback to old table if new one is empty (migration period)
    if not article_mappings:
        article_mappings = await db.news_article_symbols.find(
            {"symbol": {"$in": ticker_list}},
            {"_id": 0, "article_id": 1, "symbol": 1}
        ).to_list(length=None)
        # Normalize field name
        for m in article_mappings:
            if "symbol" in m and "ticker" not in m:
                m["ticker"] = m["symbol"]
    
    # Build article_id -> tickers map (article can have multiple tickers)
    article_to_tickers = {}
    for mapping in article_mappings:
        aid = mapping.get("article_id")
        ticker = mapping.get("ticker")
        if aid and ticker:
            if aid not in article_to_tickers:
                article_to_tickers[aid] = []
            article_to_tickers[aid].append(ticker)
    
    # P53: Fetch articles if we have mappings
    if article_to_tickers:
        article_ids = list(article_to_tickers.keys())
        all_articles = await db.news_articles.find(
            {"article_id": {"$in": article_ids}},
            {"_id": 0}
        ).sort("published_at", -1).to_list(length=None)
    else:
        all_articles = []
    
    # ==========================================================================
    # P38.4 Step 4: APPLY "3 PER TICKER" RULE (HARD)
    # ==========================================================================
    # P38.4: For each ticker, fetch up to 3 newest articles - NO EXCEPTIONS
    PER_TICKER_CAP = 3
    
    # Group articles by their primary ticker (first mapped ticker)
    articles_by_ticker = {}
    for article in all_articles:
        aid = article.get("article_id")
        if not aid:
            link = article.get("link", "")
            import hashlib
            aid = hashlib.md5(link.encode()).hexdigest()[:16] if link else None
        
        # P53: Use article_to_tickers from mapping table
        tickers_for_article = article_to_tickers.get(aid, [])
        if not tickers_for_article:
            continue
        
        # Use first ticker that's in our followed list
        primary_ticker = None
        for t in tickers_for_article:
            if t in ticker_list:
                primary_ticker = t
                break
        
        if not primary_ticker:
            continue
        
        article["_display_ticker"] = primary_ticker
        
        if primary_ticker not in articles_by_ticker:
            articles_by_ticker[primary_ticker] = []
        # P38.4: HARD CAP of 3 per ticker
        if len(articles_by_ticker[primary_ticker]) < PER_TICKER_CAP:
            articles_by_ticker[primary_ticker].append(article)
    
    # P38.4: Interleaved by recency (max 3 per ticker, sorted by published_at DESC)
    final_articles = []
    ticker_indices = {t: 0 for t in articles_by_ticker}
    max_rounds = max(len(arts) for arts in articles_by_ticker.values()) if articles_by_ticker else 0
    
    for round_num in range(max_rounds):
        for ticker in sorted(articles_by_ticker.keys()):  # Alphabetical for consistency
            idx = ticker_indices[ticker]
            if idx < len(articles_by_ticker[ticker]):
                final_articles.append(articles_by_ticker[ticker][idx])
                ticker_indices[ticker] += 1
    
    # Apply pagination
    total_count = len(final_articles)
    paginated = final_articles[offset:offset + limit]
    
    # ==========================================================================
    # P38.3 Step 5: BUILD RESPONSE WITH LOGOS
    # ==========================================================================
    cached_news = []
    for article in paginated:
        # P38.3: Use _display_ticker (mapped ticker) instead of raw ticker
        ticker = article.get("_display_ticker") or article.get("ticker", "").upper()
        info = ticker_info.get(ticker, {})
        logo_url = info.get("logo_url")
        company_name = info.get("name", ticker)
        
        # Logo guarantee
        fallback_logo_key = ticker[0].upper() if ticker else "N"
        if not logo_url and ticker:
            logo_url = f"https://eodhd.com/img/logos/US/{ticker}.png"
        
        # Calculate time_ago
        published_at = article.get("date") or article.get("published_at")
        time_ago = "Unknown"
        if published_at:
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            if isinstance(published_at, str):
                try:
                    pub_dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                except:
                    pub_dt = now
            else:
                pub_dt = published_at
            
            delta = now - pub_dt
            hours = delta.total_seconds() / 3600
            if hours < 1:
                time_ago = f"{int(delta.total_seconds() / 60)}m ago"
            elif hours < 24:
                time_ago = f"{int(hours)}h ago"
            else:
                time_ago = f"{int(hours / 24)}d ago"
        
        # Generate article ID from link
        import hashlib
        link = article.get("link", "") or article.get("source_link", "")
        article_id = hashlib.md5(link.encode()).hexdigest()[:16] if link else str(len(cached_news))
        
        # Extract source name from URL
        source_name = "Unknown"
        if link:
            try:
                from urllib.parse import urlparse
                parsed = urlparse(link)
                domain = parsed.netloc.replace("www.", "")
                source_map = {
                    "finance.yahoo.com": "Yahoo Finance",
                    "yahoo.com": "Yahoo",
                    "reuters.com": "Reuters",
                    "bloomberg.com": "Bloomberg",
                    "cnbc.com": "CNBC",
                    "wsj.com": "WSJ",
                    "marketwatch.com": "MarketWatch",
                    "fool.com": "Motley Fool",
                    "seekingalpha.com": "Seeking Alpha",
                    "investorplace.com": "InvestorPlace",
                    "benzinga.com": "Benzinga",
                    "barrons.com": "Barron's",
                }
                source_name = source_map.get(domain, domain.split('.')[0].capitalize())
            except:
                pass
        
        cached_news.append({
            "id": article_id,
            "title": article.get("title"),
            "content": article.get("content", ""),
            "source": source_name,
            "link": link,
            "date": published_at,
            "ticker": ticker,
            "company_name": company_name,
            "logo_url": logo_url,
            "fallback_logo_key": fallback_logo_key,
            "sentiment": article.get("sentiment", {}),
            "sentiment_label": article.get("sentiment_label", "neutral"),
            "tags": article.get("tags", []),
            "time_ago": time_ago,
        })
    
    # Calculate has_more
    has_more = (offset + len(cached_news)) < total_count
    
    # Calculate aggregate sentiment
    sentiment_scores = []
    for news_item in cached_news:
        label = news_item.get("sentiment_label", "neutral")
        if label == "positive":
            sentiment_scores.append(1)
        elif label == "negative":
            sentiment_scores.append(-1)
        else:
            sentiment_scores.append(0)
    
    aggregate_sentiment_score = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0
    
    if aggregate_sentiment_score > 0.3:
        aggregate_sentiment_label = "positive"
        aggregate_sentiment_color = "#10B981"
    elif aggregate_sentiment_score < -0.3:
        aggregate_sentiment_label = "negative"
        aggregate_sentiment_color = "#EF4444"
    else:
        aggregate_sentiment_label = "neutral"
        aggregate_sentiment_color = "#F59E0B"
    
    # P38.4: Count unique tickers appearing in results
    tickers_in_results = set(n.get("ticker") for n in cached_news if n.get("ticker"))
    
    return {
        "news": cached_news,
        "offset": offset,
        "limit": limit,
        "total": total_count,
        "has_more": has_more,
        "followed_tickers": ticker_list,
        "ticker_count": len(ticker_list),
        "tickers_with_news": list(tickers_in_results),
        "tickers_with_news_count": len(tickers_in_results),
        "include_portfolio": include_portfolio if 'include_portfolio' in dir() else True,
        "per_ticker_cap": 3,
        "aggregate_sentiment": {
            "score": round(aggregate_sentiment_score, 2),
            "label": aggregate_sentiment_label,
            "color": aggregate_sentiment_color,
        }
    }


# =============================================================================
# DEPRECATED: Legacy news_cache function (P30 - no longer used)
# =============================================================================
# This function wrote to news_cache collection which is now deprecated.
# News is now stored in news_articles + news_article_symbols by news_service.
# DO NOT use this function. Keeping for reference only.
# =============================================================================
async def refresh_news_cache_for_tickers(ticker_list: list):
    """
    DEPRECATED (P30): Use news_service.refresh_hot_tickers_news() instead.
    
    This function is kept for backward compatibility but is no longer used.
    News is now stored in news_articles collection by the scheduler job.
    """
    logger.warning("DEPRECATED: refresh_news_cache_for_tickers called - use news_service instead")
    return {"status": "deprecated", "message": "Use news_service.refresh_hot_tickers_news()"}


def format_time_ago(dt: datetime) -> str:
    """Format datetime as 'X hours ago', 'X days ago', etc."""
    # Ensure dt is naive (remove timezone info if present)
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    
    now = datetime.utcnow()
    diff = now - dt
    
    if diff.days > 0:
        if diff.days == 1:
            return "1d ago"
        return f"{diff.days}d ago"
    
    hours = diff.seconds // 3600
    if hours > 0:
        return f"{hours}h ago"
    
    minutes = diff.seconds // 60
    if minutes > 0:
        return f"{minutes}m ago"
    
    return "just now"


def extract_source(url: str) -> str:
    """Extract source name from URL."""
    if not url:
        return "News"
    
    # Common news sources
    sources = {
        "yahoo": "Yahoo Finance",
        "reuters": "Reuters",
        "bloomberg": "Bloomberg",
        "cnbc": "CNBC",
        "wsj": "WSJ",
        "ft.com": "FT",
        "marketwatch": "MarketWatch",
        "benzinga": "Benzinga",
        "seekingalpha": "Seeking Alpha",
        "fool": "Motley Fool",
        "investopedia": "Investopedia",
        "barrons": "Barron's",
    }
    
    url_lower = url.lower()
    for key, name in sources.items():
        if key in url_lower:
            return name
    
    return "News"


@api_router.get("/news/ticker/{ticker}")
async def get_ticker_news(
    ticker: str,
    limit: int = 10,
    offset: int = 0,
    request: Request = None
):
    """
    P53: Get news for a specific ticker (detail view).
    
    - Returns up to 100 articles for the ticker (pagination with limit/offset)
    - PRO users: can see all 100
    - Free users: only first 10 (then "Upgrade to PRO")
    """
    db = request.app.state.db
    ticker = ticker.upper()
    
    # Get user subscription tier
    user = None
    is_pro = False
    try:
        if hasattr(request.state, "user"):
            user = request.state.user
            is_pro = user.get("subscription_tier") in ["pro", "pro_plus"]
    except:
        pass
    
    # Free users limited to 10 articles
    FREE_LIMIT = 10
    PRO_LIMIT = 100
    
    max_limit = PRO_LIMIT if is_pro else FREE_LIMIT
    effective_limit = min(limit, max_limit - offset)
    
    if effective_limit <= 0:
        return {
            "ticker": ticker,
            "articles": [],
            "offset": offset,
            "limit": limit,
            "total": 0,
            "has_more": False,
            "upgrade_required": not is_pro,
        }
    
    # Get mappings for this ticker (sorted by published_at desc)
    mappings = await db.article_ticker_mapping.find(
        {"ticker": ticker},
        {"_id": 0, "article_id": 1, "rank": 1}
    ).sort("published_at", -1).skip(offset).limit(effective_limit).to_list(length=effective_limit)
    
    total_count = await db.article_ticker_mapping.count_documents({"ticker": ticker})
    
    if not mappings:
        return {
            "ticker": ticker,
            "articles": [],
            "offset": offset,
            "limit": limit,
            "total": total_count,
            "has_more": False,
            "upgrade_required": False,
        }
    
    # Fetch articles
    article_ids = [m["article_id"] for m in mappings]
    articles = await db.news_articles.find(
        {"article_id": {"$in": article_ids}},
        {"_id": 0}
    ).to_list(length=None)
    
    # Map articles by ID for ordering
    articles_by_id = {a["article_id"]: a for a in articles}
    
    # Build response with articles in order
    result_articles = []
    for m in mappings:
        article = articles_by_id.get(m["article_id"])
        if article:
            result_articles.append({
                "article_id": article.get("article_id"),
                "title": article.get("title"),
                "content": article.get("content", "")[:500] + "..." if len(article.get("content", "")) > 500 else article.get("content", ""),
                "published_at": article.get("published_at"),
                "source_link": article.get("source_link"),
                "sentiment_label": article.get("sentiment_label"),
                "eodhd_symbols_raw": article.get("eodhd_symbols_raw", []),
            })
    
    # Determine if there are more articles
    has_more = (offset + len(result_articles)) < min(total_count, max_limit)
    upgrade_required = not is_pro and total_count > FREE_LIMIT and (offset + limit) >= FREE_LIMIT
    
    return {
        "ticker": ticker,
        "articles": result_articles,
        "offset": offset,
        "limit": limit,
        "total": total_count,
        "has_more": has_more,
        "upgrade_required": upgrade_required,
    }


@api_router.get("/news/{article_id}")
async def get_news_article(article_id: str):
    """
    Get full article content by ID.
    
    Note: For now, we don't cache articles. 
    Frontend should store the article data when fetching the list.
    """
    # In future, we could cache articles in MongoDB
    # For now, return a message that article should be accessed via the list
    raise HTTPException(404, "Article not found. Please access articles via /api/news endpoint.")


# ----- Scheduler Endpoints -----

from scheduler_service import (
    get_scheduler_enabled,
    set_scheduler_enabled,
    get_scheduler_status,
    run_daily_price_sync,
    run_fundamentals_changes_sync,
    STEP3_QUERY,
    _to_prague_iso as _sched_to_prague_iso,
)

@api_router.get("/admin/scheduler/status")
async def admin_scheduler_status():
    """
    Get comprehensive scheduler status.
    
    Returns:
        - scheduler_enabled: Whether scheduler is running
        - kill_switch_engaged: Inverse of enabled
        - schedule: Configured schedule times
        - last_runs: Last run times for each job type
        - pending_work: Work queued for next runs
    """
    status = await get_scheduler_status(db)
    return status

@api_router.post("/admin/scheduler/kill-switch")
async def admin_scheduler_kill_switch(enabled: bool = Query(..., description="True to enable scheduler, False to engage kill switch")):
    """
    Enable or disable the scheduler (kill switch).
    
    When kill switch is engaged (enabled=False):
    - Scheduled jobs will NOT run
    - Manual admin API calls still work
    
    Args:
        enabled: True to enable scheduler, False to stop scheduled jobs
    
    Returns:
        Updated config
    """
    result = await set_scheduler_enabled(db, enabled)
    return result


# =============================================================================
# ADMIN JOB CONTROL ENDPOINTS (P1: Manual Jobs)
# =============================================================================

@api_router.post("/admin/job/{job_name}/toggle")
async def admin_toggle_job(job_name: str):
    """
    Toggle job enabled/disabled state for scheduled runs.
    Uses ops_config collection to persist state.
    
    Manual jobs (like backfill_all) are disabled by default.
    Toggling to "enabled" allows them to run on their schedule.
    
    Args:
        job_name: Name of the job (e.g., "backfill_all")
    
    Returns:
        Updated config with new enabled state
    """
    from datetime import timezone
    
    config_key = f"job_{job_name}_enabled"
    current = await db.ops_config.find_one({"key": config_key})
    
    # Default is False for manual jobs, True for others
    current_value = current.get("value", False) if current else False
    new_value = not current_value
    
    await db.ops_config.update_one(
        {"key": config_key},
        {"$set": {
            "key": config_key,
            "value": new_value,
            "updated_at": datetime.now(timezone.utc),
            "updated_by": "admin_api"
        }},
        upsert=True
    )
    
    logger.info(f"Job {job_name} toggled: enabled={new_value}")
    return {"job": job_name, "enabled": new_value, "config_key": config_key}


@api_router.get("/admin/job/{job_name}/status")
async def admin_get_job_status(job_name: str):
    """
    Get current enabled/disabled status of a job.
    READ-ONLY: no finalization side-effects (page refresh must never mutate).

    Returns:
        Job config and last run info
    """

    config_key = f"job_{job_name}_enabled"
    config = await db.ops_config.find_one({"key": config_key}, {"_id": 0})
    
    def _iso(dt):
        if not dt:
            return None
        return dt.isoformat() if hasattr(dt, "isoformat") else str(dt)

    # Get last run (latest by started_at, no status filter)
    raw_last_run = await db.ops_job_runs.find_one(
        {"job_name": job_name},
        {"_id": 0},
        sort=[("started_at", -1)]
    )

    last_run = None
    if raw_last_run:
        details = raw_last_run.get("details") or {}
        started_at = raw_last_run.get("started_at")
        finished_at = raw_last_run.get("finished_at")
        seeded_total = (
            raw_last_run.get("progress_total")
            or details.get("seeded_total")
            or details.get("tickers_seeded_total")
        )
        tickers_with_price_data = details.get("tickers_with_price_data")
        records_upserted = details.get("records_upserted") or raw_last_run.get("records_upserted")
        phase = raw_last_run.get("phase") or details.get("phase")
        duration_seconds = None
        if started_at and finished_at:
            duration_seconds = (finished_at - started_at).total_seconds()

        last_run = {
            "status": raw_last_run.get("status"),
            "started_at": _iso(started_at),
            "finished_at": _iso(finished_at),
            "started_at_prague": raw_last_run.get("started_at_prague"),
            "finished_at_prague": raw_last_run.get("finished_at_prague"),
            "progress_processed": raw_last_run.get("progress_processed"),
            "progress_total": seeded_total or raw_last_run.get("progress_total"),
            "progress_pct": raw_last_run.get("progress_pct"),
            "phase": phase,
            "duration_seconds": duration_seconds,
            "details": {
                **details,
                "seeded_total": seeded_total,
                "tickers_with_price_data": tickers_with_price_data,
                "records_upserted": records_upserted,
                "phase": phase,
            },
        }

    # Get previous completed run (most recent successful/completed run)
    previous_completed_run = None
    raw_prev = await db.ops_job_runs.find_one(
        {"job_name": job_name, "status": {"$in": ["success", "completed"]}},
        {"_id": 0},
        sort=[("finished_at", -1)],
    )
    if raw_prev:
        prev_started = raw_prev.get("started_at")
        prev_finished = raw_prev.get("finished_at")
        prev_duration = None
        if prev_started and prev_finished:
            prev_duration = (prev_finished - prev_started).total_seconds()
        previous_completed_run = {
            "status": raw_prev.get("status"),
            "started_at": _iso(prev_started),
            "finished_at": _iso(prev_finished),
            "started_at_prague": raw_prev.get("started_at_prague"),
            "finished_at_prague": raw_prev.get("finished_at_prague"),
            "duration_seconds": prev_duration,
        }

    return {
        "job": job_name,
        "enabled": config.get("value", False) if config else False,
        "config_key": config_key,
        "last_run": last_run,
        "previous_completed_run": previous_completed_run,
    }


# C2: Enhanced audit trail helper functions
async def create_job_audit_entry(database, job_name: str, triggered_by: str = "admin_api") -> str:
    """Create initial audit entry for manual job run with inventory snapshot."""
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    PRAGUE = ZoneInfo("Europe/Prague")
    started_at = datetime.now(timezone.utc)
    
    # Get inventory snapshot BEFORE
    inventory_before = {
        "stock_prices_total": await database.stock_prices.count_documents({}),
        "stock_prices_tickers": len(await database.stock_prices.distinct("ticker")),
        "fundamentals_total": await database.company_fundamentals_cache.count_documents({}),
        "financials_total": await database.financials_cache.count_documents({}),
    }
    
    audit_doc = {
        "job_name": job_name,
        "job_type": "manual_ad_hoc",
        "started_at": started_at,
        "started_at_prague": started_at.astimezone(PRAGUE).isoformat(),
        "finished_at": None,
        "finished_at_prague": None,
        "log_timezone": "Europe/Prague",
        "triggered_by": triggered_by,
        "trigger_source": "Admin Panel",
        "tickers_targeted": 0,
        "tickers_updated": 0,
        "tickers_skipped": 0,
        "tickers_failed": 0,
        "api_calls": 0,
        "api_credits_estimated": 0,
        "inventory_snapshot_before": inventory_before,
        "inventory_snapshot_after": None,
        "status": "running",
        "error_message": None,
    }
    
    result = await database.ops_job_runs.insert_one(audit_doc)
    return str(result.inserted_id)


async def finalize_job_audit_entry(database, audit_id: str, result: dict = None, error: str = None):
    """Update audit entry after job completion with inventory snapshot AFTER."""
    from datetime import datetime, timezone
    from bson import ObjectId
    from zoneinfo import ZoneInfo
    PRAGUE = ZoneInfo("Europe/Prague")
    finished_at = datetime.now(timezone.utc)
    
    # Get inventory snapshot AFTER
    inventory_after = {
        "stock_prices_total": await database.stock_prices.count_documents({}),
        "stock_prices_tickers": len(await database.stock_prices.distinct("ticker")),
        "fundamentals_total": await database.company_fundamentals_cache.count_documents({}),
        "financials_total": await database.financials_cache.count_documents({}),
    }
    
    update_doc = {
        "finished_at": finished_at,
        "finished_at_prague": finished_at.astimezone(PRAGUE).isoformat(),
        "inventory_snapshot_after": inventory_after,
        "status": "error" if error else "completed",
    }
    
    if error:
        update_doc["error_message"] = str(error)[:1000]
    
    if result:
        update_doc["tickers_updated"] = result.get("tickers_updated", 0)
        update_doc["api_calls"] = result.get("api_calls", 0)
    
    await database.ops_job_runs.update_one(
        {"_id": ObjectId(audit_id)},
        {"$set": update_doc}
    )


@api_router.post("/admin/job/{job_name}/run")
async def admin_run_job_now(job_name: str, background_tasks: BackgroundTasks, wait: bool = Query(False)):
    """
    Manually trigger a job immediately (bypasses schedule).
    Creates full audit trail with inventory snapshots.
    
    Args:
        job_name: Name of the job to run
        wait: If True, wait for completion (may timeout)
    
    Returns:
        Job execution result or started status
    """
    # Pipeline steps must only be run via the full sequential chain.
    # Note: universe_seed, price_sync, and fundamentals_sync are blocked in their
    # own dedicated endpoints above. recompute_visibility_all (visibility recompute) is
    # accessible via this generic endpoint, so it is blocked here.
    _PIPELINE_STEP_JOBS = {"recompute_visibility_all"}
    if job_name in _PIPELINE_STEP_JOBS:
        raise HTTPException(
            status_code=403,
            detail={
                "error": "per_step_run_disabled",
                "message": "Individual step execution is disabled. Use the full sequential pipeline run (Run Full Pipeline Now).",
            },
        )
    from parallel_batch_service import run_scheduled_backfill_all_prices
    from visibility_rules import recompute_visibility_all, clean_zombie_tickers, recompute_visibility_with_zombie_cleanup
    from scheduler_service import save_step3_visibility_exclusion_report as _save_step3_vis_report
    from benchmark_service import update_all_benchmarks

    async def _recompute_visibility_and_report(database):
        """Run recompute_visibility_all then regenerate Step 3 visibility exclusion report."""
        result = await recompute_visibility_all(database, parent_run_id=None)
        now_vis = datetime.now(timezone.utc)
        vis_report = await _save_step3_vis_report(database, now_vis)
        result["step3_visibility_exclusion_report"] = {
            "rows_written": vis_report.get("visibility_exclusion_rows", 0),
            "_debug":       vis_report.get("_debug", {}),
        }
        result["exclusion_report_run_id"] = vis_report.get("exclusion_report_run_id")
        return result

    JOB_RUNNERS = {
        "backfill_all": run_scheduled_backfill_all_prices,
        "recompute_visibility_all": _recompute_visibility_and_report,
        "clean_zombie_tickers": clean_zombie_tickers,
        "recompute_visibility_with_zombies": recompute_visibility_with_zombie_cleanup,
        "benchmark_update": update_all_benchmarks,
    }
    
    if job_name not in JOB_RUNNERS:
        raise HTTPException(
            status_code=400, 
            detail=f"Unknown or non-runnable job: {job_name}. Available: {list(JOB_RUNNERS.keys())}"
        )
    
    job_func = JOB_RUNNERS[job_name]
    
    logger.info(f"Admin manually triggering job: {job_name}")
    
    # C2: Create audit entry with inventory snapshot BEFORE
    audit_id = await create_job_audit_entry(db, job_name)
    
    if wait:
        try:
            result = await job_func(db)
            # C2: Finalize audit with inventory snapshot AFTER
            await finalize_job_audit_entry(db, audit_id, result=result)
            return {"job": job_name, "status": "completed", "result": result, "audit_id": audit_id}
        except Exception as e:
            logger.error(f"Job {job_name} failed: {e}")
            await finalize_job_audit_entry(db, audit_id, error=str(e))
            return {"job": job_name, "status": "error", "error": str(e), "audit_id": audit_id}
    
    # Background execution with audit trail
    async def run_with_audit():
        try:
            result = await job_func(db)
            await finalize_job_audit_entry(db, audit_id, result=result)
        except Exception as e:
            logger.error(f"Job {job_name} failed in background: {e}")
            await finalize_job_audit_entry(db, audit_id, error=str(e))
    
    # Run in background with audit trail
    background_tasks.add_task(run_with_audit)
    return {
        "job": job_name,
        "status": "started",
        "audit_id": audit_id,
        "message": "Job started in background with audit trail. Check Admin Panel for results."
    }


@api_router.post("/admin/scheduler/run/price-sync")
async def admin_manual_price_sync(background_tasks: BackgroundTasks, wait: bool = Query(False, description="Wait for completion (may timeout)")):
    """Individually triggering pipeline steps is disabled. Use the full sequential run instead."""
    raise HTTPException(
        status_code=403,
        detail={
            "error": "per_step_run_disabled",
            "message": "Individual step execution is disabled. Use the full sequential pipeline run (Run Full Pipeline Now).",
        },
    )

@api_router.post("/admin/scheduler/run/fundamentals-sync")
async def admin_manual_fundamentals_sync(background_tasks: BackgroundTasks, batch_size: int = Query(10000, ge=1, le=10000), wait: bool = Query(False)):
    """Individually triggering pipeline steps is disabled. Use the full sequential run instead."""
    raise HTTPException(
        status_code=403,
        detail={
            "error": "per_step_run_disabled",
            "message": "Individual step execution is disabled. Use the full sequential pipeline run (Run Full Pipeline Now).",
        },
    )

@api_router.post("/admin/scheduler/run/peer-medians")
async def admin_manual_peer_medians(background_tasks: BackgroundTasks):
    """Individually triggering pipeline steps is disabled. Use the full sequential run instead."""
    raise HTTPException(
        status_code=403,
        detail={
            "error": "per_step_run_disabled",
            "message": "Individual step execution is disabled. Use the full sequential pipeline run (Run Full Pipeline Now).",
        },
    )


@api_router.post("/admin/jobs/{job_name}/cancel")
async def admin_cancel_job(job_name: str):
    """
    Request cancellation of a running job.
    Sets a cancellation flag in ops_config. Running jobs check this flag
    and abort gracefully at the next safe checkpoint.
    """
    from datetime import timezone
    now = datetime.now(timezone.utc)
    await db.ops_config.update_one(
        {"key": f"cancel_job_{job_name}"},
        {"$set": {"key": f"cancel_job_{job_name}", "value": True, "requested_at": now}},
        upsert=True,
    )
    logger.info(f"Cancel requested for job: {job_name}")
    return {"job_name": job_name, "cancel_requested": True, "requested_at": now.isoformat()}


@api_router.delete("/admin/jobs/{job_name}/cancel")
async def admin_clear_cancel_flag(job_name: str):
    """Clear the cancellation flag for a job (called after job stops)."""
    await db.ops_config.delete_one({"key": f"cancel_job_{job_name}"})
    return {"job_name": job_name, "cancel_cleared": True}


@api_router.post("/admin/jobs/cancel_running")
async def admin_cancel_running_job(job_name: str = Query(..., description="fundamentals_sync|price_sync")):
    """
    Request cancellation for the latest running Step 2/3 job.
    Marks the run as cancel_requested (does not finalize timestamps).
    Auth is enforced by AdminAuthMiddleware for /api/admin/* routes.
    """
    from services.admin_jobs_service import (
        VALID_CANCEL_RUNNING_JOB_NAMES,
        cancel_latest_running_job,
        is_valid_cancel_running_job_name,
    )

    if not is_valid_cancel_running_job_name(job_name):
        allowed = "|".join(sorted(VALID_CANCEL_RUNNING_JOB_NAMES))
        raise HTTPException(
            status_code=400,
            detail=f"Invalid job_name '{job_name}'. Expected one of: {allowed}",
        )

    from scheduler_service import finalize_stuck_admin_job_runs
    await finalize_stuck_admin_job_runs(db, job_names=[job_name])

    result = await cancel_latest_running_job(db, job_name)
    if not result:
        raise HTTPException(status_code=404, detail=f"No running job found for {job_name}")

    logger.info(f"Cancel-running requested for job: {job_name} run_id={result['run_id']}")
    return result


@api_router.post("/admin/jobs/enqueue-manual-refresh/run")
async def admin_enqueue_manual_refresh():
    """
    Enqueue manual fundamentals refresh for 28 tickers confirmed stuck in Step 3.

    For each ticker:
      - Ensures exactly ONE fundamentals_events doc per (ticker, event_type='manual_refresh').
        pending/processing → touch updated_at only.
        terminal status   → reset to pending, clear finished timestamps.
        missing           → insert fresh.
      - Sets tracked_tickers.needs_fundamentals_refresh=True so the skip-gate
        in run_fundamentals_changes_sync lets the ticker through.

    Protected by AdminAuthMiddleware (same as all /api/admin/* endpoints).
    Run Step 3 after calling this endpoint.
    """
    _REFRESH_TICKERS = [
        "AME.US", "AMP.US", "AMRC.US", "AMX.US", "AON.US", "APAM.US",
        "APLE.US", "APTV.US", "AR.US", "AREN.US", "ARL.US", "AROC.US",
        "AS.US", "ASC.US", "ASIX.US", "ASX.US", "ATNM.US", "AVA.US",
        "AVD.US", "AWX.US", "AXIA-P.US", "AXS.US", "AZTR.US", "BABA.US",
        "BALL.US", "BB.US", "BBU.US", "BCC.US",
    ]

    from zoneinfo import ZoneInfo as _ZoneInfo
    _PRAGUE_TZ = _ZoneInfo("Europe/Prague")

    now = datetime.now(timezone.utc)
    event_type = "manual_refresh"
    source_job = "admin_enqueue_manual_refresh"

    inserted = 0
    reset = 0
    skipped = 0
    flags_set = 0
    detail_rows = []

    for ticker in _REFRESH_TICKERS:
        existing = await db.fundamentals_events.find_one(
            {"ticker": ticker, "event_type": event_type},
            {"_id": 1, "status": 1},
        )

        if existing is None:
            await db.fundamentals_events.insert_one({
                "ticker":        ticker,
                "event_type":    event_type,
                "status":        "pending",
                "source_job":    source_job,
                "detector_step": "manual",
                "created_at":    now,
                "updated_at":    now,
            })
            inserted += 1
            action = "inserted"
        elif existing["status"] in ("pending", "processing"):
            await db.fundamentals_events.update_one(
                {"_id": existing["_id"]},
                {"$set": {"updated_at": now}},
            )
            skipped += 1
            action = "skipped"
        else:
            await db.fundamentals_events.update_one(
                {"_id": existing["_id"]},
                {"$set": {
                    "status":         "pending",
                    "source_job":     source_job,
                    "updated_at":     now,
                    "completed_at":   None,
                    "skipped_at":     None,
                    "deduped_at":     None,
                    "skipped_reason": None,
                }},
            )
            reset += 1
            action = "reset"

        flag_result = await db.tracked_tickers.update_one(
            {"ticker": ticker},
            {"$set": {"needs_fundamentals_refresh": True, "updated_at": now}},
        )
        if flag_result.modified_count:
            flags_set += 1

        detail_rows.append({"ticker": ticker, "action": action})

    return {
        "inserted":          inserted,
        "reset":             reset,
        "skipped":           skipped,
        "flags_set":         flags_set,
        "tickers_processed": len(_REFRESH_TICKERS),
        "detail":            detail_rows,
    }


@api_router.get("/admin/jobs/{job_name}/status")
async def admin_job_status(job_name: str):
    """Get the latest run status for a specific job.
    READ-ONLY: no finalization side-effects (page refresh must never mutate).
    """

    run = await db.ops_job_runs.find_one(
        {"job_name": job_name},
        {"_id": 0, "status": 1, "started_at": 1, "finished_at": 1,
         "details": 1, "records_upserted": 1, "progress": 1,
         "progress_processed": 1, "progress_total": 1, "progress_pct": 1,
         "phase": 1},
        sort=[("started_at", -1)],
    )
    cancel_requested = bool(run and run.get("status") == "cancel_requested")

    # Real-time DB counts for fundamentals jobs — scoped to the canonical Step 3
    # universe (NYSE+NASDAQ Common Stock with price data) so they match the
    # "Tickers with prices" count shown in the Step 3 pipeline card.
    db_complete_count: Optional[int] = None
    db_pending_count:  Optional[int] = None
    db_total_count:    Optional[int] = None
    # Ticker-level Step 3 funnel counts (additive — do not replace db_* fields).
    # "Up-to-date" = fundamentals_status='complete'
    #               AND needs_fundamentals_refresh != True
    #               AND fundamentals_updated_at not null/missing
    step3_input_total:        Optional[int] = None
    step3_output_total:       Optional[int] = None
    step3_filtered_out_total: Optional[int] = None
    if job_name in ("fundamentals_sync", "full_fundamentals_sync"):
        # Use the canonical STEP3_QUERY imported from scheduler_service —
        # same constant the job itself now uses for universe scoping.
        db_total_count = await db.tracked_tickers.count_documents(STEP3_QUERY)
        db_complete_count = await db.tracked_tickers.count_documents(
            {**STEP3_QUERY, "fundamentals_status": "complete"}
        )
        # Canonical Step 3 pending work: tracked_tickers flag only.
        db_pending_count = await db.tracked_tickers.count_documents(
            {**STEP3_QUERY, "needs_fundamentals_refresh": True}
        )
        step3_input_total = db_total_count
        step3_output_total = await db.tracked_tickers.count_documents({
            **STEP3_QUERY,
            "fundamentals_status": "complete",
            "needs_fundamentals_refresh": {"$ne": True},
            "fundamentals_updated_at": {"$nin": [None, ""], "$exists": True},
        })
        step3_filtered_out_total = max(step3_input_total - step3_output_total, 0)

    return {
        "job_name":               job_name,
        "last_run":               run,
        "cancel_requested":       cancel_requested,
        "db_complete_count":      db_complete_count,
        "db_pending_count":       db_pending_count,
        "db_total_count":         db_total_count,
        "step3_input_total":      step3_input_total,
        "step3_output_total":     step3_output_total,
        "step3_filtered_out_total": step3_filtered_out_total,
    }


@api_router.get("/admin/step3/telemetry")
async def admin_step3_telemetry():
    """Live Step 3 telemetry (fundamentals_sync) for admin monitoring."""
    from services.admin_overview_service import get_step3_live_telemetry
    return await get_step3_live_telemetry(db)


# =============================================================================
# P25: PAIN CACHE COMPUTATION
# =============================================================================
# Computes exact PAIN (max drawdown) details from full daily series for all
# visible tickers. Results are cached in ticker_pain_cache for instant frontend access.
# =============================================================================

async def compute_pain_cache_for_ticker(ticker: str) -> dict:
    """
    Compute PAIN details for a single ticker from full daily price series.
    
    Args:
        ticker: Full ticker symbol (e.g., "NVDA.US")
    
    Returns:
        dict with PAIN details or None if no data
    """
    # Get ALL price data for ticker (full series, not downsampled)
    prices = await db.stock_prices.find(
        {"ticker": ticker},
        {"_id": 0, "date": 1, "adjusted_close": 1, "close": 1}
    ).sort("date", 1).to_list(length=None)  # No limit - full series
    
    if not prices or len(prices) < 2:
        return None
    
    # Calculate PAIN using full series
    pain_data = calculate_pain_details(prices)
    
    return {
        "ticker": ticker,
        "pain_pct": pain_data["pain_pct"],
        "pain_percentage": pain_data["pain_percentage"],  # P26 ADDENDUM: negative value for UI
        "pain_peak_date": pain_data["pain_peak_date"],
        "pain_trough_date": pain_data["pain_trough_date"],
        "pain_duration_days": pain_data["pain_duration_days"],
        "pain_recovery_date": pain_data["pain_recovery_date"],
        "is_recovered": pain_data["is_recovered"],
        "data_points_used": len(prices),
        "cached_at": datetime.now(timezone.utc)
    }


async def run_pain_cache_refresh(database, batch_size: int = 100) -> dict:
    """
    P25: Refresh PAIN cache for all visible tickers.
    
    This job:
    1. Gets all is_visible=true tickers
    2. Computes PAIN from full daily series (all data points)
    3. Stores exact dates in ticker_pain_cache
    4. Logs to ops_job_runs
    """
    job_id = str(uuid.uuid4())[:8]
    started_at = datetime.now(timezone.utc)
    
    logger.info(f"[PAIN_CACHE:{job_id}] Starting PAIN cache refresh")
    
    # Get all visible tickers
    visible_tickers = await database.tracked_tickers.find(
        VISIBLE_UNIVERSE_QUERY,
        {"_id": 0, "ticker": 1}
    ).to_list(length=None)
    
    total_tickers = len(visible_tickers)
    logger.info(f"[PAIN_CACHE:{job_id}] Found {total_tickers} visible tickers")
    
    processed = 0
    success = 0
    errors = 0
    error_list = []
    
    for ticker_doc in visible_tickers:
        ticker = ticker_doc["ticker"]
        try:
            pain_data = await compute_pain_cache_for_ticker(ticker)
            
            if pain_data:
                # Upsert to cache
                await database.ticker_pain_cache.update_one(
                    {"ticker": ticker},
                    {"$set": pain_data},
                    upsert=True
                )
                success += 1
            
            processed += 1
            
            # Log progress every 100 tickers
            if processed % 100 == 0:
                logger.info(f"[PAIN_CACHE:{job_id}] Progress: {processed}/{total_tickers}")
                
        except Exception as e:
            errors += 1
            error_list.append({"ticker": ticker, "error": str(e)})
            logger.error(f"[PAIN_CACHE:{job_id}] Error for {ticker}: {e}")
    
    finished_at = datetime.now(timezone.utc)
    duration_sec = (finished_at - started_at).total_seconds()
    
    result = {
        "job_id": job_id,
        "job_type": "pain_cache_refresh",
        "source": "manual",
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": round(duration_sec, 2),
        "total_tickers": total_tickers,
        "processed": processed,
        "success": success,
        "errors": errors,
        "error_list": error_list[:10] if error_list else [],
        "status": "completed" if errors == 0 else "completed_with_errors"
    }
    
    # Log to ops_job_runs
    log_doc = {k: v for k, v in result.items() if k != '_id'}
    await database.ops_job_runs.insert_one(log_doc)
    
    logger.info(f"[PAIN_CACHE:{job_id}] Completed: {success}/{total_tickers} in {duration_sec:.1f}s")
    
    return result


# P26: PAIN endpoints removed - PAIN data now exposed via /v1/ticker/{ticker}/detail
# run_pain_cache_refresh() is called by scheduler at 05:00 Europe/Prague daily


@api_router.get("/admin/ops/job-runs")
async def admin_get_job_runs(
    job_type: str = Query(None, description="Filter by job type"),
    limit: int = Query(20, ge=1, le=100),
    source: str = Query(None, description="Filter by source: scheduler or manual")
):
    """
    Get recent job runs from ops_job_runs.
    
    Args:
        job_type: Optional filter by job type
        limit: Max results to return
        source: Filter by 'scheduler' or 'manual'
    
    Returns:
        List of recent job runs
    """
    query = {}
    if job_type:
        query["job_type"] = {"$regex": job_type, "$options": "i"}
    if source:
        query["source"] = source
    
    cursor = db.ops_job_runs.find(query, {"_id": 0}).sort("started_at", -1).limit(limit)
    runs = await cursor.to_list(length=limit)
    
    # Convert datetime objects to ISO strings
    for run in runs:
        if run.get("started_at"):
            run["started_at"] = run["started_at"].isoformat() if hasattr(run["started_at"], 'isoformat') else str(run["started_at"])
        if run.get("finished_at"):
            run["finished_at"] = run["finished_at"].isoformat() if hasattr(run["finished_at"], 'isoformat') else str(run["finished_at"])
        if run.get("created_at"):
            run["created_at"] = run["created_at"].isoformat() if hasattr(run["created_at"], 'isoformat') else str(run["created_at"])
    
    return {
        "count": len(runs),
        "runs": runs
    }

# =========================================================================
# P47: Admin Overview - Single aggregated endpoint for fast Admin Panel
# =========================================================================

from services.admin_overview_service import get_admin_overview

@api_router.get("/admin/overview")
async def admin_overview():
    """
    P47: Single aggregated endpoint for Admin Panel v2.
    Returns all data needed in one response for fast page load (<3s).
    READ-ONLY: no finalization side-effects (page refresh must never mutate).
    """
    return await get_admin_overview(db)


# =========================================================================
# FUNDAMENTALS WHITELIST AUDIT ENDPOINT (BINDING)
# =========================================================================
@api_router.get("/admin/fundamentals-whitelist-audit")
async def fundamentals_whitelist_audit():
    """
    BINDING: Audit fundamentals whitelist compliance.
    
    Returns:
    - forbidden_keys_count: MUST always be 0
    - whitelist_version: Current approved version
    - sample_violations: First 10 violating tickers (if any)
    - compliance_status: PASS/FAIL
    
    Any non-zero forbidden_keys_count is a CRITICAL data integrity breach.
    """
    from whitelist_mapper import WHITELIST_VERSION, WHITELIST_STATUS, FORBIDDEN_SECTIONS
    
    # Top-level forbidden keys to check
    FORBIDDEN_TOP_LEVEL_KEYS = {
        "Highlights", "Valuation", "Technicals", 
        "ETF_Data", "MutualFund_Data", "AnalystRatings", "ESGScores"
    }
    
    # Build query for any forbidden key
    forbidden_conditions = []
    for key in FORBIDDEN_TOP_LEVEL_KEYS:
        forbidden_conditions.append({f"fundamentals.{key}": {"$exists": True}})
    
    # Count violations
    violations_count = 0
    if forbidden_conditions:
        violations_count = await db.tracked_tickers.count_documents({
            "$or": forbidden_conditions
        })
    
    # Get sample violations if any
    sample_violations = []
    if violations_count > 0:
        cursor = db.tracked_tickers.find(
            {"$or": forbidden_conditions},
            {"ticker": 1, "fundamentals": 1, "_id": 0}
        ).limit(10)
        
        async for doc in cursor:
            ticker = doc.get("ticker", "unknown")
            fund = doc.get("fundamentals", {})
            found_keys = [k for k in fund.keys() if k in FORBIDDEN_TOP_LEVEL_KEYS]
            sample_violations.append({
                "ticker": ticker,
                "forbidden_keys": found_keys
            })
    
    # Total tickers with fundamentals
    total_with_fundamentals = await db.tracked_tickers.count_documents({
        "fundamentals": {"$exists": True, "$ne": None}
    })
    
    # Compliance status
    compliance_status = "PASS" if violations_count == 0 else "FAIL"
    
    return {
        "audit_type": "fundamentals_whitelist",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "whitelist_version": WHITELIST_VERSION,
        "whitelist_status": WHITELIST_STATUS,
        "forbidden_keys_checked": sorted(list(FORBIDDEN_TOP_LEVEL_KEYS)),
        "total_tickers_with_fundamentals": total_with_fundamentals,
        "forbidden_keys_count": violations_count,
        "compliance_status": compliance_status,
        "sample_violations": sample_violations,
        "message": "All fundamentals comply with whitelist" if violations_count == 0 
                   else f"CRITICAL: {violations_count} tickers have forbidden keys!"
    }


# =========================================================================
# PROVIDER DEBUG SNAPSHOT (STRICT DEBUG/PRODUCTION SEPARATION)
# =========================================================================
@api_router.get("/admin/provider-debug-snapshot/{ticker}")
async def admin_provider_debug_snapshot(ticker: str):
    """
    Return provider_debug_snapshot for a ticker.
    Debug-only collection; runtime APIs must not consume it.
    """
    ticker_full = ticker.upper()
    if not ticker_full.endswith(".US"):
        ticker_full = f"{ticker_full}.US"

    snapshot = await db.provider_debug_snapshot.find_one(
        {"ticker": ticker_full},
        {"_id": 0},
    )
    if not snapshot:
        raise HTTPException(status_code=404, detail=f"No provider debug snapshot for {ticker_full}")
    return snapshot


@api_router.get("/admin/provider-debug-snapshot")
async def admin_provider_debug_snapshot_list(limit: int = Query(50, ge=1, le=500)):
    """
    List latest provider_debug_snapshot records.
    """
    cursor = (
        db.provider_debug_snapshot.find({}, {"_id": 0})
        .sort("last_captured_at", -1)
        .limit(limit)
    )
    rows = await cursor.to_list(length=limit)
    return {"count": len(rows), "rows": rows}


# =========================================================================
# VISIBILITY AUDIT ENDPOINT (DATA SUPREMACY MANIFESTO v1.0)
# =========================================================================
@api_router.get("/admin/visibility-audit")
async def get_visibility_audit():
    """
    Returns COMPLETE visibility audit data for Admin Panel.
    Shows exact API calls, exclude patterns, and funnel breakdown.
    """
    from visibility_rules import get_canonical_sieve_query, VisibilityFailedReason, VISIBLE_TICKERS_QUERY
    
    # =================================================================
    # API CALLS DOCUMENTATION
    # =================================================================
    api_calls = {
        "seed_nyse": {
            "url": "https://eodhd.com/api/exchange-symbol-list/NYSE?api_token=XXX&fmt=json",
            "description": "NYSE symbol list",
            "filter": "Type == 'Common Stock'"
        },
        "seed_nasdaq": {
            "url": "https://eodhd.com/api/exchange-symbol-list/NASDAQ?api_token=XXX&fmt=json",
            "description": "NASDAQ symbol list",
            "filter": "Type == 'Common Stock'"
        },
        "fundamentals": {
            "url": "https://eodhd.com/api/fundamentals/{TICKER}.US?api_token=XXX",
            "description": "Company fundamentals (sector, industry, officers, statements)"
        },
        "daily_prices": {
            "url": "https://eodhd.com/api/eod-bulk-last-day/US?api_token=XXX&fmt=json",
            "description": "Daily bulk prices for all US tickers"
        },
        "splits": {
            "url": "https://eodhd.com/api/splits/{TICKER}.US?api_token=XXX&from=YYYY-MM-DD",
            "description": "Stock splits history"
        },
        "dividends": {
            "url": "https://eodhd.com/api/div/{TICKER}.US?api_token=XXX&from=YYYY-MM-DD",
            "description": "Dividend history"
        }
    }
    
    # =================================================================
    # EXCLUDE PATTERNS
    # =================================================================
    exclude_patterns = {
        "WARRANTS": ["-WT", "-WS", "-WI"],
        "UNITS": ["-U", "-UN"],
        "PREFERRED": ["-P-", "-PA", "-PB", "-PC", "-PD", "-PE", "-PF", "-PG", "-PH", "-PI", "-PJ"],
        "RIGHTS": ["-R", "-RI"]
    }
    
    # =================================================================
    # DATABASE COUNTS
    # =================================================================
    
    # Exchange breakdown
    nyse_count = await db.tracked_tickers.count_documents({"exchange": "NYSE"})
    nasdaq_count = await db.tracked_tickers.count_documents({"exchange": "NASDAQ"})
    nyse_mkt_count = await db.tracked_tickers.count_documents({"exchange": "NYSE MKT"})
    nyse_arca_count = await db.tracked_tickers.count_documents({"exchange": "NYSE ARCA"})
    
    # Funnel steps
    step1_nyse_nasdaq = await db.tracked_tickers.count_documents({
        "exchange": {"$in": ["NYSE", "NASDAQ"]}
    })
    
    step2_common_stock = await db.tracked_tickers.count_documents({
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock"
    })
    
    step3_has_price = await db.tracked_tickers.count_documents({
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
        "has_price_data": True
    })
    
    step4_has_classification = await db.tracked_tickers.count_documents({
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
        "has_price_data": True,
        "sector": {"$nin": [None, ""]},
        "industry": {"$nin": [None, ""]}
    })
    
    step5_not_delisted = await db.tracked_tickers.count_documents(get_canonical_sieve_query())
    
    step6_visible = await db.tracked_tickers.count_documents(VISIBLE_TICKERS_QUERY)
    
    # Failed reasons breakdown
    failed_reasons = {}
    for reason in VisibilityFailedReason:
        count = await db.tracked_tickers.count_documents({
            "visibility_failed_reason": reason.value
        })
        if count > 0:
            failed_reasons[reason.value] = count
    
    # Sample tickers per reason
    samples = {}
    for reason in VisibilityFailedReason:
        sample_docs = await db.tracked_tickers.find(
            {"visibility_failed_reason": reason.value},
            {"ticker": 1, "name": 1, "_id": 0}
        ).limit(5).to_list(5)
        if sample_docs:
            samples[reason.value] = [{"ticker": d["ticker"], "name": d.get("name", "")} for d in sample_docs]
    
    # Last audit status
    last_audit = await db.ops_audit_runs.find_one({"audit_type": "visibility_guard"})
    
    return {
        "api_calls": api_calls,
        "exclude_patterns": exclude_patterns,
        "exchange_breakdown": {
            "NYSE": nyse_count,
            "NASDAQ": nasdaq_count,
            "NYSE_MKT": nyse_mkt_count,
            "NYSE_ARCA": nyse_arca_count,
            "NYSE_MKT_note": "EXCLUDED from sieve (American Stock Exchange)",
            "NYSE_ARCA_note": "EXCLUDED from sieve (Electronic exchange)"
        },
        "funnel": [
            {
                "step": 1,
                "name": "NYSE + NASDAQ (exchange filter)",
                "query": "exchange IN ['NYSE', 'NASDAQ']",
                "count": step1_nyse_nasdaq,
                "lost": 0,
                "lost_reason": None
            },
            {
                "step": 2,
                "name": "Common Stock (type filter)",
                "query": "+ asset_type == 'Common Stock'",
                "count": step2_common_stock,
                "lost": step1_nyse_nasdaq - step2_common_stock,
                "lost_reason": "Not Common Stock (ETF, Fund, etc.)"
            },
            {
                "step": 3,
                "name": "Has Price Data (activity filter)",
                "query": "+ has_price_data == true",
                "count": step3_has_price,
                "lost": step2_common_stock - step3_has_price,
                "lost_reason": "No price data in daily bulk"
            },
            {
                "step": 4,
                "name": "Has Classification (quality filter)",
                "query": "+ sector AND industry present",
                "count": step4_has_classification,
                "lost": step3_has_price - step4_has_classification,
                "lost_reason": "Missing sector or industry"
            },
            {
                "step": 5,
                "name": "Not Delisted (status filter)",
                "query": "+ is_delisted != true",
                "count": step5_not_delisted,
                "lost": step4_has_classification - step5_not_delisted,
                "lost_reason": "Delisted"
            },
            {
                "step": 6,
                "name": "VISIBLE (final)",
                "query": "is_visible == true",
                "count": step6_visible,
                "lost": step5_not_delisted - step6_visible,
                "lost_reason": "Canonical sieve mismatch (run cleanup)"
            }
        ],
        "failed_reasons": failed_reasons,
        "samples": samples,
        "canonical_sieve_count": step5_not_delisted,
        "is_visible_count": step6_visible,
        "mismatch": abs(step5_not_delisted - step6_visible),
        "last_audit": {
            "status": last_audit.get("status") if last_audit else "NEVER_RUN",
            "timestamp": last_audit.get("timestamp").isoformat() if last_audit and last_audit.get("timestamp") else None,
        }
    }


@api_router.get("/admin/health-report")
async def admin_health_report():
    """
    Comprehensive health report for Admin Panel.
    Uses VISIBLE UNIVERSE RULE: is_visible=true only.
    OPTIMIZED: Uses aggregation instead of distinct for large collections.
    """
    from datetime import datetime, timezone, timedelta
    import asyncio
    
    # VISIBLE UNIVERSE QUERY (PERMANENT)
    visible_filter = VISIBLE_UNIVERSE_QUERY
    
    # Run fast queries in parallel
    async def count_distinct_tickers(collection, field="ticker"):
        """Count distinct tickers using aggregation (faster than distinct)."""
        pipeline = [{"$group": {"_id": f"${field}"}}, {"$count": "count"}]
        result = await collection.aggregate(pipeline).to_list(1)
        return result[0]["count"] if result else 0
    
    # Batch 1: Simple counts (fast)
    total_tickers_task = db.tracked_tickers.count_documents({})
    visible_tickers_task = db.tracked_tickers.count_documents(visible_filter)
    with_fundamentals_task = db.tracked_tickers.count_documents({**visible_filter, "status": "active"})
    pending_fundamentals_task = db.tracked_tickers.count_documents({**visible_filter, "status": "pending_fundamentals"})
    fundamentals_count_task = db.company_fundamentals_cache.count_documents({})
    insiders_tickers_task = db.insider_activity_cache.count_documents({})
    total_price_records_task = db.stock_prices.count_documents({})
    
    # Batch 2: Distinct counts (slower - use aggregation)
    financials_tickers_task = count_distinct_tickers(db.financials_cache)
    earnings_tickers_task = count_distinct_tickers(db.earnings_history_cache)
    unique_price_tickers_task = count_distinct_tickers(db.stock_prices)
    
    # Execute batch 1 & 2 in parallel
    (total_tickers, visible_tickers, with_fundamentals, pending_fundamentals,
     fundamentals_count, insiders_tickers, total_price_records,
     financials_tickers, earnings_tickers, unique_price_tickers) = await asyncio.gather(
        total_tickers_task, visible_tickers_task, with_fundamentals_task, pending_fundamentals_task,
        fundamentals_count_task, insiders_tickers_task, total_price_records_task,
        financials_tickers_task, earnings_tickers_task, unique_price_tickers_task
    )
    
    # Batch 3: Other queries
    five_days_ago = datetime.now(timezone.utc) - timedelta(days=5)
    recent_price_pipeline = [
        {"$match": {"date": {"$gte": five_days_ago.strftime("%Y-%m-%d")}}},
        {"$group": {"_id": "$ticker"}},
        {"$count": "count"}
    ]
    recent_price_result = await db.stock_prices.aggregate(recent_price_pipeline).to_list(length=1)
    tickers_with_recent_prices = recent_price_result[0]["count"] if recent_price_result else 0
    
    # Latest price date
    latest_price = await db.stock_prices.find_one({}, {"date": 1}, sort=[("date", -1)])
    latest_price_date = latest_price.get("date") if latest_price else None
    
    # Last scheduler runs - parallel
    last_price_sync_task = db.ops_job_runs.find_one(
        {"job_type": {"$in": ["daily_price_sync", "scheduled_price_sync"]}},
        {"_id": 0}, sort=[("started_at", -1)]
    )
    last_fundamentals_sync_task = db.ops_job_runs.find_one(
        {"job_type": "scheduled_fundamentals_sync"},
        {"_id": 0}, sort=[("started_at", -1)]
    )
    last_backfill_task = db.ops_job_runs.find_one(
        {"job_type": "scheduled_price_backfill"},
        {"_id": 0}, sort=[("started_at", -1)]
    )
    scheduler_config_task = db.ops_config.find_one({"key": "scheduler_enabled"}, {"_id": 0})
    
    last_price_sync, last_fundamentals_sync, last_backfill, scheduler_config = await asyncio.gather(
        last_price_sync_task, last_fundamentals_sync_task, last_backfill_task, scheduler_config_task
    )
    
    scheduler_enabled = scheduler_config.get("value", True) if scheduler_config else True
    
    # Data gaps - simplified count (skip top 20 aggregation for speed)
    gaps_by_field = {}
    gap_fields = ["fundamentals", "price", "financials", "earnings", "benchmark"]
    for field in gap_fields:
        count = await db.data_gaps.count_documents({f"missing_{field}": True, "resolved": {"$ne": True}})
        gaps_by_field[field] = count
    
    def format_job_run(run):
        if not run:
            return None
        return {
            "status": run.get("status"),
            "started_at": run.get("started_at").isoformat() if run.get("started_at") and hasattr(run.get("started_at"), 'isoformat') else str(run.get("started_at")),
            "records": run.get("records_upserted") or run.get("details", {}).get("processed"),
            "api_calls": run.get("api_calls") or run.get("details", {}).get("api_calls_used"),
        }
    
    # Pipeline anchor run = Step 1 Universe Seed (23:00 Mon-Sat, Prague).
    PRAGUE = ZoneInfo("Europe/Prague")
    now_prague = datetime.now(PRAGUE)
    next_run_prague = now_prague.replace(hour=23, minute=0, second=0, microsecond=0)
    if now_prague >= next_run_prague:
        next_run_prague += timedelta(days=1)
    # Sunday (weekday=6) has no universe seed run.
    while next_run_prague.weekday() == 6:
        next_run_prague += timedelta(days=1)
    next_run_utc = next_run_prague.astimezone(timezone.utc)
    
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scheduler": {
            "enabled": scheduler_enabled,
            "schedule": "Mon-Sat @ 23:00 (Step 1), Step 2/3 auto by dependency",
            "next_run": next_run_utc.isoformat(),
            "next_run_prague": next_run_prague.strftime("%Y-%m-%d %H:%M Prague"),
        },
        "last_runs": {
            "price_sync": format_job_run(last_price_sync),
            "fundamentals_sync": format_job_run(last_fundamentals_sync),
            "price_backfill": format_job_run(last_backfill),
        },
        "tickers": {
            "total": total_tickers,
            "visible": visible_tickers,
            "with_fundamentals": with_fundamentals,
            "pending_fundamentals": pending_fundamentals,
            "fundamentals_percent": round((with_fundamentals / visible_tickers) * 100, 1) if visible_tickers > 0 else 0,
            "active": with_fundamentals,
            "pending": pending_fundamentals,
            "active_percent": round((with_fundamentals / visible_tickers) * 100, 1) if visible_tickers > 0 else 0,
        },
        "coverage": {
            "fundamentals": {
                "count": with_fundamentals,
                "percent": round((with_fundamentals / visible_tickers) * 100, 1) if visible_tickers > 0 else 0,
            },
            "financials": {
                "count": financials_tickers,
                "percent": round((financials_tickers / visible_tickers) * 100, 1) if visible_tickers > 0 else 0,
            },
            "earnings": {
                "count": earnings_tickers,
                "percent": round((earnings_tickers / visible_tickers) * 100, 1) if visible_tickers > 0 else 0,
            },
            "insiders": {
                "count": insiders_tickers,
                "percent": round((insiders_tickers / visible_tickers) * 100, 1) if visible_tickers > 0 else 0,
            },
            "prices": {
                "total_records": total_price_records,
                "unique_tickers": unique_price_tickers,
                "tickers_with_recent": tickers_with_recent_prices,
                "latest_date": latest_price_date,
            },
        },
        "data_gaps": {
            "by_field": gaps_by_field,
            "top_20_tickers": [],
        },
    }


@api_router.get("/admin/audit/missing-sector")
async def admin_audit_missing_sector(
    limit: int = Query(20, ge=1, le=100),
):
    """
    Audit: Find tickers in visible universe that are missing sector/industry.
    
    If any are found, it's a BUG in the data ingestion pipeline.
    
    Uses VISIBLE_UNIVERSE_QUERY (is_visible=true) as the only filter.
    """
    # Universe filter - SINGLE SOURCE OF TRUTH
    universe_filter = VISIBLE_UNIVERSE_QUERY
    
    # Missing sector query
    missing_sector_query = {
        **universe_filter,
        "$or": [
            {"sector": None},
            {"sector": ""},
            {"sector": {"$exists": False}}
        ]
    }
    
    # Missing industry query
    missing_industry_query = {
        **universe_filter,
        "$or": [
            {"industry": None},
            {"industry": ""},
            {"industry": {"$exists": False}}
        ]
    }
    
    # Counts
    universe_count = await db.tracked_tickers.count_documents(universe_filter)
    missing_sector_count = await db.tracked_tickers.count_documents(missing_sector_query)
    missing_industry_count = await db.tracked_tickers.count_documents(missing_industry_query)
    
    # Sample tickers missing sector (sorted by market_cap desc)
    missing_sector_sample = []
    cursor = db.tracked_tickers.find(
        missing_sector_query,
        {"ticker": 1, "name": 1, "exchange": 1, "sector": 1, "industry": 1, "market_cap": 1, "fundamentals_status": 1, "status": 1, "_id": 0}
    ).sort("market_cap", -1).limit(limit)
    
    async for doc in cursor:
        missing_sector_sample.append(doc)
    
    # Sample tickers missing industry
    missing_industry_sample = []
    cursor = db.tracked_tickers.find(
        missing_industry_query,
        {"ticker": 1, "name": 1, "exchange": 1, "sector": 1, "industry": 1, "market_cap": 1, "fundamentals_status": 1, "status": 1, "_id": 0}
    ).sort("market_cap", -1).limit(limit)
    
    async for doc in cursor:
        missing_industry_sample.append(doc)
    
    # Status
    is_ok = missing_sector_count == 0 and missing_industry_count == 0
    
    return {
        "status": "OK" if is_ok else "BUG - Missing data in universe",
        "universe_count": universe_count,
        "missing_sector": {
            "count": missing_sector_count,
            "percent": round(100 * missing_sector_count / universe_count, 2) if universe_count > 0 else 0,
            "sample": missing_sector_sample,
        },
        "missing_industry": {
            "count": missing_industry_count,
            "percent": round(100 * missing_industry_count / universe_count, 2) if universe_count > 0 else 0,
            "sample": missing_industry_sample,
        },
        "action": None if is_ok else "Fix data ingestion - these tickers should have sector/industry from fundamentals sync",
    }


@api_router.get("/admin/audit/missing-sector/export")
async def admin_audit_missing_sector_export():
    """
    Export ALL tickers missing sector/industry as JSON array.
    No limit - returns full list for CSV export.
    """
    # Universe filter
    # Universe filter - SINGLE SOURCE OF TRUTH
    universe_filter = VISIBLE_UNIVERSE_QUERY
    
    # Missing sector query
    missing_query = {
        **universe_filter,
        "$or": [
            {"sector": None},
            {"sector": ""},
            {"sector": {"$exists": False}}
        ]
    }
    
    # Get ALL missing
    all_missing = []
    cursor = db.tracked_tickers.find(
        missing_query,
        {"ticker": 1, "name": 1, "exchange": 1, "sector": 1, "industry": 1, "asset_type": 1, "type": 1, "status": 1, "_id": 0}
    ).sort("ticker", 1)
    
    async for doc in cursor:
        all_missing.append(doc)
    
    return {
        "count": len(all_missing),
        "tickers": all_missing,
    }


@api_router.get("/admin/audit/missing-sector/csv")
async def admin_audit_missing_sector_csv():
    """
    Export ALL tickers missing sector/industry as CSV file download.
    """
    from fastapi.responses import StreamingResponse
    import io
    import csv
    
    # Universe filter
    universe_filter = {
        "$or": [
            {"is_whitelisted": True},
            {"is_whitelisted": {"$exists": False}},
        ],
        **VISIBLE_UNIVERSE_QUERY,
    }
    
    # Missing sector query
    missing_query = {
        **universe_filter,
        "$or": [
            {"sector": None},
            {"sector": ""},
            {"sector": {"$exists": False}}
        ]
    }
    
    # Build CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['ticker', 'exchange', 'name', 'asset_type', 'type', 'sector', 'industry', 'status'])
    
    cursor = db.tracked_tickers.find(
        missing_query,
        {"ticker": 1, "name": 1, "exchange": 1, "sector": 1, "industry": 1, "asset_type": 1, "type": 1, "status": 1, "_id": 0}
    ).sort("ticker", 1)
    
    async for doc in cursor:
        writer.writerow([
            doc.get('ticker', ''),
            doc.get('exchange', ''),
            doc.get('name', ''),
            doc.get('asset_type', ''),
            doc.get('type', ''),
            doc.get('sector', ''),
            doc.get('industry', ''),
            doc.get('status', ''),
        ])
    
    output.seek(0)
    
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=missing_sector_audit.csv"}
    )


@api_router.get("/admin/download/master-verification-csv")
async def admin_download_master_verification_csv():
    """
    Download the master verification table CSV.
    """
    from fastapi.responses import FileResponse
    import os
    
    csv_path = "/app/backend/scripts/master_verification_table.csv"
    if not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail="CSV file not found. Run audit first.")
    
    return FileResponse(
        csv_path,
        media_type="text/csv",
        filename="master_verification_table.csv"
    )


@api_router.get("/admin/download/master-verification-summary")
async def admin_download_master_verification_summary():
    """
    Download the master verification summary JSON.
    """
    from fastapi.responses import FileResponse
    import os
    
    json_path = "/app/backend/scripts/master_verification_summary.json"
    if not os.path.exists(json_path):
        raise HTTPException(status_code=404, detail="JSON file not found. Run audit first.")
    
    return FileResponse(
        json_path,
        media_type="application/json",
        filename="master_verification_summary.json"
    )


@api_router.post("/admin/backfill/fundamentals")
async def admin_run_fundamentals_backfill(
    limit: int = Query(100, ge=1, le=1000),
    dry_run: bool = Query(False),
):
    """
    Run fundamentals backfill for universe tickers missing sector/industry.
    
    This is the ONLY endpoint to trigger fundamentals backfill.
    Rate limited to 5 req/sec.
    
    Args:
        limit: Max tickers to process (default 100, max 1000)
        dry_run: If True, don't write to DB
    """
    import httpx
    
    EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")
    if not EODHD_API_KEY:
        raise HTTPException(status_code=500, detail="EODHD_API_KEY not configured")
    
    # Universe query - SINGLE SOURCE OF TRUTH
    universe_filter = {
        "$or": [
            {"is_whitelisted": True},
            {"is_whitelisted": {"$exists": False}},
        ],
        **VISIBLE_UNIVERSE_QUERY,
    }
    
    # Find tickers missing sector
    missing_query = {
        **universe_filter,
        "$or": [
            {"sector": None},
            {"sector": ""},
            {"sector": {"$exists": False}}
        ]
    }
    
    # Get tickers
    cursor = db.tracked_tickers.find(
        missing_query,
        {"ticker": 1, "_id": 0}
    ).sort("ticker", 1).limit(limit)
    
    tickers = [doc["ticker"] for doc in await cursor.to_list(length=limit)]
    
    if not tickers:
        return {"status": "nothing_to_do", "message": "No tickers missing sector/industry"}
    
    stats = {
        "total": len(tickers),
        "processed": 0,
        "updated": 0,
        "not_found": 0,
        "failed": 0,
        "dry_run": dry_run,
    }
    
    async with httpx.AsyncClient() as http_client:
        for i, ticker in enumerate(tickers):
            # Rate limiting
            if i > 0 and i % 5 == 0:
                await asyncio.sleep(1)
            
            try:
                # Ensure .US suffix
                ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
                
                url = f"https://eodhd.com/api/fundamentals/{ticker_full}"
                params = {"api_token": EODHD_API_KEY, "fmt": "json"}
                
                response = await http_client.get(url, params=params, timeout=30)
                stats["processed"] += 1
                
                if response.status_code == 200:
                    data = response.json()
                    general = data.get("General", {})
                    
                    sector = general.get("Sector", "")
                    industry = general.get("Industry", "")
                    
                    if sector and not dry_run:
                        await db.tracked_tickers.update_one(
                            {"ticker": ticker},
                            {"$set": {
                                "sector": sector,
                                "industry": industry,
                                "logo_url": general.get("LogoURL", ""),
                                "fundamentals_status": "ok",
                                "updated_at": datetime.now(timezone.utc),
                            }}
                        )
                        stats["updated"] += 1
                    elif not sector:
                        stats["not_found"] += 1
                        
                elif response.status_code == 404:
                    stats["not_found"] += 1
                else:
                    stats["failed"] += 1
                    
            except Exception as e:
                stats["failed"] += 1
                logger.error(f"Error fetching {ticker}: {e}")
    
    return {
        "status": "completed",
        "stats": stats,
    }


@api_router.post("/admin/backfill-fundamentals-complete")
async def admin_run_complete_fundamentals_backfill(
    background_tasks: BackgroundTasks,
    limit: int = Query(None, ge=1, description="Limit tickers (None = all)"),
    dry_run: bool = Query(False),
    start_from: str = Query(None, description="Start from ticker (for resuming)"),
):
    """
    LEGACY endpoint is intentionally disabled.

    Reason:
    - Strict RAW FACTS ONLY production policy.
    - Provider-computed sections must not be stored in production flows.
    - Use /api/admin/backfill-raw-facts (or fundamentals-refill) instead.
    """
    raise HTTPException(
        status_code=410,
        detail=(
            "Legacy endpoint disabled. Use /api/admin/backfill-raw-facts "
            "or /api/admin/fundamentals-refill."
        ),
    )


@api_router.get("/admin/backfill-fundamentals-complete/verify")
async def admin_verify_fundamentals_backfill():
    """
    LEGACY verification endpoint disabled together with complete backfill.
    """
    raise HTTPException(
        status_code=410,
        detail=(
            "Legacy endpoint disabled. Use /api/admin/backfill-raw-facts/verify "
            "or /api/admin/fundamentals-refill/verify."
        ),
    )


# =============================================================================
# FUNDAMENTALS REFILL (LOCKED WHITELIST)
# =============================================================================
# BINDING: Uses whitelist_mapper.py with Richard-approved fields ONLY.
# Status: LOCKED. No changes without explicit Richard approval (2026-02-25).
# =============================================================================

@api_router.get("/admin/fundamentals-whitelist")
async def admin_get_fundamentals_whitelist():
    """
    Get the LOCKED fundamentals whitelist document.
    
    This document is IMMUTABLE and cannot be edited or deleted.
    Any changes require explicit Richard approval.
    """
    from whitelist_mapper import get_whitelist_document
    return get_whitelist_document()



# =============================================================================
# VISIBLE UNIVERSE ADMIN ENDPOINTS
# =============================================================================

@api_router.get("/admin/pipeline/exclusion-report")
async def admin_get_pipeline_exclusion_report(
    report_date: str = Query(None, description="Report date in YYYY-MM-DD (defaults to latest available date)"),
    step: str = Query(None, description="Optional step filter, e.g. 'Step 1 - Universe Seed'"),
    run_id: str = Query(None, description="Filter strictly to a specific run_id (overrides report_date aggregation)"),
    limit: int = Query(100, ge=0, le=2000),
    offset: int = Query(0, ge=0),
):
    """
    Get pipeline exclusion report rows (Ticker | Name | Step | Reason).
    Shared report collection across all pipeline steps.

    ?run_id=<run_id> filters all counts and rows strictly to that run_id,
    enabling per-run verification independent of report_date collisions.
    """
    from zoneinfo import ZoneInfo
    PRAGUE = ZoneInfo("Europe/Prague")

    if not report_date:
        latest = await db.pipeline_exclusion_report.find_one(
            {},
            {"_id": 0, "report_date": 1},
            sort=[("report_date", -1), ("created_at", -1)],
        )
        report_date = (
            latest.get("report_date")
            if latest and latest.get("report_date")
            else datetime.now(PRAGUE).strftime("%Y-%m-%d")
        )

    # Base query — run_id filter takes precedence over report_date if provided.
    if run_id:
        query: dict = {"run_id": run_id}
    else:
        query = {"report_date": report_date}
    if step:
        query["step"] = step

    total_rows = await db.pipeline_exclusion_report.count_documents(query)
    cursor = db.pipeline_exclusion_report.find(
        query,
        {"_id": 0}
    ).sort([("created_at", -1), ("step", 1), ("ticker", 1)]).skip(offset).limit(limit)
    rows = await cursor.to_list(length=limit)

    by_step: Dict[str, int] = {}
    by_reason: Dict[str, int] = {}

    async for doc in db.pipeline_exclusion_report.aggregate([
        {"$match": query},
        {"$group": {"_id": "$step", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]):
        by_step[doc["_id"]] = doc["count"]

    async for doc in db.pipeline_exclusion_report.aggregate([
        {"$match": query},
        {"$group": {"_id": "$reason", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]):
        by_reason[doc["_id"]] = doc["count"]

    # Latest run_id per step for the resolved report_date — for per-run verification.
    latest_run_id_per_step: Dict[str, str] = {}
    async for doc in db.pipeline_exclusion_report.aggregate([
        {"$match": {"report_date": report_date}},
        {"$sort": {"created_at": -1}},
        {"$group": {"_id": "$step", "run_id": {"$first": "$run_id"}}},
    ]):
        if doc["_id"] and doc.get("run_id"):
            latest_run_id_per_step[doc["_id"]] = doc["run_id"]

    available_dates = await db.pipeline_exclusion_report.distinct("report_date")
    available_dates = sorted([d for d in available_dates if d], reverse=True)[:30]
    latest_universe_seed_run = await db.ops_job_runs.find_one(
        {"job_name": "universe_seed"},
        {"_id": 0, "started_at": 1, "status": 1, "triggered_by": 1},
        sort=[("started_at", -1)],
    )
    latest_universe_seed_started_at = (
        latest_universe_seed_run.get("started_at").isoformat()
        if latest_universe_seed_run and latest_universe_seed_run.get("started_at")
        else None
    )
    empty_report_hint = None
    if total_rows == 0:
        empty_report_hint = (
            "No filtered-out rows are stored for this date. "
            "Run Step 1 (Universe Seed) again to generate the exclusion report."
        )

    # When run_id_filter is a Step 1 run_id, enrich response with reconciliation
    # debug from the matching ops_job_runs document.
    step1_reconciliation = None
    step1_counts = None
    if run_id and run_id.startswith("universe_seed_"):
        seed_run_doc = await db.ops_job_runs.find_one(
            {"job_name": "universe_seed", "$or": [
                {"result.exclusion_report_run_id": run_id},
                {"details.exclusion_report_run_id": run_id},
            ]},
            {"_id": 0, "result": 1, "details": 1},
        )
        if seed_run_doc:
            r = seed_run_doc.get("result") or seed_run_doc.get("details") or {}
            dbg = r.get("universe_seed_debug") or {}
            step1_reconciliation = dbg.get("reconciliation")
            step1_counts = {
                "raw_distinct": r.get("fetched"),
                "seeded_count": r.get("seeded_total"),
                "filtered_out_total_step1": r.get("filtered_out_total_step1"),
                "fetched_raw_per_exchange": r.get("fetched_raw_per_exchange"),
                "run_id": run_id,
            }

    return {
        "report_date": report_date,
        "run_id_filter": run_id,
        "total_rows": total_rows,
        "has_rows": total_rows > 0,
        "offset": offset,
        "limit": limit,
        "by_step": by_step,
        "by_reason": by_reason,
        "latest_run_id_per_step": latest_run_id_per_step,
        "available_dates": available_dates,
        "latest_universe_seed_started_at": latest_universe_seed_started_at,
        "empty_report_hint": empty_report_hint,
        "step1_reconciliation": step1_reconciliation,
        "step1_counts": step1_counts,
        "rows": rows,
    }


@api_router.get("/admin/pipeline/data-freshness")
async def admin_data_freshness():
    """Return data-freshness metrics for the pipeline dashboard."""
    from services.data_freshness_service import get_data_freshness
    return await get_data_freshness(db)


@api_router.get("/admin/pipeline/fundamentals-health")
async def admin_fundamentals_health():
    """
    Read-only health check for all tickers with fundamentals_status == 'complete'.

    Phase 1 (free):    DB-only counts of missing fields / rows.
    Phase 2 (credits): Sample up to 50 suspect tickers, fetch live from EODHD,
                       count real corruption (provider-has-data but DB-missing).
    Max cost: 50 x 10 = 500 EODHD credits per call.
    """
    import random
    from fundamentals_service import fetch_fundamentals_from_eodhd

    # ── Phase 1: DB-only counts (free) ───────────────────────────────────────

    total_complete = await db.tracked_tickers.count_documents(
        {"fundamentals_status": "complete"}
    )

    db_missing_shares_count = await db.tracked_tickers.count_documents({
        "fundamentals_status": "complete",
        "$or": [
            {"shares_outstanding": None},
            {"shares_outstanding": {"$exists": False}},
        ],
    })

    db_missing_classification_count = await db.tracked_tickers.count_documents({
        "fundamentals_status": "complete",
        "$or": [
            {"sector":   None}, {"sector":   ""},  {"sector":   {"$exists": False}},
            {"industry": None}, {"industry": ""},  {"industry": {"$exists": False}},
        ],
    })

    complete_tickers: list = await db.tracked_tickers.distinct(
        "ticker", {"fundamentals_status": "complete"}
    )
    complete_set = set(complete_tickers)

    tickers_with_financials = set(
        await db.company_financials.distinct(
            "ticker", {"ticker": {"$in": complete_tickers}}
        )
    )
    tickers_with_earnings = set(
        await db.company_earnings_history.distinct(
            "ticker", {"ticker": {"$in": complete_tickers}}
        )
    )

    missing_fin_set  = complete_set - tickers_with_financials
    missing_earn_set = complete_set - tickers_with_earnings

    db_missing_financials_count = len(missing_fin_set)
    db_missing_earnings_count   = len(missing_earn_set)

    # ── Phase 2: Corruption sample (live EODHD calls) ────────────────────────

    SAMPLE_SIZE = 50
    corruption_estimate: Dict[str, Any] = {
        "skipped": "EODHD_API_KEY not configured — MOCK mode"
    }

    if os.getenv("EODHD_API_KEY", ""):
        missing_shares_tickers: list = await db.tracked_tickers.distinct(
            "ticker",
            {
                "fundamentals_status": "complete",
                "$or": [
                    {"shares_outstanding": None},
                    {"shares_outstanding": {"$exists": False}},
                ],
            },
        )
        missing_class_tickers: list = await db.tracked_tickers.distinct(
            "ticker",
            {
                "fundamentals_status": "complete",
                "$or": [
                    {"sector":   None}, {"sector":   ""},
                    {"industry": None}, {"industry": ""},
                ],
            },
        )

        suspect_set = (
            missing_fin_set
            | missing_earn_set
            | set(missing_shares_tickers)
            | set(missing_class_tickers)
        )

        sample = random.sample(sorted(suspect_set), min(SAMPLE_SIZE, len(suspect_set)))

        async def _probe(ticker_full: str) -> Dict[str, Any]:
            ticker_plain = ticker_full.replace(".US", "")
            data = await fetch_fundamentals_from_eodhd(ticker_plain)
            if not data:
                return {"ticker": ticker_full, "provider_responded": False}
            general    = data.get("General")    or {}
            shares     = data.get("SharesStats") or {}
            financials = data.get("Financials")  or {}
            earnings   = data.get("Earnings")    or {}
            sector_raw   = (general.get("Sector")   or "").strip()
            industry_raw = (general.get("Industry") or "").strip()
            # True only if at least one statement period-dict (yearly or quarterly)
            # contains at least one actual period — empty section dicts do not count.
            has_financials = any(
                isinstance((financials.get(stmt) or {}).get(period), dict)
                and bool((financials[stmt])[period])
                for stmt in ("Income_Statement", "Balance_Sheet", "Cash_Flow")
                for period in ("yearly", "quarterly")
                if isinstance(financials.get(stmt), dict)
            )
            return {
                "ticker":                      ticker_full,
                "provider_responded":          True,
                "provider_has_shares":         bool(shares.get("SharesOutstanding")),
                "provider_has_classification": bool(sector_raw and industry_raw),
                "provider_has_financials":     has_financials,
                "provider_has_earnings":       bool(earnings.get("History")),
                "db_missing_financials":       ticker_full in missing_fin_set,
                "db_missing_earnings":         ticker_full in missing_earn_set,
                "db_missing_shares":           ticker_full in set(missing_shares_tickers),
                "db_missing_classification":   ticker_full in set(missing_class_tickers),
            }

        probe_results = await asyncio.gather(*[_probe(t) for t in sample])
        credits_used  = len(sample)  # 10 credits per call regardless of response content

        corruption_sample_financials = [
            r["ticker"] for r in probe_results
            if r.get("provider_responded") and r.get("provider_has_financials") and r.get("db_missing_financials")
        ][:10]

        corruption_estimate = {
            "suspect_tickers_found":  len(suspect_set),
            "sampled":                len(sample),
            "credits_used":           credits_used * 10,
            "provider_has_financials_but_db_missing": sum(
                1 for r in probe_results
                if r.get("provider_responded") and r.get("provider_has_financials") and r.get("db_missing_financials")
            ),
            "provider_has_earnings_but_db_missing": sum(
                1 for r in probe_results
                if r.get("provider_responded") and r.get("provider_has_earnings") and r.get("db_missing_earnings")
            ),
            "provider_has_shares_but_db_missing": sum(
                1 for r in probe_results
                if r.get("provider_responded") and r.get("provider_has_shares") and r.get("db_missing_shares")
            ),
            "provider_has_classification_but_db_missing": sum(
                1 for r in probe_results
                if r.get("provider_responded") and r.get("provider_has_classification") and r.get("db_missing_classification")
            ),
        }

    return {
        "checked_at":                      datetime.now(timezone.utc).isoformat(),
        "total_complete":                  total_complete,
        "db_missing_shares_count":         db_missing_shares_count,
        "db_missing_classification_count": db_missing_classification_count,
        "db_missing_financials_count":     db_missing_financials_count,
        "db_missing_earnings_count":       db_missing_earnings_count,
        "sample_size":                     SAMPLE_SIZE,
        "corruption_estimate":             corruption_estimate,
        "corruption_sample_financials":    corruption_sample_financials,
    }


@api_router.get("/admin/pipeline/funnel-gap")
async def admin_pipeline_funnel_gap(
    report_date: str = Query(None, description="Report date YYYY-MM-DD (defaults to latest)"),
):
    """
    Identify tickers that appear in the classified universe but are
    unaccounted for in the pipeline exclusion report — i.e. the 'ghost' gap
    between classified count and the exclusion-report arithmetic chain.

    Returns:
      - classified_count   : count(classified query)
      - report_step3_count : distinct tickers in pipeline_exclusion_report Step 3 rows
      - visible_count      : count(is_visible=True) in classified universe
      - gap_tickers        : classified tickers NOT in (visible ∪ step3_report)
      - gap_count          : len(gap_tickers)
    """
    from zoneinfo import ZoneInfo as _ZoneInfo
    _PRAGUE = _ZoneInfo("Europe/Prague")

    if not report_date:
        latest = await db.pipeline_exclusion_report.find_one(
            {}, {"_id": 0, "report_date": 1},
            sort=[("report_date", -1), ("created_at", -1)],
        )
        report_date = (
            latest.get("report_date") if latest and latest.get("report_date")
            else datetime.now(_PRAGUE).strftime("%Y-%m-%d")
        )

    _CLASSIFIED_QUERY = {
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
        "has_price_data": True,
        "sector":   {"$nin": [None, ""]},
        "industry": {"$nin": [None, ""]},
    }

    # Sets from tracked_tickers
    classified_tickers: set = set(
        await db.tracked_tickers.distinct("ticker", _CLASSIFIED_QUERY)
    )
    visible_tickers: set = set(
        await db.tracked_tickers.distinct("ticker", {**_CLASSIFIED_QUERY, "is_visible": True})
    )

    # Sets from exclusion report for this date (all Step 3 rows including visibility gates)
    step3_reported: set = set(
        await db.pipeline_exclusion_report.distinct(
            "ticker", {"report_date": report_date, "step": "Step 3 - Fundamentals Sync"}
        )
    )

    accounted = visible_tickers | step3_reported
    gap_tickers = sorted(classified_tickers - accounted)

    # Fetch name for each gap ticker for readability
    gap_docs = []
    if gap_tickers:
        async for doc in db.tracked_tickers.find(
            {"ticker": {"$in": gap_tickers}},
            {"_id": 0, "ticker": 1, "name": 1, "sector": 1, "industry": 1,
             "is_visible": 1, "visibility_failed_reason": 1,
             "fundamentals_status": 1, "shares_outstanding": 1,
             "financial_currency": 1, "is_delisted": 1},
        ):
            gap_docs.append(doc)

    return {
        "report_date": report_date,
        "classified_count": len(classified_tickers),
        "visible_count": len(visible_tickers),
        "report_step3_count": len(step3_reported),
        "accounted_count": len(accounted & classified_tickers),
        "gap_count": len(gap_tickers),
        "gap_tickers": gap_docs,
    }


@api_router.get("/admin/pipeline/export/step/{step_number}")
async def admin_pipeline_export_step(
    step_number: int,
    run_id: str = Query(None, description="Step 1 run_id (default: latest). Only affects step 1."),
):
    """
    Export full ticker list for a pipeline step as CSV.

    Each row: ticker, name, status
      status = "ok"  — ticker is in the OUTPUT of this step
      status = reason — ticker was filtered out at this step

    Step 1: Source = universe_seed_raw_rows (verbatim EODHD rows) for that run_id.
            Seeded set and exclusion reasons are also run-scoped.
            Supports ?run_id= param; defaults to latest run.
    Step 2: input has_price_data tickers + Step 2 exclusion report.
    Step 3: input classified tickers + Step 3 exclusion report (incl. visibility gates).
    """
    from fastapi.responses import StreamingResponse as _SR
    from fastapi import HTTPException as _HTTPException
    import io as _io
    import csv as _csv

    _SEED_QUERY = {"exchange": {"$in": ["NYSE", "NASDAQ"]}, "asset_type": "Common Stock"}
    _STEP3_QUERY = {**_SEED_QUERY, "has_price_data": True}

    if step_number not in (1, 2, 3):
        raise _HTTPException(status_code=400, detail="step_number must be 1, 2, or 3")

    output = _io.StringIO()
    writer = _csv.writer(output, quoting=_csv.QUOTE_ALL)
    writer.writerow(["ticker", "name", "status"])

    if step_number == 1:
        # Source: universe_seed_raw_rows (verbatim EODHD rows) for that run_id.
        # Seeded set: universe_seed_seeded_tickers (run-scoped, not live DB).
        # Exclusion reasons: pipeline_exclusion_report for that run_id.
        # Invariant: raw_rows_total == ok_rows + filtered_rows.

        # Resolve run_id: explicit param or latest by created_at UTC.
        _raw_run_id = run_id
        if not _raw_run_id:
            _latest_raw = await db.universe_seed_raw_rows.find_one(
                {}, {"run_id": 1},
                sort=[("created_at", -1)],
            )
            _raw_run_id = _latest_raw["run_id"] if _latest_raw else None

        if not _raw_run_id:
            writer.writerow(["(no raw data)", "",
                              "Run Step 1 first to generate raw rows"])
        else:
            # A: Load seeded set run-scoped — NOT live tracked_tickers.
            _seeded_us: set = set(
                await db.universe_seed_seeded_tickers.distinct(
                    "ticker", {"run_id": _raw_run_id}
                )
            )
            _seeded_plain: set = {t.replace(".US", "") for t in _seeded_us}

            # Preload exclusion reasons for this run_id — single O(n) query.
            _excl: Dict[str, str] = {}
            async for edoc in db.pipeline_exclusion_report.find(
                {"run_id": _raw_run_id, "step": "Step 1 - Universe Seed"},
                {"_id": 0, "ticker": 1, "reason": 1},
            ):
                _excl[edoc["ticker"]] = edoc["reason"]

            # Stream raw rows in global insertion order — one CSV row per raw row.
            _seen_codes: Dict[str, int] = {}  # normalised_code -> global_raw_row_id

            async for row in db.universe_seed_raw_rows.find(
                {"run_id": _raw_run_id},
                {"_id": 0},
                sort=[("global_raw_row_id", 1)],
            ):
                raw_sym   = row.get("raw_symbol") or {}
                code_raw  = raw_sym.get("Code") or ""
                code_norm = code_raw.strip().upper()
                name      = (raw_sym.get("Name") or "").strip()
                ticker_us = f"{code_norm}.US" if code_norm else "(empty)"

                if not code_norm:
                    writer.writerow([ticker_us, name,
                        _excl.get("(empty)", "Empty ticker code")])
                    continue

                if code_norm in _seen_codes:
                    # Duplicate: already emitted once; reason is in exclusion report.
                    first_gid = _seen_codes[code_norm]
                    writer.writerow([ticker_us, name,
                        _excl.get(ticker_us)
                        or f"{DUPLICATE_REASON_PREFIX} global_raw_row_id={first_gid})"])
                    continue

                _seen_codes[code_norm] = row["global_raw_row_id"]

                if ticker_us in _seeded_us or code_norm in _seeded_plain:
                    writer.writerow([ticker_us, name, "ok"])
                else:
                    writer.writerow([ticker_us, name,
                        _excl.get(ticker_us)
                        or _excl.get(code_norm)
                        or "Filtered"])

    else:  # step_number in (2, 3)
        _STEP_META: Dict[int, tuple] = {
            2: ("Step 2 - Price Sync",        "price_sync"),
            3: ("Step 3 - Fundamentals Sync",  "fundamentals_sync"),
        }
        _step_label, _step_job = _STEP_META[step_number]

        # Resolve this step's run_id (explicit param or latest by created_at).
        if not run_id:
            _le = await db.pipeline_exclusion_report.find_one(
                {"step": _step_label},
                {"_id": 0, "run_id": 1},
                sort=[("created_at", -1)],
            )
            run_id = _le["run_id"] if _le else None
        if not run_id:
            writer.writerow(["(no data)", "", f"Run Step {step_number} first"])
        else:
            # Resolve ops_job_runs doc for this step by details.exclusion_report_run_id.
            _jdoc = await db.ops_job_runs.find_one(
                {"job_name": _step_job,
                 "details.exclusion_report_run_id": run_id},
                {"_id": 0, "details.parent_run_id": 1},
            )
            if not _jdoc:
                writer.writerow(["(chain broken)", "",
                    f"ops_job_runs not found for job={_step_job} "
                    f"with details.exclusion_report_run_id={run_id}. "
                    "Run the full pipeline once with current version deployed."])
            else:
                # Walk the parent chain to resolve s1/s2 run_ids.
                _s1_run_id: Optional[str] = None
                _s2_run_id: Optional[str] = None

                if step_number == 2:
                    _s1_run_id = _jdoc.get("details", {}).get("parent_run_id")
                elif step_number == 3:
                    _s2_run_id = _jdoc.get("details", {}).get("parent_run_id")
                    if _s2_run_id:
                        _s2jdoc = await db.ops_job_runs.find_one(
                            {"job_name": "price_sync",
                             "details.exclusion_report_run_id": _s2_run_id},
                            {"_id": 0, "details.parent_run_id": 1},
                        )
                        _s1_run_id = (_s2jdoc or {}).get("details", {}).get("parent_run_id")

                if not _s1_run_id:
                    writer.writerow(["(chain broken)", "",
                        "parent_run_id is NULL for this run — was triggered manually "
                        "without a pipeline chain. Re-run Step 1 through the scheduler "
                        "to establish a linked chain."])
                else:
                    # Build Step 1 seeded set (run-scoped, .US format).
                    _s1_seeded: set = set(
                        await db.universe_seed_seeded_tickers.distinct(
                            "ticker", {"run_id": _s1_run_id}
                        )
                    )

                    # Derive this step's input by subtracting upstream filtered sets.
                    _input: set = set(_s1_seeded)

                    if step_number >= 3 and _s2_run_id:
                        async for edoc in db.pipeline_exclusion_report.find(
                            {"run_id": _s2_run_id, "step": "Step 2 - Price Sync"},
                            {"_id": 0, "ticker": 1},
                        ):
                            _input.discard(edoc["ticker"])

                    # Preload this step's exclusion reasons — single O(n) query.
                    _excl: Dict[str, str] = {}
                    async for edoc in db.pipeline_exclusion_report.find(
                        {"run_id": run_id, "step": _step_label},
                        {"_id": 0, "ticker": 1, "reason": 1},
                    ):
                        _excl[edoc["ticker"]] = edoc["reason"]

                    # Name lookup — chunked in batches of 500.
                    _names: Dict[str, str] = {}
                    _input_list = sorted(_input)
                    for _ci in range(0, len(_input_list), 500):
                        async for tdoc in db.tracked_tickers.find(
                            {"ticker": {"$in": _input_list[_ci:_ci + 500]}},
                            {"_id": 0, "ticker": 1, "name": 1},
                        ):
                            _names[tdoc["ticker"]] = tdoc.get("name", "")

                    # Emit exactly one row per input ticker.
                    for ticker in _input_list:
                        writer.writerow([ticker, _names.get(ticker, ""),
                                         _excl.get(ticker, "ok")])

    output.seek(0)
    return _SR(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=pipeline_step{step_number}_export.csv"},
    )


@api_router.post("/admin/pipeline/run-full-now")
async def admin_run_full_pipeline_now(background_tasks: BackgroundTasks):
    """
    Run the full Step 1→3 pipeline chain immediately, exactly like the scheduler.
    Each step receives the exact parent_run_id from the preceding step.
    Step 3 includes fundamentals sync + visibility recompute.
    Returns a chain_run_id — use it to poll /chain-status/<id> and download
    /export/full?chain_run_id=<id>.
    Auth: AdminAuthMiddleware.
    """
    import uuid as _uuid2

    # Guard: refuse if a per-step manual universe seed is already running.
    _step_running = await db.ops_job_runs.find_one(
        {"job_name": "universe_seed", "status": "running", "source": "admin_manual"},
    )
    if _step_running:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "busy",
                "message": "A per-step Universe Seed run is currently in progress. Wait for it to finish before starting the full chain.",
                "job_id": _step_running.get("job_id"),
            },
        )
    # Guard: refuse if another full chain is already running.
    # First, auto-cleanup orphaned chains whose child jobs are all terminal.
    from scheduler_service import _finalize_orphaned_chain_runs
    await _finalize_orphaned_chain_runs(db, datetime.now(timezone.utc))
    _chain_running = await db.pipeline_chain_runs.find_one({"status": "running"})
    if _chain_running:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "busy",
                "message": "A full pipeline chain is already running.",
                "chain_run_id": _chain_running.get("chain_run_id"),
            },
        )

    chain_run_id = f"chain_{_uuid2.uuid4().hex[:12]}"

    async def _run_chain(chain_id: str) -> None:
        logger.info(f"[run-full-now] Starting chain {chain_id}")

        await db.pipeline_chain_runs.insert_one({
            "chain_run_id": chain_id,
            "status":       "running",
            "current_step": 1,
            "steps_done":   [],
            "started_at":   datetime.now(timezone.utc),
            "step_run_ids": {},
        })

        step_run_ids: Dict[str, Optional[str]] = {
            "step1": None, "step2": None, "step3": None, "step3_visibility": None
        }
        chain_status = "completed"
        chain_error: Optional[str] = None
        chain_failed_step: Optional[int] = None
        last_completed_step: int = 0
        _all_steps_done = False  # Set True only when all steps complete; used in finally

        async def _cancelled() -> bool:
            _d = await db.pipeline_chain_runs.find_one(
                {"chain_run_id": chain_id}, {"cancel_requested": 1}
            )
            return bool(_d and _d.get("cancel_requested"))

        try:
            import uuid as _uuidc
            # ── Step 1 ────────────────────────────────────────────────────────
            job_id_s1 = f"universe_seed_{_uuidc.uuid4().hex[:8]}"
            s1_started_at = datetime.now(timezone.utc)
            # Insert running sentinel so Admin overview shows Step 1 as "running"
            # with job_name="universe_seed" (matches JOB_REGISTRY and get_job_last_runs).
            _s1_run_doc_id = (await db.ops_job_runs.insert_one({
                "job_name": "universe_seed",
                "status": "running",
                "source": "admin_manual",
                "started_at": s1_started_at,
                "started_at_prague": _sched_to_prague_iso(s1_started_at),
                "log_timezone": "Europe/Prague",
                "details": {"chain_run_id": chain_id},
            })).inserted_id
            try:
                # Progress callback: update the sentinel live so UI polls show
                # "processed / total" tickers without waiting for Step 1 to finish.
                async def _s1_progress(processed: int, total: int) -> None:
                    await db.ops_job_runs.update_one(
                        {"_id": _s1_run_doc_id},
                        {"$set": {
                            "progress": f"Step 1: {processed} / {total} tickers seeded",
                            "progress_processed": processed,
                            "progress_total": total,
                            "progress_pct": round(100 * processed / total) if total else 0,
                        }},
                    )

                async def _s1_raw_total(raw_rows_total: int) -> None:
                    """Write raw total to sentinel as soon as all exchange symbols are fetched."""
                    await db.ops_job_runs.update_one(
                        {"_id": _s1_run_doc_id},
                        {"$set": {
                            "raw_rows_total": raw_rows_total,
                            "details.raw_rows_total": raw_rows_total,
                        }},
                    )

                s1_result = await sync_ticker_whitelist(
                    db, dry_run=False, job_run_id=job_id_s1,
                    progress_callback=_s1_progress,
                    raw_total_callback=_s1_raw_total,
                )
                s1_finished_at = datetime.now(timezone.utc)
                s1_run_id: Optional[str] = s1_result.get("raw_run_id") or job_id_s1
                _s1_seeded_total = s1_result.get("seeded_total") or 0
                # Update the sentinel with completed status, full result, and final progress.
                await db.ops_job_runs.update_one(
                    {"_id": _s1_run_doc_id},
                    {"$set": {
                        "status": "completed",
                        "finished_at": s1_finished_at,
                        "finished_at_prague": _sched_to_prague_iso(s1_finished_at),
                        "duration_seconds": (s1_finished_at - s1_started_at).total_seconds(),
                        "result": s1_result,
                        "details": {
                            "chain_run_id": chain_id,
                            "exclusion_report_run_id": s1_result.get("raw_run_id"),
                            "fetched": s1_result.get("fetched"),
                            "raw_rows_total": s1_result.get("raw_rows_total"),
                            "seeded_total": _s1_seeded_total,
                            "filtered_out_total_step1": s1_result.get("filtered_out_total_step1"),
                            "fetched_raw_per_exchange": s1_result.get("fetched_raw_per_exchange"),
                        },
                        "progress": f"Completed: {_s1_seeded_total:,} seeded",
                        "progress_processed": _s1_seeded_total,
                        "progress_total": _s1_seeded_total,
                        "progress_pct": 100,
                    }},
                )
            except Exception as _s1_exc:
                _s1_fail_at = datetime.now(timezone.utc)
                await db.ops_job_runs.update_one(
                    {"_id": _s1_run_doc_id},
                    {"$set": {
                        "status": "failed",
                        "finished_at": _s1_fail_at,
                        "finished_at_prague": _sched_to_prague_iso(_s1_fail_at),
                        "error": str(_s1_exc),
                    }},
                )
                raise  # re-raise so the outer except marks chain as failed
            step_run_ids["step1"] = s1_run_id
            await db.pipeline_chain_runs.update_one(
                {"chain_run_id": chain_id},
                {"$set": {
                    "step_run_ids.step1": s1_run_id,
                    "current_step": 2,
                    "steps_done": [1],
                }},
            )
            logger.info(f"[run-full-now] Step 1 done: {s1_run_id}")
            last_completed_step = 1

            # Chain robustness: verify Step 1 run record was persisted in DB.
            _s1_verify = await db.ops_job_runs.find_one(
                {"_id": _s1_run_doc_id}, {"_id": 1}
            )
            if not _s1_verify:
                raise RuntimeError(f"Step 1 run record not found (doc_id={_s1_run_doc_id})")

            if await _cancelled():
                chain_status = "cancelled"
                raise Exception("cancelled")

            # ── Step 2 ────────────────────────────────────────────────────────
            s2_started_at = datetime.now(timezone.utc)
            # Insert admin_manual sentinel before delegating to run_daily_price_sync
            # so the chain orchestrator owns lifecycle; the function reuses this doc.
            _s2_run_doc_id = (await db.ops_job_runs.insert_one({
                "job_name": "price_sync",
                "status": "running",
                "source": "admin_manual",
                "started_at": s2_started_at,
                "started_at_prague": _sched_to_prague_iso(s2_started_at),
                "log_timezone": "Europe/Prague",
                "details": {"chain_run_id": chain_id, "parent_run_id": s1_run_id},
                "phase": "bulk_catchup",
                "progress_processed": 0,
                "progress_total": 0,
                "progress_pct": 0,
            })).inserted_id
            try:
                s2_result = await run_daily_price_sync(
                    db, ignore_kill_switch=True, parent_run_id=s1_run_id,
                    chain_run_id=chain_id,
                    run_doc_id=_s2_run_doc_id,
                    cancel_check=_cancelled,
                )
                if s2_result.get("status") == "cancelled":
                    chain_status = "cancelled"
                    raise Exception("cancelled")
                if s2_result.get("status") == "failed":
                    raise RuntimeError(
                        f"Step 2 price_sync failed: {s2_result.get('error', 'unknown error')}"
                    )
                s2_finished_at = datetime.now(timezone.utc)
                await db.ops_job_runs.update_one(
                    {"_id": _s2_run_doc_id},
                    {"$set": {
                        "finished_at_prague": _sched_to_prague_iso(s2_finished_at),
                        "duration_seconds": (s2_finished_at - s2_started_at).total_seconds(),
                    }},
                )
            except Exception as _s2_exc:
                # Only mark as failed if not a cancellation (cancel already sets status in DB)
                if chain_status != "cancelled":
                    _s2_fail_at = datetime.now(timezone.utc)
                    await db.ops_job_runs.update_one(
                        {"_id": _s2_run_doc_id},
                        {"$set": {
                            "status": "failed",
                            "finished_at": _s2_fail_at,
                            "finished_at_prague": _sched_to_prague_iso(_s2_fail_at),
                            "error": str(_s2_exc),
                        }},
                    )
                    chain_failed_step = 2
                raise  # re-raise so the outer except marks chain as failed/cancelled
            s2_run_id: Optional[str] = s2_result.get("exclusion_report_run_id")
            if not s2_run_id:
                raise RuntimeError("Step 2 run record not found (exclusion_report_run_id missing)")
            step_run_ids["step2"] = s2_run_id
            await db.pipeline_chain_runs.update_one(
                {"chain_run_id": chain_id},
                {"$set": {
                    "step_run_ids.step2": s2_run_id,
                    "current_step": 3,
                    "steps_done": [1, 2],
                }},
            )
            logger.info(f"[run-full-now] Step 2 done: {s2_run_id}")
            last_completed_step = 2

            if await _cancelled():
                chain_status = "cancelled"
                raise Exception("cancelled")

            # ── Step 3 (includes visibility recompute) ───────────────────────
            s3_result = await run_fundamentals_changes_sync(
                db, ignore_kill_switch=True, parent_run_id=s2_run_id,
                cancel_check=_cancelled, chain_run_id=chain_id,
            )
            if s3_result.get("status") == "cancelled":
                chain_status = "cancelled"
                raise Exception("cancelled")
            s3_run_id: Optional[str] = s3_result.get("exclusion_report_run_id")
            # Visibility exclusion_report_run_id is set by the Step 3 visibility recompute.
            s3_vis_run_id: Optional[str] = s3_result.get("step3_visibility_exclusion_report_run_id")

            # Chain robustness: verify Step 3 produced a usable run_id.
            if not s3_run_id:
                raise RuntimeError("Step 3 run record not found (exclusion_report_run_id missing)")

            step_run_ids["step3"] = s3_run_id
            step_run_ids["step3_visibility"] = s3_vis_run_id
            await db.pipeline_chain_runs.update_one(
                {"chain_run_id": chain_id},
                {"$set": {
                    "step_run_ids.step3": s3_run_id,
                    "step_run_ids.step3_visibility": s3_vis_run_id,
                    "steps_done": [1, 2, 3],
                    "current_step": None,
                }},
            )
            logger.info(f"[run-full-now] Step 3 done: {s3_run_id}, visibility: {s3_vis_run_id}")
            last_completed_step = 3
            _all_steps_done = True

        except Exception as exc:
            if chain_status != "cancelled":
                chain_status = "failed"
            chain_error = str(exc) if chain_status != "cancelled" else None
            if chain_failed_step is None and chain_status == "failed":
                # Fallback inference for unexpected failures not caught above.
                # Step 2 handler sets failed_step explicitly; Steps 1/3/post-run rely on this branch.
                if last_completed_step == 0:
                    chain_failed_step = 1
                elif last_completed_step < 3:
                    chain_failed_step = last_completed_step + 1
                else:
                    logger.warning(
                        f"[run-full-now] Chain {chain_id} failed after completing all steps (post-run failure)")
            if chain_status == "cancelled":
                logger.info(f"[run-full-now] Chain {chain_id} cancelled by request")
            else:
                logger.error(f"[run-full-now] Chain {chain_id} failed: {exc}")

        finally:
            # Guarantee finalization even if a BaseException (e.g. asyncio.CancelledError)
            # bypassed the except block, which would leave the chain stuck in a non-terminal
            # status (e.g. "step1_done") with no finished_at.
            _fin_doc: Dict[str, Any] = {}
            try:
                _fin_doc = await db.pipeline_chain_runs.find_one(
                    {"chain_run_id": chain_id},
                    {"cancel_requested": 1, "status": 1, "finished_at": 1},
                ) or {}
            except Exception:
                _fin_doc = {}
            _cancel_requested = bool(_fin_doc.get("cancel_requested"))
            _current_status = _fin_doc.get("status")
            _current_finished_at = _fin_doc.get("finished_at")

            if not _all_steps_done and chain_status == "completed":
                # Reached here via BaseException — determine correct terminal status.
                if _cancel_requested:
                    chain_status = "cancelled"
                else:
                    chain_status = "failed"

            _chain_set: Dict[str, Any] = {
                "step_run_ids": step_run_ids,
                "error": chain_error,
                "failed_step": chain_failed_step,
            }
            _finished_now = None
            if _cancel_requested and _current_status != "failed":
                chain_status = "cancelled"
                _chain_set["status"] = "cancelled"
                if _current_finished_at is None:
                    _finished_now = datetime.now(timezone.utc)
                    _chain_set["finished_at"] = _finished_now
                    _chain_set["finished_at_prague"] = _sched_to_prague_iso(_finished_now)
            else:
                _chain_set["status"] = chain_status
                if _current_finished_at is None:
                    _finished_now = datetime.now(timezone.utc)
                    _chain_set["finished_at"] = _finished_now
                    _chain_set["finished_at_prague"] = _sched_to_prague_iso(_finished_now)

            await db.pipeline_chain_runs.update_one(
                {"chain_run_id": chain_id},
                {"$set": _chain_set},
            )
                        # Fallback: pokud je cancel_requested==true a finished_at stále chybí,
            # doraz nekompletní záznam (ochrana proti race/nomatch v hlavním updatu).
            if _cancel_requested:
                _fallback_at = _finished_now or datetime.now(timezone.utc)
                await db.pipeline_chain_runs.update_one(
                    {
                        "chain_run_id": chain_id,
                        "cancel_requested": True,
                        "finished_at": {"$exists": False},
                    },
                    {"$set": {
                        "status": "cancelled" if _current_status != "failed" else "failed",
                        "finished_at": _fallback_at,
                        "finished_at_prague": _sched_to_prague_iso(_fallback_at),
                    }},
                )
            logger.info(f"[run-full-now] Chain {chain_id} {chain_status}")

            # Step 2 sentinel deterministic finalization.
            # When the chain ends non-normally and step2 completed (step_run_ids.step2
            # is non-null), the price_sync sentinel must never remain status="running".
            # We look it up by job_name + details.exclusion_report_run_id (the run-id
            # string stored in step_run_ids.step2 and written by
            # save_price_sync_exclusion_report into the sentinel's details).
            # Using details.chain_run_id is NOT reliable (real observed case: absent).
            if chain_status in ("cancelled", "failed"):
                _chain_doc = await db.pipeline_chain_runs.find_one(
                    {"chain_run_id": chain_id},
                    {"step_run_ids": 1},
                )
                _s2_run_id = (
                    (_chain_doc or {}).get("step_run_ids") or {}
                ).get("step2")
                if _s2_run_id:
                    _s2_fin_at = datetime.now(timezone.utc)
                    await db.ops_job_runs.update_one(
                        {
                            "job_name": "price_sync",
                            "status": "running",
                            "details.exclusion_report_run_id": _s2_run_id,
                        },
                        {"$set": {
                            "status": "cancelled",
                            "finished_at": _s2_fin_at,
                            "finished_at_prague": _sched_to_prague_iso(_s2_fin_at),
                            "details.cancelled_by": "chain_cancel",
                            "details.chain_run_id": chain_id,
                        }},
                    )

    background_tasks.add_task(_run_chain, chain_run_id)
    return {
        "status":       "started",
        "chain_run_id": chain_run_id,
        "message": (
            f"Full pipeline chain started (chain_run_id={chain_run_id}). "
            "Poll /api/admin/pipeline/chain-status/<chain_run_id> until status=completed, "
            "then download /api/admin/pipeline/export/full?chain_run_id=<chain_run_id>."
        ),
    }


@api_router.get("/admin/pipeline/chain-status/{chain_run_id}")
async def admin_pipeline_chain_status(chain_run_id: str):
    """Poll status of a run-full-now chain. Auth: AdminAuthMiddleware."""
    doc = await db.pipeline_chain_runs.find_one(
        {"chain_run_id": chain_run_id}, {"_id": 0}
    )
    if not doc:
        from fastapi import HTTPException as _HE
        raise _HE(status_code=404, detail=f"chain_run_id not found: {chain_run_id}")
    for k in ("started_at", "finished_at"):
        if isinstance(doc.get(k), datetime):
            doc[k] = doc[k].isoformat()
    # Derive current_step, steps_done, failed_step for UI live progress.
    # Prefer stored fields (new behaviour); fall back to legacy status derivation
    # for backward-compat with chain docs created before this fix.
    _status = doc.get("status")
    _srids = doc.get("step_run_ids", {})
    _steps_done = doc.get("steps_done")
    _current_step = doc.get("current_step")
    _failed_step: Optional[int] = None

    # Legacy fallback: derive from status string if stored fields are absent
    if _steps_done is None:
        _steps_done = [i for i, k in enumerate(("step1", "step2", "step3"), 1) if _srids.get(k)]
    if _current_step is None and _status in ("running", "step1_done", "step2_done", "step3_done"):
        if _status == "running":
            _current_step = 1
        elif _status == "step1_done":
            _current_step = 2
        elif _status == "step2_done":
            _current_step = 3
        elif _status == "step3_done":
            _current_step = None
    if _status == "failed":
        _failed_step = next(
            (i for i, k in enumerate(("step1", "step2", "step3"), 1) if not _srids.get(k)),
            3,
        )
    doc["current_step"] = _current_step
    doc["steps_done"] = _steps_done
    doc["failed_step"] = _failed_step
    return doc


@api_router.post("/admin/pipeline/chain-cancel/{chain_run_id}")
async def admin_pipeline_chain_cancel(chain_run_id: str):
    """Request cancellation of a running chain. Auth: AdminAuthMiddleware."""
    from fastapi import HTTPException as _HEc

    doc = await db.pipeline_chain_runs.find_one({"chain_run_id": chain_run_id}, {"status": 1})
    if not doc:
        raise _HEc(status_code=404, detail=f"chain_run_id not found: {chain_run_id}")

    _active = {"running", "step1_done", "step2_done"}
    if doc.get("status") not in _active:
        raise _HEc(status_code=400, detail=f"Chain is not running (status={doc.get('status')})")

    await db.pipeline_chain_runs.update_one(
        {"chain_run_id": chain_run_id},
        {"$set": {"cancel_requested": True}},
    )

    # Eagerly finalize the chain on cancel (prevents hanging chains without finished_at)
    _now = datetime.now(timezone.utc)
    await db.pipeline_chain_runs.update_one(
        {
            "chain_run_id": chain_run_id,
            "cancel_requested": True,
            "finished_at": {"$exists": False},
            "status": {"$ne": "failed"},
        },
        {"$set": {
            "status": "cancelled",
            "finished_at": _now,
            "finished_at_prague": _sched_to_prague_iso(_now),
        }},
    )

    logger.info(f"[chain-cancel] Cancel requested for {chain_run_id}")

    # Eagerly finalize any ops_job_runs docs that are still status="running" for
    # this chain.  Guards against the edge case where _run_chain's defensive
    # cleanup never executes (e.g. background task crash, unhandled BaseException,
    # or a DB error that occurs after pipeline_chain_runs is already set to
    # "cancelled").  Uses $set with dot-notation so progress_* and all existing
    # detail fields are preserved — only cancellation metadata is added.
    _cancel_at = datetime.now(timezone.utc)
    _finalized = await db.ops_job_runs.update_many(
        {
            "status": "running",
            "details.chain_run_id": chain_run_id,
        },
        {"$set": {
            "status": "cancelled",
            "finished_at": _cancel_at,
            "finished_at_prague": _sched_to_prague_iso(_cancel_at),
            "details.cancelled_by": "chain_cancel",
            "details.chain_run_id": chain_run_id,
        }},
    )
    if _finalized.modified_count:
        logger.warning(
            f"[chain-cancel] Eagerly finalized {_finalized.modified_count} "
            f"running ops_job_runs doc(s) for chain {chain_run_id}"
        )

    return {"status": "cancel_requested", "chain_run_id": chain_run_id}


# ─────────────────────────────────────────────────────────────────────────────
# Canonical Pipeline Report — single source of truth for both UI and CSV
# ─────────────────────────────────────────────────────────────────────────────
async def build_canonical_pipeline_report(db_ref, chain_run_id: str) -> Dict[str, Any]:
    """
    Build the canonical funnel report for a given chain_run_id.
    Both the admin UI cards and the CSV export MUST use this exact payload.

    Funnel semantics (strict sequential, 3-step main pipeline):
      raw_symbols → Step 1 (seeded) → Step 2 (with_price) → Step 3 (visible)

    Reconciliation rule:
      raw_symbols == step1_filtered_out + step2_filtered_out + step3_filtered_out + visible
    """
    from zoneinfo import ZoneInfo as _ZI
    _PRAGUE = _ZI("Europe/Prague")

    chain_doc = await db_ref.pipeline_chain_runs.find_one(
        {"chain_run_id": chain_run_id}, {"_id": 0}
    )
    if not chain_doc:
        return {"error": f"chain_run_id not found: {chain_run_id}"}

    sids = chain_doc.get("step_run_ids") or {}
    s1_run_id = sids.get("step1")

    # ── Raw symbols count (from Step 1 run) ─────────────────────────────────
    raw_symbols = 0
    if s1_run_id:
        # Count distinct codes from universe_seed_raw_rows
        raw_codes: set = set()
        async for raw_row in db_ref.universe_seed_raw_rows.find(
            {"run_id": s1_run_id},
            {"_id": 0, "raw_symbol.Code": 1},
        ):
            code = (raw_row.get("raw_symbol") or {}).get("Code", "")
            code_norm = code.strip().upper()
            if code_norm:
                raw_codes.add(code_norm)
        raw_symbols = len(raw_codes)

    # ── Seeded tickers count ────────────────────────────────────────────────
    seeded_tickers = 0
    if s1_run_id:
        seeded_tickers = await db_ref.universe_seed_seeded_tickers.count_documents(
            {"run_id": s1_run_id}
        )
    # Fallback to tracked_tickers count if seed run data is absent
    if seeded_tickers == 0:
        seeded_tickers = await db_ref.tracked_tickers.count_documents({
            "exchange": {"$in": ["NYSE", "NASDAQ"]},
            "asset_type": "Common Stock",
        })

    # ── With price (Step 2 output) ──────────────────────────────────────────
    with_price = await db_ref.tracked_tickers.count_documents({
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
        "has_price_data": True,
    })

    # ── Visible (Step 3 output) ─────────────────────────────────────────────
    visible = await db_ref.tracked_tickers.count_documents({
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
        "has_price_data": True,
        "fundamentals_status": "complete",
        "is_visible": True,
    })

    # ── Filtered-out counts (strict 3-step funnel) ──────────────────────────
    step1_filtered_out = max(raw_symbols - seeded_tickers, 0)
    step2_filtered_out = max(seeded_tickers - with_price, 0)
    step3_filtered_out = max(with_price - visible, 0)

    # ── Step 3 sub-breakdown ────────────────────────────────────────────────
    # Each non-visible ticker gets exactly one primary reason category.
    # fundamentals_blocker: with_price but fundamentals_status != "complete"
    # visibility_rule: fundamentals_status == "complete" but is_visible != true
    fundamentals_blocker = await db_ref.tracked_tickers.count_documents({
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
        "has_price_data": True,
        "$or": [
            {"fundamentals_status": {"$ne": "complete"}},
            {"fundamentals_status": {"$exists": False}},
        ],
    })
    visibility_rule = max(step3_filtered_out - fundamentals_blocker, 0)

    now_prague = datetime.now(_PRAGUE)

    return {
        "chain_run_id": chain_run_id,
        "last_generated_at_prague": now_prague.isoformat(),
        "raw_symbols": raw_symbols,
        "seeded_tickers": seeded_tickers,
        "with_price": with_price,
        "visible": visible,
        "step1_filtered_out": step1_filtered_out,
        "step2_filtered_out": step2_filtered_out,
        "step3_filtered_out": step3_filtered_out,
        "step3_sub_breakdown": {
            "fundamentals_blocker": fundamentals_blocker,
            "visibility_rule": visibility_rule,
        },
        "reconciliation_check": (
            raw_symbols == step1_filtered_out + step2_filtered_out
            + step3_filtered_out + visible
        ),
        "steps_done": chain_doc.get("steps_done", []),
        "status": chain_doc.get("status"),
    }


@api_router.get("/admin/pipeline/report")
async def admin_pipeline_report(
    chain_run_id: str = Query(..., description="chain_run_id from pipeline chain run"),
):
    """
    Canonical pipeline report for a given chain_run_id.
    Both Admin UI funnel cards and CSV export read from this same payload.
    Auth: AdminAuthMiddleware.
    """
    report = await build_canonical_pipeline_report(db, chain_run_id)
    if report.get("error"):
        from fastapi import HTTPException as _HEr
        raise _HEr(status_code=404, detail=report["error"])
    return report

@api_router.get("/admin/pipeline/export/full")
async def admin_pipeline_export_full(
    chain_run_id: str = Query(..., description="chain_run_id from POST /run-full-now"),
):
    """
    Download a unified pipeline CSV for a full chain run.
    Columns: ticker, name, status, failed_step, reason_code, reason_text
    One row per Step 1 raw row.
    status = OK/FAIL, failed_step = first pipeline step where the ticker was filtered.
    Auth: AdminAuthMiddleware.
    """
    from fastapi.responses import StreamingResponse as _SR2
    from fastapi import HTTPException as _HE2
    import io as _io2
    import csv as _csv2

    chain_doc = await db.pipeline_chain_runs.find_one(
        {"chain_run_id": chain_run_id}, {"_id": 0}
    )
    if not chain_doc:
        raise _HE2(status_code=404, detail=f"chain_run_id not found: {chain_run_id}")
    if chain_doc.get("status") not in ("completed", "step3_done"):
        raise _HE2(status_code=409,
            detail=f"Chain not finished (status={chain_doc.get('status')}). "
                   "Wait for status=completed then retry.")

    sids = chain_doc.get("step_run_ids") or {}
    s1_run_id = sids.get("step1")
    if not s1_run_id:
        raise _HE2(status_code=409, detail="Step 1 run_id missing in chain.")

    _STEP_LABELS = {
        "step1": "Step 1 - Universe Seed",
        "step2": "Step 2 - Price Sync",
        "step3": "Step 3 - Fundamentals Sync",
        "visibility": "Step 3 - Fundamentals Sync",  # visibility is a Step 3 sub-reason
    }

    _ALLOWED_FAILED_STEPS = {
        _STEP_LABELS["step1"],
        _STEP_LABELS["step2"],
        _STEP_LABELS["step3"],
    }

    def _normalize_seeded_ticker(value: Any) -> Optional[str]:
        raw = str(value).strip().upper()
        if not raw:
            return None
        if "." in raw:
            return raw
        return f"{raw}.US"

    def _to_reason_code(reason: Optional[str]) -> str:
        if not reason:
            return "unknown_failure"
        _clean = re.sub(r"[^a-z0-9\s]+", " ", reason.lower())
        parts = [p for p in _clean.split() if p]
        _code = "_".join(parts) if parts else "unknown_failure"
        # This helper is used only for FAIL rows; fail reason_code must never be "ok".
        return "unknown_failure" if _code == "ok" else _code

    # Preload exclusion reasons per step — one query each (O(n) total).
    _excl: Dict[str, Dict[str, str]] = {"step1": {}, "step2": {}, "step3": {}, "visibility": {}}
    for _sk, _label in _STEP_LABELS.items():
        if _sk == "visibility":
            continue
        _rid = sids.get(_sk)
        if not _rid:
            continue
        async for edoc in db.pipeline_exclusion_report.find(
            {"run_id": _rid, "step": _label},
            {"_id": 0, "ticker": 1, "reason": 1},
        ):
            _excl[_sk][edoc["ticker"]] = edoc["reason"]

    # Also load step3 visibility exclusion rows (different run_id, same source step label)
    _s3_vis_rid = sids.get("step3_visibility")
    if _s3_vis_rid:
        async for edoc in db.pipeline_exclusion_report.find(
            {"run_id": _s3_vis_rid, "step": "Step 3 - Fundamentals Sync"},
            {"_id": 0, "ticker": 1, "reason": 1},
        ):
            _excl["visibility"].setdefault(edoc["ticker"], edoc["reason"])

    _seeded_tickers: set = set()
    for _ticker in await db.universe_seed_seeded_tickers.distinct(
        "ticker", {"run_id": s1_run_id}
    ):
        _norm = _normalize_seeded_ticker(_ticker)
        if _norm:
            _seeded_tickers.add(_norm)
    for _ticker in await db.tracked_tickers.distinct("ticker", {"is_seeded": True}):
        _norm = _normalize_seeded_ticker(_ticker)
        if _norm:
            _seeded_tickers.add(_norm)

    # Pre-load with_price and visible ticker sets for deterministic per-row
    # classification that matches the canonical report funnel math.
    _with_price_tickers: set = set()
    async for _tdoc in db.tracked_tickers.find(
        {"exchange": {"$in": ["NYSE", "NASDAQ"]}, "asset_type": "Common Stock",
         "has_price_data": True},
        {"_id": 0, "ticker": 1},
    ):
        _with_price_tickers.add(_tdoc["ticker"])

    _visible_tickers: set = set()
    async for _tdoc in db.tracked_tickers.find(
        {"exchange": {"$in": ["NYSE", "NASDAQ"]}, "asset_type": "Common Stock",
         "has_price_data": True, "fundamentals_status": "complete", "is_visible": True},
        {"_id": 0, "ticker": 1},
    ):
        _visible_tickers.add(_tdoc["ticker"])

    # Build canonical report to embed summary counts in CSV
    _canonical = await build_canonical_pipeline_report(db, chain_run_id)

    output = _io2.StringIO()
    writer = _csv2.writer(output, quoting=_csv2.QUOTE_ALL)

    # Metadata header rows
    writer.writerow(["# chain_run_id", chain_run_id])
    writer.writerow(["# last_generated_at_prague", _canonical.get("last_generated_at_prague", "")])
    writer.writerow(["# raw_symbols", _canonical.get("raw_symbols", "")])
    writer.writerow(["# seeded_tickers", _canonical.get("seeded_tickers", "")])
    writer.writerow(["# with_price", _canonical.get("with_price", "")])
    writer.writerow(["# visible", _canonical.get("visible", "")])
    writer.writerow(["# step1_filtered_out", _canonical.get("step1_filtered_out", "")])
    writer.writerow(["# step2_filtered_out", _canonical.get("step2_filtered_out", "")])
    writer.writerow(["# step3_filtered_out", _canonical.get("step3_filtered_out", "")])
    _sub = _canonical.get("step3_sub_breakdown", {})
    # "step3_sub:" prefix distinguishes sub-breakdown metadata from the main
    # step3_filtered_out total, so CSV parsers can separate them cleanly.
    writer.writerow(["# step3_sub: fundamentals_blocker", _sub.get("fundamentals_blocker", "")])
    writer.writerow(["# step3_sub: visibility_rule", _sub.get("visibility_rule", "")])
    writer.writerow([])  # blank separator

    # Canonical funnel summary rows — always present so a pivot never loses a step
    writer.writerow(["# --- Canonical Funnel Summary ---"])
    writer.writerow(["# Step 1 filtered out", _canonical.get("step1_filtered_out", "")])
    writer.writerow(["# Step 2 filtered out", _canonical.get("step2_filtered_out", "")])
    writer.writerow(["# Step 3 filtered out", _canonical.get("step3_filtered_out", "")])
    writer.writerow(["# Visible", _canonical.get("visible", "")])
    writer.writerow([])  # blank separator

    writer.writerow(["ticker", "name", "status", "failed_step", "reason_code", "reason_text"])

    # Stream Step 1 raw rows in global order.
    # One output row per distinct ticker code — cross-exchange duplicates (same
    # code on both NYSE and NASDAQ) are silently skipped after the first
    # occurrence so that CSV row count == Admin "raw" count (distinct codes).
    # The first occurrence's exclusion reason (from pipeline_exclusion_report)
    # already contains the "Duplicate" annotation for that code, so no
    # information is lost.
    _seen_codes: Dict[str, int] = {}  # normalised_code -> global_raw_row_id

    async for raw_row in db.universe_seed_raw_rows.find(
        {"run_id": s1_run_id},
        {"_id": 0},
        sort=[("global_raw_row_id", 1)],
    ):
        raw_sym   = raw_row.get("raw_symbol") or {}
        code_raw  = raw_sym.get("Code") or ""
        code_norm = code_raw.strip().upper()
        name      = (raw_sym.get("Name") or "").strip()
        ticker_us = f"{code_norm}.US" if code_norm else "(empty)"

        if not code_norm:
            reason = _excl["step1"].get("(empty)", "Empty ticker code")
            writer.writerow([
                ticker_us,
                name,
                "FAIL",
                _STEP_LABELS["step1"],
                _to_reason_code(reason),
                reason,
            ])
            continue

        if code_norm in _seen_codes:
            # Cross-exchange duplicate raw row — the first occurrence has already
            # been written (with the correct "Duplicate" reason from the exclusion
            # report).  Skip this row to keep CSV row count == distinct code count.
            continue
        _seen_codes[code_norm] = raw_row["global_raw_row_id"]

        # Find first step where this ticker was filtered.
        # For step1, skip "Duplicate (first at…)" entries because those entries
        # in pipeline_exclusion_report represent subsequent (cross-exchange)
        # occurrences of the code, not the first occurrence we are outputting now.
        _out_step:   Optional[str] = None
        _out_reason: Optional[str] = None
        for _sk in ("step1", "step2", "step3", "visibility"):
            _r = _excl[_sk].get(ticker_us)
            if _r is not None:
                if _sk == "step1" and isinstance(_r, str) and _r.startswith(DUPLICATE_REASON_PREFIX):
                    # This exclusion entry belongs to a subsequent occurrence;
                    # the first occurrence is not excluded at step1 for this reason.
                    continue
                _out_step   = _STEP_LABELS[_sk]
                _out_reason = _r
                break

        if _out_step:
            _reason_text = (_out_reason or "").strip()
            if not _reason_text:
                _reason_text = "Excluded by pipeline filter."
            _reason_code = _to_reason_code(_reason_text)
            if _out_step not in _ALLOWED_FAILED_STEPS:
                _out_step = _STEP_LABELS["step3"]
            writer.writerow([ticker_us, name, "FAIL", _out_step, _reason_code, _reason_text])
        else:
            _ticker_norm = _normalize_seeded_ticker(ticker_us)
            if not _ticker_norm or _ticker_norm not in _seeded_tickers:
                writer.writerow([
                    ticker_us,
                    name,
                    "FAIL",
                    _STEP_LABELS["step1"],
                    "not_seeded",
                    "Ticker is not in seeded set.",
                ])
                continue
            # Seeded but no price data → Step 2 failure
            if _ticker_norm not in _with_price_tickers:
                writer.writerow([ticker_us, name, "FAIL", _STEP_LABELS["step2"],
                                 "no_price_data", "Seeded ticker has no price data."])
                continue
            # Has price but not visible → Step 3 failure
            if _ticker_norm not in _visible_tickers:
                writer.writerow([ticker_us, name, "FAIL", _STEP_LABELS["step3"],
                                 "not_visible", "Ticker has price data but is not visible."])
                continue
            # Required schema invariant: OK rows must keep failed_step and reason_text empty.
            writer.writerow([ticker_us, name, "OK", "", "ok", ""])

    output.seek(0)
    return _SR2(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition":
            f"attachment; filename=pipeline_full_{chain_run_id}.csv"},
    )


@api_router.get("/admin/pipeline/exclusion-report/download")
async def admin_download_pipeline_exclusion_report(
    report_date: str = Query(None, description="Report date in YYYY-MM-DD (defaults to latest available date)"),
    step: str = Query(None, description="Optional step filter"),
):
    """
    Download pipeline exclusion report as CSV.
    """
    from zoneinfo import ZoneInfo
    from fastapi.responses import StreamingResponse
    import io
    import csv

    PRAGUE = ZoneInfo("Europe/Prague")

    if not report_date:
        latest = await db.pipeline_exclusion_report.find_one(
            {},
            {"_id": 0, "report_date": 1},
            sort=[("report_date", -1), ("created_at", -1)],
        )
        report_date = (
            latest.get("report_date")
            if latest and latest.get("report_date")
            else datetime.now(PRAGUE).strftime("%Y-%m-%d")
        )

    query = {"report_date": report_date}
    if step:
        query["step"] = step

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Ticker", "Name", "Step", "Reason"])

    cursor = db.pipeline_exclusion_report.find(
        query,
        {"_id": 0, "ticker": 1, "name": 1, "step": 1, "reason": 1}
    ).sort([("created_at", -1), ("step", 1), ("ticker", 1)])

    async for doc in cursor:
        writer.writerow([
            doc.get("ticker", ""),
            doc.get("name", ""),
            doc.get("step", ""),
            doc.get("reason", ""),
        ])

    output.seek(0)
    safe_step = (step or "all_steps").replace(" ", "_").replace("/", "_")
    filename = f"pipeline_exclusion_report_{report_date}_{safe_step}.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@api_router.get("/admin/excluded-tickers")
async def admin_get_excluded_tickers(
    reason: str = Query(None, description="Filter by exclusion reason"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """
    Get list of excluded tickers with reasons.
    
    Reasons:
    - NOT_IN_SEED_LIST: Not in NYSE/NASDAQ seed
    - NOT_COMMON_STOCK: asset_type != "Common Stock"
    - NO_PRICE_DATA: No price data in stock_prices
    - MISSING_SECTOR_INDUSTRY: Empty sector or industry
    - DELISTED: Ticker is delisted
    - OTHER: Unknown reason
    """
    query = {}
    if reason:
        query["excluded_reason"] = reason
    
    total = await db.excluded_tickers.count_documents(query)
    
    cursor = db.excluded_tickers.find(
        query,
        {"_id": 0}
    ).sort("excluded_reason", 1).skip(offset).limit(limit)
    
    tickers = await cursor.to_list(length=limit)
    
    # Get counts by reason
    reason_counts = {}
    pipeline = [
        {"$group": {"_id": "$excluded_reason", "count": {"$sum": 1}}}
    ]
    async for doc in db.excluded_tickers.aggregate(pipeline):
        reason_counts[doc["_id"]] = doc["count"]
    
    return {
        "total_excluded": total,
        "by_reason": reason_counts,
        "offset": offset,
        "limit": limit,
        "tickers": tickers,
    }


@api_router.get("/admin/visible-universe/stats")
async def admin_visible_universe_stats():
    """
    Get Visible Universe statistics.
    
    Shows counts for each stage:
    - Total tracked
    - Seeded (NYSE/NASDAQ Common Stock)
    - Has price data
    - Has classification (sector + industry)
    - VISIBLE (all conditions met)
    - Excluded by reason
    """
    # Stage counts
    total_tracked = await db.tracked_tickers.count_documents({})
    seeded = await db.tracked_tickers.count_documents({"is_seeded": True})
    has_price = await db.tracked_tickers.count_documents({"has_price_data": True})
    has_classification = await db.tracked_tickers.count_documents({"has_classification": True})
    visible = await db.tracked_tickers.count_documents(VISIBLE_UNIVERSE_QUERY)
    
    # Excluded counts by reason
    excluded_pipeline = [
        {"$group": {"_id": "$excluded_reason", "count": {"$sum": 1}}}
    ]
    excluded_by_reason = {}
    async for doc in db.excluded_tickers.aggregate(excluded_pipeline):
        excluded_by_reason[doc["_id"]] = doc["count"]
    
    total_excluded = await db.excluded_tickers.count_documents({})
    
    return {
        "stages": {
            "total_tracked": total_tracked,
            "seeded": seeded,
            "has_price_data": has_price,
            "has_classification": has_classification,
            "visible": visible,
        },
        "excluded": {
            "total": total_excluded,
            "by_reason": excluded_by_reason,
        },
        "visible_universe_count": visible,
        "note": "Only tickers with is_visible=true appear in the app",
    }



# ============================================================================
# ADMIN USERS / CUSTOMERS ENDPOINTS
# ============================================================================

@api_router.get("/admin/users")
async def admin_list_users(limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
    """List all users with their stats. Auth: AdminAuthMiddleware."""

    users = await db.users.find({}, {"_id": 0}).skip(offset).limit(limit).to_list(limit)

    result = []
    for user in users:
        user_id = user.get("user_id") or user.get("id") or str(user.get("_id", ""))
        portfolio_count = await db.portfolios.count_documents({"user_id": user_id})
        watchlist_count = await db.user_watchlist.count_documents({"user_id": user_id})
        result.append({
            "user_id": user_id,
            "email": user.get("email"),
            "name": user.get("name"),
            "subscription_tier": user.get("subscription_tier", "free"),
            "is_suspended": user.get("is_suspended", False),
            "created_at": user.get("created_at").isoformat() if user.get("created_at") and hasattr(user.get("created_at"), "isoformat") else user.get("created_at"),
            "last_login": user.get("last_login").isoformat() if user.get("last_login") and hasattr(user.get("last_login"), "isoformat") else user.get("last_login"),
            "portfolio_count": portfolio_count,
            "watchlist_count": watchlist_count,
        })

    total = await db.users.count_documents({})
    return {"users": result, "total": total, "offset": offset, "limit": limit}


@api_router.patch("/admin/users/{user_id}/tier")
async def admin_update_user_tier(user_id: str, request: Request):
    """Change user subscription tier (free <-> pro). Auth: AdminAuthMiddleware."""

    body = await request.json()
    new_tier = body.get("subscription_tier")
    if new_tier not in ["free", "pro", "pro_plus"]:
        raise HTTPException(400, detail={"error": "Invalid tier. Must be free, pro, or pro_plus.", "code": "INVALID_TIER"})

    user = await db.users.find_one({"user_id": user_id}, {"_id": 0})
    if not user:
        raise HTTPException(404, detail={"error": "User not found", "code": "USER_NOT_FOUND"})

    old_tier = user.get("subscription_tier", "free")
    await db.users.update_one({"user_id": user_id}, {"$set": {"subscription_tier": new_tier}})

    from zoneinfo import ZoneInfo
    PRAGUE = ZoneInfo("Europe/Prague")
    admin_user = await db.users.find_one({"user_id": (await db.user_sessions.find_one({"session_token": session_token}) or {}).get("user_id", "")}, {"_id": 0, "email": 1})
    await db.admin_audit_log.insert_one({
        "action": "user.tier_change",
        "performed_by": admin_user.get("email") if admin_user else "admin",
        "target_user_id": user_id,
        "target_email": user.get("email"),
        "before": {"subscription_tier": old_tier},
        "after": {"subscription_tier": new_tier},
        "timestamp": datetime.now(PRAGUE).isoformat(),
        "ip": request.client.host if request.client else None,
    })

    return {"status": "updated", "user_id": user_id, "subscription_tier": new_tier}


@api_router.post("/admin/users/{user_id}/suspend")
async def admin_suspend_user(user_id: str, request: Request):
    """Suspend or unsuspend a user account. Auth: AdminAuthMiddleware."""

    user = await db.users.find_one({"user_id": user_id}, {"_id": 0})
    if not user:
        raise HTTPException(404, detail={"error": "User not found", "code": "USER_NOT_FOUND"})

    current_suspended = user.get("is_suspended", False)
    new_suspended = not current_suspended
    await db.users.update_one({"user_id": user_id}, {"$set": {"is_suspended": new_suspended}})

    from zoneinfo import ZoneInfo
    PRAGUE = ZoneInfo("Europe/Prague")
    action = "user.suspend" if new_suspended else "user.unsuspend"
    await db.admin_audit_log.insert_one({
        "action": action,
        "performed_by": "admin",
        "target_user_id": user_id,
        "target_email": user.get("email"),
        "before": {"is_suspended": current_suspended},
        "after": {"is_suspended": new_suspended},
        "timestamp": datetime.now(PRAGUE).isoformat(),
        "ip": request.client.host if request.client else None,
    })

    return {"status": "suspended" if new_suspended else "unsuspended", "user_id": user_id, "is_suspended": new_suspended}


@api_router.delete("/admin/users/{user_id}")
async def admin_delete_user(user_id: str, request: Request):
    """Permanently delete a user and all their data. Auth: AdminAuthMiddleware."""

    user = await db.users.find_one({"user_id": user_id}, {"_id": 0})
    if not user:
        raise HTTPException(404, detail={"error": "User not found", "code": "USER_NOT_FOUND"})

    if user.get("email") == "kurtarichard@gmail.com":
        raise HTTPException(400, detail={"error": "Cannot delete admin account", "code": "CANNOT_DELETE_ADMIN"})

    # Delete user data
    await db.users.delete_one({"user_id": user_id})
    await db.portfolios.delete_many({"user_id": user_id})
    await db.user_watchlist.delete_many({"user_id": user_id})
    await db.user_sessions.delete_many({"user_id": user_id})

    from zoneinfo import ZoneInfo
    PRAGUE = ZoneInfo("Europe/Prague")
    await db.admin_audit_log.insert_one({
        "action": "user.delete",
        "performed_by": "admin",
        "target_user_id": user_id,
        "target_email": user.get("email"),
        "before": {"subscription_tier": user.get("subscription_tier", "free")},
        "after": None,
        "timestamp": datetime.now(PRAGUE).isoformat(),
        "ip": request.client.host if request.client else None,
    })

    return {"status": "deleted", "user_id": user_id, "email": user.get("email")}


# Include router
app.include_router(api_router)

# Include v1 API routes
from routes.feed_routes import router as feed_router
from routes.talk_routes import router as talk_router
from routes.user_routes import router as user_router

app.include_router(feed_router, prefix="/api")
app.include_router(talk_router, prefix="/api")
app.include_router(user_router, prefix="/api")

# Security: User auth middleware — protects /api/portfolios, /api/positions, /api/v1/watchlist
app.add_middleware(UserAuthMiddleware, db=db)
# Security: Admin auth middleware — protects ALL /api/admin/* endpoints (Zero Trust)
app.add_middleware(AdminAuthMiddleware, db=db)

# CORS (must be registered after AdminAuthMiddleware so it runs first in the chain)
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["https://jocular-faun-27ea7b.netlify.app","https://richstox.com","http://localhost:3000","http://localhost:8081"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    # FORK_RULES loaded — external calls blocked in frontend; user-facing endpoints DB-only
    logger.info("FORK_RULES loaded — external calls blocked in frontend; user-facing endpoints DB-only")
    
    # Store db in app state for access from route handlers
    app.state.db = db
    
    # Create indexes for news and talk collections
    from services.news_service import create_news_indexes
    from services.talk_service import create_indexes as create_talk_indexes
    from services.notification_service import create_indexes as create_notification_indexes
    await create_news_indexes(db)
    await create_talk_indexes(db)
    await create_notification_indexes(db)
    
    # ⚠️ STARTUP VISIBILITY GUARD - Prevents silent data integrity breaches
    await startup_mega_caps_visibility_guard(db)
    
    # ⚠️ P55: STARTUP PAIN CACHE GUARD - Warns if mega-caps missing PAIN data
    await startup_pain_cache_guard(db)


async def startup_mega_caps_visibility_guard(database):
    """
    Soft startup guard for mega-cap visibility.

    This guard must NOT crash startup. In fresh environments (or before the
    pipeline seeds visible tickers), it logs warnings and exits.
    """
    mega_caps = ["AAPL.US", "MSFT.US", "GOOGL.US", "AMZN.US", "NVDA.US"]
    tracked_total = await database.tracked_tickers.count_documents({})
    visible_total = await database.tracked_tickers.count_documents({"is_visible": True})

    # Fresh / empty environment: nothing to validate yet.
    if tracked_total == 0 or visible_total == 0:
        logger.warning(
            "⚠️ Startup mega-cap visibility guard skipped: "
            f"tracked={tracked_total}, visible={visible_total}. "
            "Run pipeline Step 1→2→3→4 first."
        )
        return

    invisible = []
    missing = []

    for ticker in mega_caps:
        doc = await database.tracked_tickers.find_one(
            {"ticker": ticker},
            {"_id": 0, "ticker": 1, "is_visible": 1},
        )
        if not doc:
            missing.append(ticker)
            continue
        if doc.get("is_visible") is not True:
            invisible.append(ticker)

    if invisible:
        logger.error(
            "⚠️ Startup mega-cap visibility check: some mega-caps are invisible: "
            f"{invisible}. App continues to start."
        )
    if missing:
        logger.warning(
            "⚠️ Startup mega-cap visibility check: some mega-caps are missing: "
            f"{missing}. App continues to start."
        )
    if not invisible and not missing:
        logger.info("✅ Startup mega-cap visibility guard passed - mega-caps are visible")


async def startup_pain_cache_guard(database):
    """
    P55 BINDING: Verify that PAIN cache contains top tickers.
    Warns if mega-caps are missing from pain cache.
    
    This guard runs at startup to detect missing PAIN data.
    """
    mega_caps = ["AAPL.US", "MSFT.US", "GOOGL.US", "AMZN.US", "NVDA.US"]
    missing = []
    
    for ticker in mega_caps:
        pain = await database.ticker_pain_cache.find_one(
            {"ticker": ticker},
            {"_id": 0, "ticker": 1, "pain_percentage": 1}
        )
        if not pain:
            missing.append(ticker)
            logger.warning(f"⚠️ PAIN cache missing for {ticker}")
        else:
            logger.info(f"✅ PAIN cache OK: {ticker} = {pain.get('pain_percentage')}%")
    
    if missing:
        logger.error(f"❌ PAIN cache missing for: {missing}. Run pain_cache job!")
    else:
        logger.info("✅ Startup PAIN cache guard passed - mega-caps have PAIN data")


@app.on_event("shutdown")
async def shutdown():
    client.close()
