# ==============================================================================
# RICHSTOX Configuration Module
# ==============================================================================
# BINDING: All services MUST import DB settings from here.
# No hardcoded DB names allowed elsewhere.
# ==============================================================================
import os
import logging

logger = logging.getLogger("richstox.config")

# =============================================================================
# ENVIRONMENT (evaluated at import time, but .env should be loaded first)
# =============================================================================
def get_env() -> str:
    """Get current environment."""
    return os.environ.get('ENV', 'development')

# =============================================================================
# DATABASE (REQUIRED - no defaults for production safety)
# =============================================================================
def get_mongo_url() -> str:
    """Get MongoDB URL. Required - no default."""
    url = os.environ.get('MONGO_URL')
    if not url:
        raise RuntimeError("MONGO_URL environment variable is required")
    return url

def get_db_name() -> str:
    """Get DB name. Required - no default for production safety."""
    db_name = os.environ.get('DB_NAME')
    if not db_name:
        raise RuntimeError("DB_NAME environment variable is required")
    return db_name

def validate_env_db_match() -> None:
    """
    BINDING: Server MUST NOT start with mismatched ENV/DB_NAME.
    
    Rules:
    - ENV=production MUST use DB_NAME containing 'prod'
    - ENV=production MUST NOT use DB_NAME containing 'test', 'dev', 'stage'
    - ENV!=production MUST NOT use DB_NAME containing 'prod'
    
    Raises:
        RuntimeError: If ENV/DB_NAME mismatch detected
    """
    env = os.environ.get('ENV', 'development').lower()
    db_name = os.environ.get('DB_NAME', '').lower()
    
    if not db_name:
        raise RuntimeError("DB_NAME environment variable is required")
    
    is_prod_env = env == 'production'
    is_prod_db = 'prod' in db_name
    is_test_db = any(x in db_name for x in ['test', 'dev', 'stage'])
    
    if is_prod_env and not is_prod_db:
        raise RuntimeError(
            f"🚨 ENV=production but DB_NAME={db_name} (must contain 'prod'). "
            f"Refusing to start."
        )
    
    if is_prod_env and is_test_db:
        raise RuntimeError(
            f"🚨 ENV=production but DB_NAME={db_name} contains test/dev/stage. "
            f"Refusing to start."
        )
    
    if not is_prod_env and is_prod_db:
        raise RuntimeError(
            f"🚨 ENV={env} but DB_NAME={db_name} contains 'prod'. "
            f"Non-production environment must not use production DB. Refusing to start."
        )
    
    # Log success (logger may not be configured yet, so use print as fallback)
    msg = f"✅ ENV/DB Guard: ENV={env}, DB_NAME={db_name}"
    try:
        logger.info(msg)
    except:
        print(msg)

def get_db_host() -> str:
    """Extract host from MONGO_URL for display purposes."""
    url = os.environ.get('MONGO_URL', 'unknown')
    try:
        # Handle mongodb://user:pass@host:port/db format
        if '@' in url:
            host_part = url.split('@')[-1]
        else:
            host_part = url.replace('mongodb://', '')
        return host_part.split('/')[0]
    except:
        return 'unknown'

# Backwards compatibility - these are evaluated when module loads
# (after .env is loaded by server.py)
ENV = property(lambda self: get_env())

# =============================================================================
# EODHD API
# =============================================================================
EODHD_API_KEY = os.environ.get('EODHD_API_KEY', '')
EODHD_BASE_URL = "https://eodhd.com/api"

# =============================================================================
# FEATURE FLAGS
# =============================================================================
DEV_LOGIN_ENABLED = os.environ.get('DEV_LOGIN_ENABLED', '0') == '1'
