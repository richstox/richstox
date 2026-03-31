"""Redaction helpers for secret-bearing values in log output."""

import re

# ---------------------------------------------------------------------------
# Secret-bearing parameter/field names (case-insensitive)
# ---------------------------------------------------------------------------
_SECRET_NAMES = (
    "api_token",
    "token",
    "access_token",
    "key",
    "apiKey",
    "apikey",
    "secret",
    "client_secret",
    "signature",
)

# name=value  /  name: value  forms  (query-string & header-style)
# Captures: group(1)=name, group(2)=value
_SECRET_KV_RE = re.compile(
    r"(?i)\b("
    + "|".join(re.escape(n) for n in _SECRET_NAMES)
    + r")([=:]\s*)([^\s&]+)",
)

# Header patterns  (Authorization, Cookie, Set-Cookie, X-API-Key)
_HEADER_RES = [
    re.compile(r"(?i)(Authorization:\s*Bearer\s+)\S+"),
    re.compile(r"(?i)(Authorization:\s*token\s+)\S+"),
    re.compile(r"(?i)(Cookie:\s*)\S+"),
    re.compile(r"(?i)(Set-Cookie:\s*)\S+"),
    re.compile(r"(?i)(X-API-Key:\s*)\S+"),
]

# JWT-like: three base64url segments separated by dots
# Each segment: at least 4 chars of [A-Za-z0-9_-], optionally ending with =
_JWT_RE = re.compile(
    r"\b[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}=*\b"
)


def redact_secrets(text: str) -> str:
    """Redact secret values from *text* while preserving structure.

    Designed to be called on combined stdout+stderr output before it is
    logged, returned, or stored.  Keeps parameter/header names, host+path,
    HTTP methods, and stack traces intact.
    """
    if not text:
        return text

    # 1. JWT-like tokens  (do first so partial matches aren't eaten by KV)
    text = _JWT_RE.sub("[REDACTED_JWT]", text)

    # 2. Header patterns
    for hdr_re in _HEADER_RES:
        text = hdr_re.sub(r"\g<1>[REDACTED]", text)

    # 3. Secret-bearing key=value / key: value
    text = _SECRET_KV_RE.sub(r"\1\2[REDACTED]", text)

    return text
