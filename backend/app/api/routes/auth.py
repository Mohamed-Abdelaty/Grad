"""
app/api/routes/auth.py
----------------------
Authentication routes for the platform.
"""

from collections import defaultdict
import time
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordRequestForm
from typing import Dict, Any, List, Tuple

from app.core.security import create_access_token
from app.core.config import get_settings
from app.api.deps import get_current_user

router = APIRouter(tags=["auth"])

# In-memory store for failed login timestamps: (client_ip, username) -> [timestamps]
_FAILED_ATTEMPTS: Dict[Tuple[str, str], List[float]] = defaultdict(list)
_WINDOW_SECONDS = 60
_MAX_ATTEMPTS = 5


def reset_login_rate_limits() -> None:
    """Helper to reset in-memory rate limiting state during testing."""
    _FAILED_ATTEMPTS.clear()


@router.post("/api/auth/login")
async def login(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
) -> Dict[str, Any]:
    """
    Authenticate user and return a JWT access token.
    Enforces rate limiting: maximum 5 failed attempts per 60 seconds per (client_ip, username).
    """
    client_ip = (
        request.headers.get("x-forwarded-for", "").split(",")[0].strip()
        or request.headers.get("x-real-ip")
        or (request.client.host if request.client else "unknown")
    )
    key = (client_ip, form_data.username)
    now = time.time()

    # Clean up attempts outside sliding window
    _FAILED_ATTEMPTS[key] = [t for t in _FAILED_ATTEMPTS[key] if now - t < _WINDOW_SECONDS]

    # Check if rate limit exceeded
    if len(_FAILED_ATTEMPTS[key]) >= _MAX_ATTEMPTS:
        earliest = _FAILED_ATTEMPTS[key][0]
        retry_after = max(1, int(_WINDOW_SECONDS - (now - earliest)))
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too many failed login attempts. Please try again in {retry_after} seconds.",
            headers={"Retry-After": str(retry_after)},
        )

    settings = get_settings()
    # Check credentials
    if form_data.username != settings.ADMIN_USERNAME or form_data.password != settings.ADMIN_PASSWORD:
        _FAILED_ATTEMPTS[key].append(now)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Success: clear recorded failed attempts for this key
    _FAILED_ATTEMPTS.pop(key, None)
    access_token = create_access_token(data={"sub": form_data.username})
    return {"access_token": access_token, "token_type": "bearer"}

@router.get("/api/auth/me")
async def get_me(username: str = Depends(get_current_user)) -> Dict[str, str]:
    """Return the current authenticated user's details."""
    return {"username": username, "role": "admin"}

@router.post("/api/auth/refresh")
async def refresh_token(username: str = Depends(get_current_user)) -> Dict[str, Any]:
    """Refresh the current access token for keep-alive."""
    access_token = create_access_token(data={"sub": username})
    return {"access_token": access_token, "token_type": "bearer"}
