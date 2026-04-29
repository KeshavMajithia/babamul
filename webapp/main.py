"""BOOM Filter UI — FastAPI backend.

Proxies filter API calls to the BOOM main API so the browser never
needs the admin JWT.  Serves a simple static frontend.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger(__name__)

BOOM_API_URL = os.environ.get(
    "BOOM_API_BASE_URL", "https://boom-api.nrp-nautilus.io/api"
).rstrip("/")
BOOM_USERNAME = os.environ.get("BOOM_USERNAME", "admin")
BOOM_PASSWORD = os.environ.get("BOOM_PASSWORD", "")

app = FastAPI(title="BOOM Filter UI", docs_url="/api/docs")

# Cached JWT token (refreshed on 401)
_cached_token: str | None = None


async def _get_token() -> str:
    """Get or refresh the BOOM API JWT token."""
    global _cached_token
    if _cached_token:
        return _cached_token

    if not BOOM_PASSWORD:
        raise HTTPException(
            status_code=500,
            detail="BOOM_PASSWORD not set. Cannot authenticate with BOOM API.",
        )

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{BOOM_API_URL}/auth",
            data={"username": BOOM_USERNAME, "password": BOOM_PASSWORD},
        )
    if resp.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to authenticate with BOOM API: {resp.text}",
        )
    _cached_token = resp.json()["access_token"]
    return _cached_token  # type: ignore[return-value]


async def _boom_request(
    method: str,
    endpoint: str,
    json_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Make an authenticated request to the BOOM API, retrying once on 401."""
    global _cached_token

    for attempt in range(2):
        token = await _get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.request(
                method,
                f"{BOOM_API_URL}{endpoint}",
                json=json_body,
                headers=headers,
            )

        if resp.status_code == 401 and attempt == 0:
            _cached_token = None
            continue

        if resp.status_code >= 400:
            raise HTTPException(
                status_code=resp.status_code,
                detail=resp.text,
            )
        return resp.json()  # type: ignore[no-any-return]

    raise HTTPException(status_code=502, detail="BOOM API authentication failed after retry")


# ---- API routes ----


@app.get("/api/schemas/{survey}")
async def get_schema(survey: str) -> dict[str, Any]:
    """Get the filter schema for a survey."""
    return await _boom_request("GET", f"/filters/schemas/{survey}")


@app.post("/api/filters/test")
async def test_filter(request: Request) -> dict[str, Any]:
    """Test a filter pipeline against real alert data."""
    body = await request.json()
    return await _boom_request("POST", "/filters/test", json_body=body)


@app.post("/api/filters/test/count")
async def test_filter_count(request: Request) -> dict[str, Any]:
    """Count alerts matching a filter pipeline."""
    body = await request.json()
    return await _boom_request("POST", "/filters/test/count", json_body=body)


@app.get("/api/filters")
async def list_filters() -> dict[str, Any]:
    """List all saved filters."""
    return await _boom_request("GET", "/filters")


@app.post("/api/filters")
async def create_filter(request: Request) -> dict[str, Any]:
    """Create a new filter."""
    body = await request.json()
    return await _boom_request("POST", "/filters", json_body=body)


@app.get("/api/filters/{filter_id}")
async def get_filter(filter_id: str) -> dict[str, Any]:
    """Get a single filter."""
    return await _boom_request("GET", f"/filters/{filter_id}")


@app.patch("/api/filters/{filter_id}")
async def update_filter(filter_id: str, request: Request) -> dict[str, Any]:
    """Update a filter."""
    body = await request.json()
    return await _boom_request("PATCH", f"/filters/{filter_id}", json_body=body)


@app.post("/api/filters/{filter_id}/versions")
async def add_filter_version(filter_id: str, request: Request) -> dict[str, Any]:
    """Add a new version to a filter."""
    body = await request.json()
    return await _boom_request(
        "POST", f"/filters/{filter_id}/versions", json_body=body
    )


# Serve static files (must come after API routes)
app.mount(
    "/",
    StaticFiles(directory="static", html=True),
    name="static",
)
