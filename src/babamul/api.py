"""REST API client for Babamul."""

from __future__ import annotations

import base64
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Literal, cast, get_args

import httpx
from astropy.coordinates import SkyCoord
from astropy.table import Table

from .config import get_base_url, get_boom_api_url
from .exceptions import APIAuthenticationError, APIError, APINotFoundError
from .models import (
    AlertCutouts,
    BoomFilter,
    CrossMatches,
    FilterTestCount,
    FilterTestResult,
    LsstAlert,
    ObjectSearchResult,
    ObjPhotometry,
    UserProfile,
    ZtfAlert,
)

logger = logging.getLogger(__name__)

Survey = Literal["ZTF", "LSST"]


def _resolve_token() -> str:
    """Resolve the API token from environment variable.

    Raises
    ------
    APIAuthenticationError
        If no token is found.
    """
    token = os.environ.get("BABAMUL_API_TOKEN")
    if not token:
        raise APIAuthenticationError(
            "No API token provided. Set the BABAMUL_API_TOKEN environment variable.",
            status_code=401,
        )
    return token


def _request(
    method: str,
    endpoint: str,
    *,
    params: dict[str, Any] | None = None,
    json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Make an authenticated HTTP request to the Babamul API.

    Parameters
    ----------
    method : str
        HTTP method (GET, POST, DELETE, …).
    endpoint : str
        API endpoint path (e.g. ``/profile``).
    params : dict | None
        Query parameters.
    json : dict | None
        JSON body for POST requests.

    Returns
    -------
    dict
        Response JSON data.

    Raises
    ------
    APIAuthenticationError
        If authentication is required but no token is set, or auth fails.
    APINotFoundError
        If the requested resource is not found.
    APIError
        For other API errors.
    """
    url = f"{get_base_url()}{endpoint}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_resolve_token()}",
    }

    try:
        response = httpx.request(
            method,
            url,
            params=params,
            json=json,
            headers=headers,
            timeout=30.0,
        )
    except httpx.RequestError as e:
        raise APIError(f"Request failed: {e}") from e

    if response.status_code == 401:
        raise APIAuthenticationError(
            "Authentication failed. Check your credentials.",
            status_code=401,
        )
    if response.status_code == 404:
        raise APINotFoundError(
            f"Resource not found: {endpoint}",
            status_code=404,
        )
    if response.status_code >= 400:
        try:
            error_data = response.json()
            message = error_data.get("message", response.text)
        except Exception:
            message = response.text
        raise APIError(
            f"API error ({response.status_code}): {message}",
            status_code=response.status_code,
        )

    return cast(dict[str, Any], response.json())


def get_alerts(
    survey: Survey,
    *,
    object_id: str | None = None,
    ra: float | None = None,
    dec: float | None = None,
    radius_arcsec: float | None = None,
    start_jd: float | None = None,
    end_jd: float | None = None,
    min_magpsf: float | None = None,
    max_magpsf: float | None = None,
    min_drb: float | None = None,
    max_drb: float | None = None,
    is_rock: bool | None = None,
    is_star: bool | None = None,
    is_near_brightstar: bool | None = None,
    is_stationary: bool | None = None,
) -> list[ZtfAlert | LsstAlert]:
    """Query alerts from the API.

    Parameters
    ----------
    survey : Survey
        The survey to query ("ZTF" or "LSST").
    object_id : str | None
        Filter by object ID.
    ra : float | None
        Right Ascension in degrees (requires *dec* and *radius_arcsec*).
    dec : float | None
        Declination in degrees (requires *ra* and *radius_arcsec*).
    radius_arcsec : float | None
        Cone search radius in arcseconds (max 600).
    start_jd : float | None
        Start Julian Date filter.
    end_jd : float | None
        End Julian Date filter.
    min_magpsf : float | None
        Minimum PSF magnitude filter.
    max_magpsf : float | None
        Maximum PSF magnitude filter.
    min_drb : float | None
        Minimum DRB (reliability) score filter.
    max_drb : float | None
        Maximum DRB score filter.
    is_rock : bool | None
        Filter for likely solar system objects.
    is_star : bool | None
        Filter for likely stellar sources.
    is_near_brightstar : bool | None
        Filter for sources near bright stars.
    is_stationary : bool | None
        Filter for likely stationary sources (not moving).

    Returns
    -------
    list[ZtfAlert | LsstAlert]
        List of alerts matching the query parameters.
    """

    params = {
        key: value
        for key, value in {
            "object_id": object_id,
            "ra": ra,
            "dec": dec,
            "radius_arcsec": radius_arcsec,
            "start_jd": start_jd,
            "end_jd": end_jd,
            "min_magpsf": min_magpsf,
            "max_magpsf": max_magpsf,
            "min_drb": min_drb,
            "max_drb": max_drb,
            "is_rock": is_rock,
            "is_star": is_star,
            "is_near_brightstar": is_near_brightstar,
            "is_stationary": is_stationary,
        }.items()
        if value is not None
    }

    response = _request("GET", f"/surveys/{survey}/alerts", params=params)
    data = response.get("data", [])
    alert_model = ZtfAlert if survey == "ZTF" else LsstAlert
    return [alert_model.model_validate(alert) for alert in data]


def cone_search_alerts(
    survey: Survey,
    coordinates: SkyCoord
    | list[tuple[str, float, float]]
    | list[dict[str, Any]]
    | dict[str, tuple[float, float]]
    | Table,
    radius_arcsec: float,
    *,
    start_jd: float | None = None,
    end_jd: float | None = None,
    min_magpsf: float | None = None,
    max_magpsf: float | None = None,
    min_drb: float | None = None,
    max_drb: float | None = None,
    is_rock: bool | None = None,
    is_star: bool | None = None,
    is_near_brightstar: bool | None = None,
    is_stationary: bool | None = None,
    n_threads: int = 4,
    batch_size: int = 500,
) -> dict[str, list[ZtfAlert | LsstAlert]]:
    """Query alerts from the API.

    Parameters
    ----------
    survey : Survey
        The survey to query ("ZTF" or "LSST").
    coordinates: SkyCoord | list[tuple[str, float, float]] | list[dict[str, Any]]
        Coordinates for the cone search.
    radius_arcsec : float
        Cone search radius in arcseconds (max 600).
    radius_arcsec : float | None
        Cone search radius in arcseconds (max 600).
    start_jd : float | None
        Start Julian Date filter.
    end_jd : float | None
        End Julian Date filter.
    min_magpsf : float | None
        Minimum PSF magnitude filter.
    max_magpsf : float | None
        Maximum PSF magnitude filter.
    min_drb : float | None
        Minimum DRB (reliability) score filter.
    max_drb : float | None
        Maximum DRB score filter.
    is_rock : bool | None
        Filter for likely solar system objects.
    is_star : bool | None
        Filter for likely stellar sources.
    is_near_brightstar : bool | None
        Filter for sources near bright stars.
    is_stationary : bool | None
        Filter for likely stationary sources (not moving).

    Returns
    -------
    list[ZtfAlert | LsstAlert]
        List of alerts matching the query parameters.
    """
    # coordinates can be a SkyCoord (with name), a tuple of (name, ra, dec) or a dict with keys "name", "ra", "dec"
    normalized_coords: dict[str, tuple[float, float]]
    if isinstance(coordinates, SkyCoord):
        if coordinates.isscalar:
            normalized_coords = {
                "coord_0": (
                    float(coordinates.ra.deg),
                    float(coordinates.dec.deg),
                )
            }
        else:
            normalized_coords = {
                f"coord_{i}": (float(coord.ra.deg), float(coord.dec.deg))
                for i, coord in enumerate(coordinates)
            }
    elif isinstance(coordinates, list) and all(
        isinstance(coord, tuple) and len(coord) == 3 for coord in coordinates
    ):
        normalized_coords = {
            name: (float(ra), float(dec)) for name, ra, dec in coordinates
        }
    elif isinstance(coordinates, list) and all(
        isinstance(coord, dict)
        and "name" in coord
        and "ra" in coord
        and "dec" in coord
        for coord in coordinates
    ):
        coord_list = cast(list[dict[str, Any]], coordinates)
        normalized_coords = {
            str(coord["name"]): (float(coord["ra"]), float(coord["dec"]))
            for coord in coord_list
        }
    elif isinstance(coordinates, dict) and all(
        isinstance(coord, tuple) and len(coord) == 2
        for coord in coordinates.values()
    ):
        normalized_coords = {
            k: (float(v[0]), float(v[1])) for k, v in coordinates.items()
        }
    # let's be a little flexible, and allow aliases of "name", "ra", "dec" in the table, as long as we can find them
    elif isinstance(coordinates, Table):
        name_col = next(
            (
                col
                for col in coordinates.colnames
                if col.lower() in ["name", "id", "objname"]
            ),
            None,
        )
        ra_col = next(
            (
                col
                for col in coordinates.colnames
                if col.lower() in ["ra", "ra_deg", "ra_j2000"]
            ),
            None,
        )
        dec_col = next(
            (
                col
                for col in coordinates.colnames
                if col.lower()
                in ["dec", "dec_deg", "dec_j2000", "decl", "declination"]
            ),
            None,
        )
        if name_col and ra_col and dec_col:
            normalized_coords = {
                str(row[name_col]): (float(row[ra_col]), float(row[dec_col]))
                for row in coordinates
            }
        else:
            raise ValueError(
                "Table must have columns for name, ra, and dec (or their aliases)."
            )
    else:
        raise ValueError(
            "Invalid coordinates format. Must be a SkyCoord, list of (name, ra, dec) tuples, or list of dicts with keys 'name', 'ra', 'dec'."
        )

    if batch_size < 1 or batch_size > 5000:
        raise ValueError("Batch size must be between 1 and 5000.")
    if n_threads < 1 or n_threads > 12:
        raise ValueError("Number of threads must be between 1 and 12.")
    if radius_arcsec <= 0 or radius_arcsec > 600:
        raise ValueError("Radius must be between 0 and 600 arcseconds.")

    # we use the /surveys/{survey}/alerts/cone-search endpoint which accepts a list of coordinates as dicts with keys "name", "ra", "dec"
    params = {
        key: value
        for key, value in {
            "radius_arcsec": radius_arcsec,
            "start_jd": start_jd,
            "end_jd": end_jd,
            "min_magpsf": min_magpsf,
            "max_magpsf": max_magpsf,
            "min_drb": min_drb,
            "max_drb": max_drb,
            "is_rock": is_rock,
            "is_star": is_star,
            "is_near_brightstar": is_near_brightstar,
            "is_stationary": is_stationary,
        }.items()
        if value is not None
    }
    # params["coordinates"] = coordinates
    params["radius_arcsec"] = radius_arcsec

    results = {}
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = []
        batch = []
        for i, (name, coords) in enumerate(normalized_coords.items()):
            batch.append((name, coords))
            if len(batch) == batch_size or i == len(normalized_coords) - 1:
                batch_coords = dict(batch)
                batch_params: dict[str, Any] = params.copy()
                batch_params["coordinates"] = batch_coords
                futures.append(
                    executor.submit(
                        _request,
                        "POST",
                        f"/surveys/{survey}/alerts/cone-search",
                        json=batch_params,
                    )
                )
                batch = []

        for future in as_completed(futures):
            try:
                response = future.result()
                data = response.get("data", [])
                alert_model = ZtfAlert if survey == "ZTF" else LsstAlert
                for name, alerts in data.items():
                    results[name] = [
                        alert_model.model_validate(alert) for alert in alerts
                    ]
            except Exception as e:
                logger.error(f"Error processing cone search batch: {e}")

    return results


def cone_search_objects(
    survey: Survey,
    coordinates: SkyCoord
    | list[tuple[str, float, float]]
    | list[dict[str, Any]]
    | dict[str, tuple[float, float]]
    | Table,
    radius_arcsec: float,
    n_threads: int = 4,
    batch_size: int = 500,
) -> dict[str, list[ObjectSearchResult]]:
    """Cone search for objects in the API.

    Parameters
    ----------
    survey : Survey
        The survey to query ("ZTF" or "LSST").
    coordinates: SkyCoord | list[tuple[str, float, float]] | list[dict[str, Any]]
        Coordinates for the cone search.
    radius_arcsec : float
        Cone search radius in arcseconds (max 600).

    Returns
    -------
    dict[str, list[ObjectSearchResult]]
        Dictionary mapping coordinate names to lists of matching objects.
    """
    # we can reuse the same coordinate parsing logic as in cone_search_alerts, since the input format is the same
    normalized_coords: dict[str, tuple[float, float]]
    if isinstance(coordinates, SkyCoord):
        if coordinates.isscalar:
            normalized_coords = {
                "coord_0": (
                    float(coordinates.ra.deg),
                    float(coordinates.dec.deg),
                )
            }
        else:
            normalized_coords = {
                f"coord_{i}": (float(coord.ra.deg), float(coord.dec.deg))
                for i, coord in enumerate(coordinates)
            }
    elif isinstance(coordinates, list) and all(
        isinstance(coord, tuple) and len(coord) == 3 for coord in coordinates
    ):
        normalized_coords = {
            name: (float(ra), float(dec)) for name, ra, dec in coordinates
        }
    elif isinstance(coordinates, list) and all(
        isinstance(coord, dict)
        and "name" in coord
        and "ra" in coord
        and "dec" in coord
        for coord in coordinates
    ):
        coord_list = cast(list[dict[str, Any]], coordinates)
        normalized_coords = {
            str(coord["name"]): (float(coord["ra"]), float(coord["dec"]))
            for coord in coord_list
        }
    elif isinstance(coordinates, dict) and all(
        isinstance(coord, tuple) and len(coord) == 2
        for coord in coordinates.values()
    ):
        normalized_coords = {
            k: (float(v[0]), float(v[1])) for k, v in coordinates.items()
        }
    elif isinstance(coordinates, Table):
        name_col = next(
            (
                col
                for col in coordinates.colnames
                if col.lower() in ["name", "id", "objname"]
            ),
            None,
        )
        ra_col = next(
            (
                col
                for col in coordinates.colnames
                if col.lower() in ["ra", "ra_deg", "ra_j2000"]
            ),
            None,
        )
        dec_col = next(
            (
                col
                for col in coordinates.colnames
                if col.lower()
                in ["dec", "dec_deg", "dec_j2000", "decl", "declination"]
            ),
            None,
        )
        if name_col and ra_col and dec_col:
            normalized_coords = {
                str(row[name_col]): (float(row[ra_col]), float(row[dec_col]))
                for row in coordinates
            }
        else:
            raise ValueError(
                "Table must have columns for name, ra, and dec (or their aliases)."
            )
    else:
        raise ValueError(
            "Invalid coordinates format. Must be a SkyCoord, list of (name, ra, dec) tuples, or list of dicts with keys 'name', 'ra', 'dec'."
        )

    if batch_size < 1 or batch_size > 5000:
        raise ValueError("Batch size must be between 1 and 5000.")
    if n_threads < 1 or n_threads > 12:
        raise ValueError("Number of threads must be between 1 and 12.")
    if radius_arcsec <= 0 or radius_arcsec > 600:
        raise ValueError("Radius must be between 0 and 600 arcseconds.")

    results = {}
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = []
        batch = []
        for i, (name, coords) in enumerate(normalized_coords.items()):
            batch.append((name, coords))
            if len(batch) == batch_size or i == len(normalized_coords) - 1:
                batch_coords = dict(batch)
                batch_params = {
                    "radius_arcsec": radius_arcsec,
                    "coordinates": batch_coords,
                }
                futures.append(
                    executor.submit(
                        _request,
                        "POST",
                        f"/surveys/{survey}/objects/cone-search",
                        json=batch_params,
                    )
                )
                batch = []

        for future in as_completed(futures):
            try:
                response = future.result()
                data = response.get("data", {})
                for name, objects in data.items():
                    results[name] = [
                        ObjectSearchResult.model_validate(obj)
                        for obj in objects
                    ]
            except Exception as e:
                logger.error(f"Error processing cone search batch: {e}")
    return results


def get_cutouts(survey: Survey, candid: int) -> AlertCutouts:
    """Get cutout images for an alert.

    Parameters
    ----------
    survey : Survey
        Survey ("ZTF" or "LSST").
    candid : int
        Candidate ID of the alert.

    Returns
    -------
    AlertCutouts
        Cutout images (science, template, difference) as bytes.
    """
    params = {"candid": candid}
    response = _request("GET", f"/surveys/{survey}/cutouts", params=params)
    data = response.get("data", response)
    return AlertCutouts(
        candid=data["candid"],
        cutoutScience=base64.b64decode(data["cutoutScience"])
        if data.get("cutoutScience")
        else b"",
        cutoutTemplate=base64.b64decode(data["cutoutTemplate"])
        if data.get("cutoutTemplate")
        else b"",
        cutoutDifference=base64.b64decode(data["cutoutDifference"])
        if data.get("cutoutDifference")
        else b"",
    )


def get_object(survey: Survey, object_id: str) -> ZtfAlert | LsstAlert:
    """Get full object details including history and cutouts.
    This returns the complete object with:
    - Candidate information
    - Full photometry history (prv_candidates, prv_nondetections, fp_hists)
    - Cutout images
    - Cross-matches with other surveys

    Parameters
    ----------
    survey : Survey
        Survey ("ZTF" or "LSST").
    object_id : str
        Object ID.

    Returns
    -------
    ZtfAlert | LsstAlert
        Full object with all available data.
    """
    response = _request("GET", f"/surveys/{survey}/objects/{object_id}")
    data = response.get("data", response)

    for key in ["cutoutScience", "cutoutTemplate", "cutoutDifference"]:
        if data.get(key) and isinstance(data[key], str):
            data[key] = base64.b64decode(data[key])

    if survey == "ZTF":
        return ZtfAlert.model_validate(data)
    elif survey == "LSST":
        return LsstAlert.model_validate(data)
    else:
        valid_surveys = ", ".join(get_args(Survey))
        raise ValueError(
            f"Survey {survey} is not supported, must be one of: {valid_surveys}"
        )


def get_photometry(survey: Survey, object_id: str) -> ObjPhotometry:
    """Get photometry history for an object.

    Parameters
    ----------
    survey : Survey
        Survey ("ZTF" or "LSST").
    object_id : str
        Object ID.

    Returns
    -------
    dict[str, Photometry]
        Dictionary containing photometry information, including:
        - prv_candidates: list of previous detections
        - prv_nondetections: list of previous non-detections
        - fp_hists: list of forced photometry measurements
    """
    # TODO: call the dedicated photometry endpoint once it's implemented, instead of fetching the full object
    obj = get_object(survey, object_id)
    return ObjPhotometry(
        objectId=obj.objectId,
        prv_candidates=getattr(obj, "prv_candidates", []),
        prv_nondetections=getattr(obj, "prv_nondetections", []),
        fp_hists=getattr(obj, "fp_hists", []),
    )


def get_cross_matches(survey: Survey, object_id: str) -> CrossMatches:
    """Get cross-matches for an object.

    Parameters
    ----------
    survey : Survey
        Survey ("ZTF" or "LSST").
    object_id : str
        Object ID.

    Returns
    -------
    CrossMatches
        Cross-match information with other archival catalogs (e.g. NED, CatWISE, VSX).
    """
    response = _request(
        "GET", f"/surveys/{survey}/objects/{object_id}/cross-matches"
    )
    data = response.get("data", response)

    return CrossMatches.model_validate(data)


def get_cross_matches_bulk(
    survey: Survey,
    object_ids: list[str],
    n_threads: int = 1,
    batch_size: int = 100,
) -> dict[str, CrossMatches]:
    """Get cross-matches for multiple objects in bulk.

    Parameters
    ----------
    survey : Survey
        Survey ("ZTF" or "LSST").
    object_ids : list[str]
        List of object IDs.

    Returns
    -------
    dict[str, CrossMatches]
        Dictionary mapping object IDs to their cross-match information.
    """
    results = {}
    if n_threads < 1 or n_threads > 12:
        raise ValueError("Number of threads must be between 1 and 12.")
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = []
        for i in range(0, len(object_ids), batch_size):
            batch_ids = object_ids[i : i + batch_size]
            futures.append(
                executor.submit(
                    _request,
                    "POST",
                    f"/surveys/{survey}/objects/cross-matches",
                    json={"object_ids": batch_ids},
                )
            )

        for future in as_completed(futures):
            try:
                response = future.result()
                data = response.get("data", {})
                for obj_id, cm in data.items():
                    results[obj_id] = CrossMatches.model_validate(cm)
            except Exception as e:
                logger.error(f"Error fetching cross-matches batch: {e}")
    return results


def search_objects(
    object_id: str, limit: int = 10
) -> list[ObjectSearchResult]:
    """Search for objects by partial ID.

    Parameters
    ----------
    object_id : str
        Partial object ID to search for.
    limit : int
        Maximum number of results (1–100, default 10).

    Returns
    -------
    list[ObjectSearchResult]
        List of matching objects with basic info.
    """
    response = _request(
        "GET",
        "/objects",
        params={"object_id": object_id, "limit": min(max(1, limit), 100)},
    )
    data = response.get("data", [])
    return [ObjectSearchResult.model_validate(obj) for obj in data]


def get_profile() -> UserProfile:
    """Get the current user's profile.

    Returns
    -------
    UserProfile
        User profile information.
    """
    response = _request("GET", "/profile")
    data = response.get("data", response)
    return UserProfile.model_validate(data)


# ---------------------------------------------------------------------------
# BOOM main API helpers (for filter management)
# ---------------------------------------------------------------------------


def _resolve_boom_token(token: str | None = None) -> str:
    """Resolve a BOOM main API token.

    Priority: explicit token > BOOM_API_TOKEN env var.
    """
    resolved = token or os.environ.get("BOOM_API_TOKEN")
    if not resolved:
        raise APIAuthenticationError(
            "No BOOM API token provided. Either pass a token from login(), "
            "or set the BOOM_API_TOKEN environment variable.",
            status_code=401,
        )
    return resolved


def _boom_request(
    method: str,
    endpoint: str,
    *,
    token: str | None = None,
    params: dict[str, Any] | None = None,
    json: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Make an authenticated request to the BOOM main API.

    Parameters
    ----------
    method : str
        HTTP method.
    endpoint : str
        API endpoint path (e.g. ``/filters``).
    token : str | None
        Bearer token. If None, resolved from env.
    params : dict | None
        Query parameters.
    json : dict | None
        JSON body.
    data : dict | None
        Form data (used for /auth login).
    """
    url = f"{get_boom_api_url()}{endpoint}"
    headers: dict[str, str] = {}

    # /auth endpoint uses form-encoded data, no bearer token needed
    if endpoint != "/auth":
        resolved_token = _resolve_boom_token(token)
        headers["Authorization"] = f"Bearer {resolved_token}"
        headers["Content-Type"] = "application/json"

    try:
        response = httpx.request(
            method,
            url,
            params=params,
            json=json,
            data=data,
            headers=headers,
            timeout=60.0,
        )
    except httpx.RequestError as e:
        raise APIError(f"BOOM API request failed: {e}") from e

    if response.status_code == 401:
        raise APIAuthenticationError(
            "BOOM API authentication failed. Check your token or credentials.",
            status_code=401,
        )
    if response.status_code == 404:
        raise APINotFoundError(
            f"Resource not found: {endpoint}",
            status_code=404,
        )
    if response.status_code >= 400:
        try:
            error_data = response.json()
            message = error_data.get("message", error_data.get("error_description", response.text))
        except Exception:
            message = response.text
        raise APIError(
            f"BOOM API error ({response.status_code}): {message}",
            status_code=response.status_code,
        )

    return cast(dict[str, Any], response.json())


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


def login(
    username: str,
    password: str,
    base_url: str | None = None,
) -> str:
    """Authenticate with the BOOM API and return a JWT token.

    Parameters
    ----------
    username : str
        BOOM admin username.
    password : str
        BOOM admin password.
    base_url : str | None
        Override the BOOM API URL for this call.

    Returns
    -------
    str
        JWT access token for subsequent API calls.
    """
    url = base_url.rstrip("/") if base_url else get_boom_api_url()
    try:
        response = httpx.post(
            f"{url}/auth",
            data={"username": username, "password": password},
            timeout=30.0,
        )
    except httpx.RequestError as e:
        raise APIError(f"Login request failed: {e}") from e

    if response.status_code == 401:
        raise APIAuthenticationError(
            "Login failed: invalid username or password.",
            status_code=401,
        )
    if response.status_code >= 400:
        raise APIError(
            f"Login error ({response.status_code}): {response.text}",
            status_code=response.status_code,
        )

    result = response.json()
    return cast(str, result["access_token"])


# ---------------------------------------------------------------------------
# Filter CRUD
# ---------------------------------------------------------------------------


def get_filters(token: str | None = None) -> list[BoomFilter]:
    """List all filters for the authenticated user.

    Parameters
    ----------
    token : str | None
        BOOM API token. Falls back to BOOM_API_TOKEN env var.

    Returns
    -------
    list[BoomFilter]
        List of the user's saved filters.
    """
    response = _boom_request("GET", "/filters", token=token)
    data = response.get("data", [])
    return [BoomFilter.model_validate(f) for f in data]


def get_filter(filter_id: str, token: str | None = None) -> BoomFilter:
    """Get a single filter by ID.

    Parameters
    ----------
    filter_id : str
        The filter UUID.
    token : str | None
        BOOM API token.

    Returns
    -------
    BoomFilter
        The requested filter.
    """
    response = _boom_request("GET", f"/filters/{filter_id}", token=token)
    data = response.get("data", response)
    return BoomFilter.model_validate(data)


def create_filter(
    name: str,
    pipeline: list[dict[str, Any]],
    survey: Survey,
    permissions: dict[str, list[int]],
    *,
    description: str | None = None,
    token: str | None = None,
) -> BoomFilter:
    """Create a new filter.

    Parameters
    ----------
    name : str
        Human-readable filter name.
    pipeline : list[dict]
        MongoDB aggregation pipeline stages.
    survey : Survey
        Target survey ("ZTF" or "LSST").
    permissions : dict
        Survey permission mapping (e.g. {"ZTF": [1, 2, 3]}).
    description : str | None
        Optional description.
    token : str | None
        BOOM API token.

    Returns
    -------
    BoomFilter
        The newly created filter.
    """
    body: dict[str, Any] = {
        "name": name,
        "pipeline": pipeline,
        "survey": survey,
        "permissions": permissions,
    }
    if description is not None:
        body["description"] = description

    response = _boom_request("POST", "/filters", json=body, token=token)
    data = response.get("data", response)
    return BoomFilter.model_validate(data)


def update_filter(
    filter_id: str,
    *,
    name: str | None = None,
    description: str | None = None,
    active: bool | None = None,
    active_fid: str | None = None,
    permissions: dict[str, list[int]] | None = None,
    token: str | None = None,
) -> None:
    """Update a filter's metadata.

    Parameters
    ----------
    filter_id : str
        The filter UUID.
    name : str | None
        New name.
    description : str | None
        New description.
    active : bool | None
        Enable or disable the filter.
    active_fid : str | None
        Set the active pipeline version.
    permissions : dict | None
        Updated permissions.
    token : str | None
        BOOM API token.
    """
    body: dict[str, Any] = {}
    if name is not None:
        body["name"] = name
    if description is not None:
        body["description"] = description
    if active is not None:
        body["active"] = active
    if active_fid is not None:
        body["active_fid"] = active_fid
    if permissions is not None:
        body["permissions"] = permissions

    _boom_request("PATCH", f"/filters/{filter_id}", json=body, token=token)


def add_filter_version(
    filter_id: str,
    pipeline: list[dict[str, Any]],
    *,
    changelog: str | None = None,
    set_as_active: bool = True,
    token: str | None = None,
) -> str:
    """Add a new pipeline version to an existing filter.

    Parameters
    ----------
    filter_id : str
        The filter UUID.
    pipeline : list[dict]
        New MongoDB aggregation pipeline.
    changelog : str | None
        Description of what changed.
    set_as_active : bool
        Whether to make this the active version (default True).
    token : str | None
        BOOM API token.

    Returns
    -------
    str
        The new filter version ID (fid).
    """
    body: dict[str, Any] = {
        "pipeline": pipeline,
        "set_as_active": set_as_active,
    }
    if changelog is not None:
        body["changelog"] = changelog

    response = _boom_request(
        "POST", f"/filters/{filter_id}/versions", json=body, token=token
    )
    data = response.get("data", response)
    return cast(str, data["fid"])


# ---------------------------------------------------------------------------
# Filter testing
# ---------------------------------------------------------------------------


def test_filter(
    pipeline: list[dict[str, Any]],
    survey: Survey,
    permissions: dict[str, list[int]],
    *,
    start_jd: float | None = None,
    end_jd: float | None = None,
    object_ids: list[str] | None = None,
    candids: list[str] | None = None,
    sort_by: str | None = None,
    sort_order: str | None = None,
    limit: int | None = None,
    token: str | None = None,
) -> FilterTestResult:
    """Test a filter pipeline against real alert data.

    At least one of (start_jd + end_jd), object_ids, or candids must be
    provided to scope the test.

    Parameters
    ----------
    pipeline : list[dict]
        MongoDB aggregation pipeline stages.
    survey : Survey
        Target survey.
    permissions : dict
        Survey permissions.
    start_jd : float | None
        Start Julian Date.
    end_jd : float | None
        End Julian Date (max 7 JD window).
    object_ids : list[str] | None
        Filter to specific object IDs (max 1000).
    candids : list[str] | None
        Filter to specific candidate IDs (max 100000).
    sort_by : str | None
        Field to sort results by.
    sort_order : str | None
        "ascending" or "descending".
    limit : int | None
        Max number of results.
    token : str | None
        BOOM API token.

    Returns
    -------
    FilterTestResult
        The compiled pipeline and matching alert documents.
    """
    body: dict[str, Any] = {
        "pipeline": pipeline,
        "survey": survey,
        "permissions": permissions,
    }
    if start_jd is not None:
        body["start_jd"] = start_jd
    if end_jd is not None:
        body["end_jd"] = end_jd
    if object_ids is not None:
        body["object_ids"] = object_ids
    if candids is not None:
        body["candids"] = candids
    if sort_by is not None:
        body["sort_by"] = sort_by
    if sort_order is not None:
        body["sort_order"] = sort_order
    if limit is not None:
        body["limit"] = limit

    response = _boom_request("POST", "/filters/test", json=body, token=token)
    data = response.get("data", response)
    return FilterTestResult.model_validate(data)


def test_filter_count(
    pipeline: list[dict[str, Any]],
    survey: Survey,
    permissions: dict[str, list[int]],
    *,
    start_jd: float | None = None,
    end_jd: float | None = None,
    object_ids: list[str] | None = None,
    candids: list[str] | None = None,
    token: str | None = None,
) -> FilterTestCount:
    """Count how many alerts match a filter pipeline.

    Parameters
    ----------
    pipeline : list[dict]
        MongoDB aggregation pipeline stages.
    survey : Survey
        Target survey.
    permissions : dict
        Survey permissions.
    start_jd : float | None
        Start Julian Date.
    end_jd : float | None
        End Julian Date (max 7 JD window).
    object_ids : list[str] | None
        Filter to specific object IDs.
    candids : list[str] | None
        Filter to specific candidate IDs.
    token : str | None
        BOOM API token.

    Returns
    -------
    FilterTestCount
        Count and the compiled pipeline.
    """
    body: dict[str, Any] = {
        "pipeline": pipeline,
        "survey": survey,
        "permissions": permissions,
    }
    if start_jd is not None:
        body["start_jd"] = start_jd
    if end_jd is not None:
        body["end_jd"] = end_jd
    if object_ids is not None:
        body["object_ids"] = object_ids
    if candids is not None:
        body["candids"] = candids

    response = _boom_request(
        "POST", "/filters/test/count", json=body, token=token
    )
    data = response.get("data", response)
    return FilterTestCount.model_validate(data)


def get_filter_schema(
    survey: Survey,
    token: str | None = None,
) -> dict[str, Any]:
    """Get the Avro schema of fields available at filtering time.

    This shows all the fields that can be used in filter pipelines
    (candidate, classifications, properties, coordinates, etc.).

    Parameters
    ----------
    survey : Survey
        Target survey ("ZTF" or "LSST").
    token : str | None
        BOOM API token.

    Returns
    -------
    dict
        Avro schema describing filterable fields.
    """
    response = _boom_request(
        "GET", f"/filters/schemas/{survey}", token=token
    )
    return response.get("data", response)
