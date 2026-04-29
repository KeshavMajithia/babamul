"""Babamul: A Python client for consuming ZTF/LSST alerts from Babamul Kafka
streams and interacting with the Babamul API.
"""

from . import api, topics
from .api import (
    add_filter_version,
    create_filter,
    get_alerts,
    get_cutouts,
    get_filter,
    get_filter_schema,
    get_filters,
    get_object,
    get_profile,
    login,
    search_objects,
    test_filter,
    test_filter_count,
    update_filter,
)
from .consumer import AlertConsumer
from .exceptions import (
    APIAuthenticationError,
    APIError,
    APINotFoundError,
    AuthenticationError,
    BabamulConnectionError,
    BabamulError,
    ConfigurationError,
    DeserializationError,
)
from .models import (
    BoomFilter,
    FilterTestCount,
    FilterTestResult,
    FilterVersion,
    LsstAlert,
    LsstCandidate,
    ZtfAlert,
    ZtfCandidate,
    add_cross_matches,
)

__all__ = [
    "api",
    "topics",
    "add_filter_version",
    "create_filter",
    "get_alerts",
    "get_cutouts",
    "get_filter",
    "get_filter_schema",
    "get_filters",
    "get_object",
    "get_profile",
    "login",
    "search_objects",
    "test_filter",
    "test_filter_count",
    "update_filter",
    "AlertConsumer",
    "APIAuthenticationError",
    "APIError",
    "APINotFoundError",
    "AuthenticationError",
    "BabamulConnectionError",
    "BabamulError",
    "ConfigurationError",
    "DeserializationError",
    "BoomFilter",
    "FilterTestCount",
    "FilterTestResult",
    "FilterVersion",
    "LsstAlert",
    "LsstCandidate",
    "ZtfAlert",
    "ZtfCandidate",
    "add_cross_matches",
]


try:
    from ._version import __version__
except ImportError:
    __version__ = "0.0.0+unknown"
