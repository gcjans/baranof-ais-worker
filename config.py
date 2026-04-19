"""Configuration for the Baranof AIS worker.

All tunables live here so the running behaviour is obvious from one
place.  Environment variables are read at import time.  Anything
sensitive (API key, DATABASE_URL) MUST come from env — never
hard-coded.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass


logger = logging.getLogger(__name__)


# ── Authoritative bounding box (SE Alaska + approaches) ────────────
#
# aisstream.io subscription takes a list of 2-point polygons
# [[sw_lat, sw_lon], [ne_lat, ne_lon]].  We subscribe to a wide box
# so cruise ships appear on the map 12+ hours before they reach
# Ketchikan — captains care about inbound traffic, not just in-port.
#
#   SW corner: 54.00° N, 135.00° W   (south of Dixon Entrance,
#                                     west toward Queen Charlotte)
#   NE corner: 58.00° N, 130.00° W   (north of Juneau, east of KTN)
#
# Override at runtime with AIS_BBOX env var — JSON-encoded same
# shape: '[[54.0,-135.0],[58.0,-130.0]]'.  Useful for dev / testing
# against a narrower region.
DEFAULT_BBOX = [[54.0, -135.0], [58.0, -130.0]]


@dataclass(frozen=True)
class Config:
    """Resolved runtime config.  Immutable so workers can't mutate it
    mid-run and confuse the reconnect logic."""

    api_key: str
    database_url: str
    bbox: list[list[float]]
    # WebSocket reconnect tuning — exponential backoff capped at 5 min.
    reconnect_initial_sec: float
    reconnect_max_sec: float
    # Treat the stream as stalled if no messages arrive for this many
    # seconds.  aisstream publishes heartbeats / traffic continuously
    # in SE Alaska; more than ~2 min of silence means the connection
    # is dead even if the TCP layer hasn't noticed yet.
    stall_timeout_sec: float
    # How often to prune ais_position_history rows older than
    # history_retention_days.  Runs inline in the ingest loop — no
    # separate cron service to manage.
    prune_interval_sec: float
    history_retention_days: int
    # Logging verbosity — "DEBUG" logs every message (flood), "INFO"
    # summarises batches, "WARNING" is quiet-mode for steady state.
    log_level: str


def _parse_bbox(raw: str | None) -> list[list[float]]:
    if not raw:
        return DEFAULT_BBOX
    try:
        parsed = json.loads(raw)
        if (
            isinstance(parsed, list)
            and len(parsed) == 2
            and all(isinstance(p, list) and len(p) == 2 for p in parsed)
            and all(isinstance(v, (int, float)) for p in parsed for v in p)
        ):
            return [[float(parsed[0][0]), float(parsed[0][1])],
                    [float(parsed[1][0]), float(parsed[1][1])]]
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    logger.warning("AIS_BBOX unparseable (%r); falling back to DEFAULT_BBOX", raw)
    return DEFAULT_BBOX


def load_config() -> Config:
    """Load from env.  Raises RuntimeError if required vars are
    missing — the worker refuses to start in that case (vs silently
    connecting with no key)."""
    api_key = os.environ.get("AIS_STREAM_API_KEY", "").strip()
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not api_key:
        raise RuntimeError(
            "AIS_STREAM_API_KEY not set — refusing to start.  "
            "Create a key at https://aisstream.io/apikeys and add "
            "to the Railway service env."
        )
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL not set — refusing to start.  "
            "Share the PostgreSQL URL with the existing dispatch_app service."
        )

    return Config(
        api_key=api_key,
        database_url=database_url,
        bbox=_parse_bbox(os.environ.get("AIS_BBOX")),
        reconnect_initial_sec=float(
            os.environ.get("AIS_RECONNECT_INITIAL_SEC", "2.0")
        ),
        reconnect_max_sec=float(
            os.environ.get("AIS_RECONNECT_MAX_SEC", "300.0")
        ),
        stall_timeout_sec=float(
            os.environ.get("AIS_STALL_TIMEOUT_SEC", "120.0")
        ),
        prune_interval_sec=float(
            os.environ.get("AIS_PRUNE_INTERVAL_SEC", "3600.0")
        ),
        history_retention_days=int(
            os.environ.get("AIS_HISTORY_RETENTION_DAYS", "30")
        ),
        log_level=os.environ.get("AIS_LOG_LEVEL", "INFO").upper(),
    )
