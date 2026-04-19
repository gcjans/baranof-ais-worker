"""Parse aisstream.io WebSocket messages and write to PostgreSQL.

Two message types we care about (others are silently discarded):

  * PositionReport — upsert `ais_vessels` (position fields +
    last_position_utc), and append a row to `ais_position_history`.
    The history-append is cheap — a single row per message — and
    lets us draw tracks later without having to buy archive access.

  * ShipStaticData — upsert `ais_vessels` (name, call sign, ship
    type, dimensions, destination + last_static_utc).  Classification
    runs each time so the vessel_kind catches up once length arrives.

Schema is owned by dispatch_app/dispatch_tool/database.py — this
file only ever writes, never creates tables.

All writes use psycopg2's native connection pooling via a single
long-lived connection per worker instance.  No threading — the
ingest loop is serial.  If throughput ever becomes an issue we can
switch to COPY-based batch writes, but at SE-Alaska traffic volumes
(~thousands of messages/hour) straight UPSERT is fine.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import psycopg2
import psycopg2.extensions

from classify import classify_vessel


logger = logging.getLogger(__name__)


# ── Connection management ─────────────────────────────────────────
# One persistent connection for the worker.  Reconnect on loss
# rather than opening a new connection per message.

def connect(database_url: str) -> psycopg2.extensions.connection:
    """Open a new PostgreSQL connection with autocommit enabled.

    We use autocommit because each message is its own logical
    transaction — there's nothing to batch or roll back.  Autocommit
    avoids accumulating uncommitted state if the process dies
    mid-write.
    """
    conn = psycopg2.connect(database_url)
    conn.set_session(autocommit=True)
    return conn


# ── Message parsing ───────────────────────────────────────────────

def _parse_timestamp(raw: Optional[str]) -> Optional[datetime]:
    """aisstream reports UTC in ISO-8601 with trailing `+0000 UTC` or
    `Z`.  Normalise to a timezone-aware datetime."""
    if not raw:
        return None
    # "2022-12-29 18:22:32.318353 +0000 UTC" — strip the trailing tz name
    s = raw.strip()
    if s.endswith(" UTC"):
        s = s[:-4]
    # Replace space-offset with standard "+00:00" so fromisoformat works.
    s = s.replace(" +0000", "+00:00")
    # Python's fromisoformat tolerates "T" and " " separators; ensure one.
    if " " in s and "T" not in s:
        s = s.replace(" ", "T", 1)
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _mmsi(msg: dict[str, Any]) -> Optional[int]:
    """Pull MMSI from either the Metadata block or the inner Message."""
    meta = msg.get("Metadata") or {}
    mmsi = meta.get("MMSI")
    if isinstance(mmsi, int) and mmsi > 0:
        return mmsi
    # PositionReport / StaticDataReport carry UserID = MMSI
    inner = msg.get("Message") or {}
    for key in ("PositionReport", "ShipStaticData", "StandardClassBPositionReport"):
        block = inner.get(key)
        if isinstance(block, dict):
            uid = block.get("UserID")
            if isinstance(uid, int) and uid > 0:
                return uid
    return None


def _position_fields(msg: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Extract position fields from a PositionReport message.  Returns
    None if the message isn't a position report or lacks valid coords.
    """
    inner = msg.get("Message") or {}
    pos = inner.get("PositionReport") or inner.get(
        "StandardClassBPositionReport"
    )
    if not isinstance(pos, dict):
        return None
    lat = pos.get("Latitude")
    lon = pos.get("Longitude")
    if lat is None or lon is None:
        return None
    # aisstream occasionally emits 91.0 / 181.0 sentinel values for
    # "not available" — skip those to keep the DB clean.
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    meta = msg.get("Metadata") or {}
    return {
        "latitude": float(lat),
        "longitude": float(lon),
        "cog": _opt_float(pos.get("Cog")),
        "sog": _opt_float(pos.get("Sog")),
        "true_heading": _opt_int(pos.get("TrueHeading")),
        "nav_status": _opt_int(pos.get("NavigationalStatus")),
        "ts": _parse_timestamp(meta.get("time_utc")),
        "ship_name_hint": _clean_str(meta.get("ShipName")),
    }


def _static_fields(msg: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Extract fields from a ShipStaticData message."""
    inner = msg.get("Message") or {}
    static = inner.get("ShipStaticData")
    if not isinstance(static, dict):
        return None
    # Dimension is delivered as {A, B, C, D} where length = A+B, width = C+D.
    dim = static.get("Dimension") or {}
    length = None
    width = None
    if isinstance(dim, dict):
        a = _opt_int(dim.get("A"))
        b = _opt_int(dim.get("B"))
        c = _opt_int(dim.get("C"))
        d = _opt_int(dim.get("D"))
        if a is not None and b is not None:
            length = a + b
        if c is not None and d is not None:
            width = c + d
    meta = msg.get("Metadata") or {}
    return {
        "ship_name": _clean_str(static.get("Name")),
        "call_sign": _clean_str(static.get("CallSign")),
        "ship_type": _opt_int(static.get("Type")),
        "length_m": length,
        "width_m": width,
        "destination": _clean_str(static.get("Destination")),
        "ts": _parse_timestamp(meta.get("time_utc")),
    }


# ── Field converters — AIS JSON fields come through with varying
#    shapes depending on the gateway.  Coerce defensively. ─────────

def _opt_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        i = int(v)
    except (TypeError, ValueError):
        return None
    # 1023 = "heading not available" in the AIS spec — treat as None
    # so downstream rendering doesn't draw a bogus arrow due north.
    if i in (511, 1023):
        return None
    return i


def _opt_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    # COG 360 or SOG 102.3 are "not available" sentinels per spec.
    return f


def _clean_str(v: Any) -> Optional[str]:
    if not isinstance(v, str):
        return None
    # AIS strings are ASCII padded with '@' nulls (ITU-R M.1371).
    s = v.replace("@", "").strip()
    return s or None


# ── Writes ────────────────────────────────────────────────────────

_UPSERT_POSITION = """
    INSERT INTO ais_vessels (
        mmsi, ship_name, latitude, longitude, cog, sog,
        true_heading, nav_status, last_position_utc,
        first_seen_utc, updated_at
    ) VALUES (
        %(mmsi)s, %(ship_name)s, %(latitude)s, %(longitude)s, %(cog)s, %(sog)s,
        %(true_heading)s, %(nav_status)s, %(ts)s,
        %(ts)s, NOW()
    )
    ON CONFLICT (mmsi) DO UPDATE SET
        ship_name = COALESCE(EXCLUDED.ship_name, ais_vessels.ship_name),
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        cog = EXCLUDED.cog,
        sog = EXCLUDED.sog,
        true_heading = EXCLUDED.true_heading,
        nav_status = EXCLUDED.nav_status,
        last_position_utc = EXCLUDED.last_position_utc,
        updated_at = NOW()
    WHERE ais_vessels.last_position_utc IS NULL
       OR ais_vessels.last_position_utc < EXCLUDED.last_position_utc
"""

_APPEND_HISTORY = """
    INSERT INTO ais_position_history (
        mmsi, ts_utc, latitude, longitude, cog, sog
    ) VALUES (
        %(mmsi)s, %(ts)s, %(latitude)s, %(longitude)s, %(cog)s, %(sog)s
    )
    ON CONFLICT (mmsi, ts_utc) DO NOTHING
"""

_UPSERT_STATIC = """
    INSERT INTO ais_vessels (
        mmsi, ship_name, call_sign, ship_type, vessel_kind,
        length_m, width_m, destination,
        last_static_utc, first_seen_utc, updated_at
    ) VALUES (
        %(mmsi)s, %(ship_name)s, %(call_sign)s, %(ship_type)s, %(vessel_kind)s,
        %(length_m)s, %(width_m)s, %(destination)s,
        %(ts)s, %(ts)s, NOW()
    )
    ON CONFLICT (mmsi) DO UPDATE SET
        ship_name = COALESCE(EXCLUDED.ship_name, ais_vessels.ship_name),
        call_sign = COALESCE(EXCLUDED.call_sign, ais_vessels.call_sign),
        ship_type = COALESCE(EXCLUDED.ship_type, ais_vessels.ship_type),
        vessel_kind = EXCLUDED.vessel_kind,
        length_m = COALESCE(EXCLUDED.length_m, ais_vessels.length_m),
        width_m = COALESCE(EXCLUDED.width_m, ais_vessels.width_m),
        destination = COALESCE(EXCLUDED.destination, ais_vessels.destination),
        last_static_utc = EXCLUDED.last_static_utc,
        updated_at = NOW()
    WHERE ais_vessels.last_static_utc IS NULL
       OR ais_vessels.last_static_utc < EXCLUDED.last_static_utc
"""


def handle_message(
    conn: psycopg2.extensions.connection,
    msg: dict[str, Any],
) -> Optional[str]:
    """Parse + persist one aisstream message.

    Returns a short kind label ("position" | "static" | None) for
    caller-side logging / counters.  None means the message type was
    not of interest and nothing was written.
    """
    mtype = msg.get("MessageType")
    mmsi = _mmsi(msg)
    if mmsi is None:
        return None

    cur = conn.cursor()
    try:
        if mtype in ("PositionReport", "StandardClassBPositionReport"):
            pos = _position_fields(msg)
            if pos is None or pos.get("ts") is None:
                return None
            params = {
                "mmsi": mmsi,
                "ship_name": pos.get("ship_name_hint"),
                "latitude": pos["latitude"],
                "longitude": pos["longitude"],
                "cog": pos["cog"],
                "sog": pos["sog"],
                "true_heading": pos["true_heading"],
                "nav_status": pos["nav_status"],
                "ts": pos["ts"],
            }
            cur.execute(_UPSERT_POSITION, params)
            cur.execute(_APPEND_HISTORY, {
                "mmsi": mmsi,
                "ts": pos["ts"],
                "latitude": pos["latitude"],
                "longitude": pos["longitude"],
                "cog": pos["cog"],
                "sog": pos["sog"],
            })
            return "position"

        if mtype == "ShipStaticData":
            static = _static_fields(msg)
            if static is None or static.get("ts") is None:
                return None
            params = {
                "mmsi": mmsi,
                "ship_name": static["ship_name"],
                "call_sign": static["call_sign"],
                "ship_type": static["ship_type"],
                "vessel_kind": classify_vessel(
                    static["ship_type"], static["length_m"]
                ),
                "length_m": static["length_m"],
                "width_m": static["width_m"],
                "destination": static["destination"],
                "ts": static["ts"],
            }
            cur.execute(_UPSERT_STATIC, params)
            return "static"

        return None
    except psycopg2.Error as err:
        # Autocommit aborts the implicit txn — just log and move on.
        logger.warning(
            "DB write failed for mmsi=%s type=%s: %s",
            mmsi, mtype, err,
        )
        return None
    finally:
        cur.close()


def prune_history(
    conn: psycopg2.extensions.connection,
    retention_days: int,
) -> int:
    """Delete ais_position_history rows older than retention_days.
    Returns the number of rows deleted.  Called on a slow cadence
    from the ingest loop — not its own service."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            DELETE FROM ais_position_history
             WHERE ts_utc < NOW() - make_interval(days => %s)
            """,
            (retention_days,),
        )
        return cur.rowcount or 0
    except psycopg2.Error as err:
        logger.warning("prune_history failed: %s", err)
        return 0
    finally:
        cur.close()
