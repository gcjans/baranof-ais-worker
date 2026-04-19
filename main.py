"""Baranof AIS worker — entry point.

Maintains a persistent WebSocket subscription to aisstream.io for
the SE Alaska bounding box and writes vessel positions + static data
into the shared PostgreSQL instance used by dispatch_app and
fleet-command.

Runs forever.  Exits non-zero only on fatal config errors (missing
env vars); transient upstream failures (network drop, server
restart, upstream 5xx) trigger exponential-backoff reconnect.

Deployed as its own Railway service; reads DATABASE_URL + AIS_STREAM_API_KEY
from the env.

Runtime model: asyncio single-task event loop.
  - One WS connection
  - Reads messages in a tight loop
  - Hands each to ingest.handle_message (synchronous DB write)
  - Stall watchdog fires if no message arrives within
    cfg.stall_timeout_sec
  - Prune runs inline every cfg.prune_interval_sec
  - On any error: close WS, sleep, reconnect
"""
from __future__ import annotations

import asyncio
import collections
import json
import logging
import signal
import sys
import time
from contextlib import suppress
from typing import Any

import psycopg2
import websockets
from websockets.exceptions import ConnectionClosed

import config
import ingest


logger = logging.getLogger(__name__)


AIS_STREAM_URL = "wss://stream.aisstream.io/v0/stream"


def _build_subscription(cfg: config.Config) -> dict[str, Any]:
    """Build the JSON subscription message aisstream requires within
    3 seconds of WS connect.  We ask for PositionReport +
    StandardClassBPositionReport (small vessel variant) + ShipStaticData
    — everything else aisstream publishes (voyage data, aids-to-nav,
    safety/binary messages) is out of scope for our use case."""
    return {
        "APIKey": cfg.api_key,
        "BoundingBoxes": [cfg.bbox],
        "FilterMessageTypes": [
            "PositionReport",
            "StandardClassBPositionReport",
            "ShipStaticData",
        ],
    }


async def _run_session(cfg: config.Config) -> None:
    """Open one WS session, subscribe, and pump messages until it
    closes.  Returns normally when the connection goes away — the
    caller (_main_loop) handles reconnect backoff."""
    logger.info(
        "Connecting to %s (bbox=%s)", AIS_STREAM_URL, cfg.bbox
    )
    last_message_at = time.monotonic()
    last_prune_at = time.monotonic()
    last_status_at = time.monotonic()
    counters = {"position": 0, "static": 0, "skipped": 0}
    # Diagnostic bookkeeping so we can see WHY messages get skipped
    # instead of just the count.  Cleared implicitly on reconnect
    # (new session → new locals).
    skipped_types: collections.Counter[str] = collections.Counter()
    first_skipped_sample: dict[str, Any] | None = None
    # Short status cadence so operators can see message throughput
    # in near-real-time when something looks wrong.  Steady-state
    # traffic in SE Alaska runs ~hundreds of messages/min even off-
    # season (ferries, tugs, fishing); zero counters after 60 s
    # almost certainly means the subscription was rejected or the
    # bbox / filter is wrong.
    STATUS_INTERVAL_SEC = 60.0
    # IMPORTANT: open the DB connection INSIDE the try/finally so a
    # psycopg2.OperationalError (bad DATABASE_URL, unreachable host,
    # auth failure) surfaces through our logger rather than
    # escaping up into _main_loop's catch-all.
    db_conn: "psycopg2.extensions.connection | None" = None

    try:
        # Keep DB connection open across the session so we don't re-
        # authenticate to PG on every message.  Reopen on reconnect so
        # we recover if PG restarts during a stall.
        db_conn = ingest.connect(cfg.database_url)
        # `open_timeout` bounds the handshake; `ping_interval` keeps
        # the TCP path warm so NAT boxes don't time us out.
        async with websockets.connect(
            AIS_STREAM_URL,
            open_timeout=15,
            ping_interval=30,
            ping_timeout=15,
            max_size=None,
        ) as ws:
            await ws.send(json.dumps(_build_subscription(cfg)))
            logger.info("Subscription sent.  Awaiting messages.")
            while True:
                # recv() with a timeout so we can enforce our own
                # stall watchdog regardless of TCP-level pings.
                try:
                    raw = await asyncio.wait_for(
                        ws.recv(), timeout=cfg.stall_timeout_sec
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "No messages for %.0fs — reconnecting "
                        "(counters since connect: %s)",
                        cfg.stall_timeout_sec, counters,
                    )
                    return

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    counters["skipped"] += 1
                    skipped_types["<json_error>"] += 1
                    continue

                kind = ingest.handle_message(db_conn, msg)
                if kind:
                    counters[kind] += 1
                else:
                    counters["skipped"] += 1
                    mtype = msg.get("MessageType", "<no-MessageType>")
                    if not isinstance(mtype, str):
                        mtype = f"<non-str:{type(mtype).__name__}>"
                    skipped_types[mtype] += 1
                    # Dump the first skipped message in full — one-shot,
                    # so we can see the exact JSON shape aisstream is
                    # sending us without flooding the log.  Trimmed to
                    # 1500 chars in case the message is huge.
                    if first_skipped_sample is None:
                        first_skipped_sample = msg
                        try:
                            dumped = json.dumps(msg, default=str)
                        except (TypeError, ValueError):
                            dumped = repr(msg)
                        logger.info(
                            "SAMPLE skipped message (first only): %s",
                            dumped[:1500],
                        )

                last_message_at = time.monotonic()

                # Periodic status + prune.  Cheap enough to inline.
                now = time.monotonic()
                if now - last_status_at >= STATUS_INTERVAL_SEC:
                    top_skipped = dict(skipped_types.most_common(5))
                    logger.info(
                        "Status: counters=%s, top_skipped_types=%s",
                        counters, top_skipped,
                    )
                    last_status_at = now
                if now - last_prune_at >= cfg.prune_interval_sec:
                    deleted = ingest.prune_history(
                        db_conn, cfg.history_retention_days
                    )
                    last_prune_at = now
                    logger.info(
                        "Prune: deleted %d history rows older than %dd",
                        deleted, cfg.history_retention_days,
                    )
    except ConnectionClosed as err:
        logger.warning(
            "WebSocket closed: code=%s reason=%r (counters: %s)",
            err.code, err.reason, counters,
        )
    except Exception as err:  # noqa: BLE001 — catch-all for reconnect
        logger.exception(
            "Session failed (%s) — will reconnect (counters: %s)",
            type(err).__name__, counters,
        )
    finally:
        if db_conn is not None:
            with suppress(psycopg2.Error):
                db_conn.close()
        elapsed = time.monotonic() - last_message_at
        logger.info(
            "Session ended.  Last message %.0fs ago.  Final counters: %s",
            elapsed, counters,
        )


async def _main_loop(cfg: config.Config, stop: asyncio.Event) -> None:
    """Reconnect loop with exponential backoff.  On each session end,
    sleep `backoff` seconds then retry.  Backoff doubles on failure
    and resets to `reconnect_initial_sec` on any successful session
    that lasted > 60s."""
    backoff = cfg.reconnect_initial_sec
    while not stop.is_set():
        session_start = time.monotonic()
        try:
            await _run_session(cfg)
        except Exception:  # noqa: BLE001 — keep the reconnect loop alive
            # _run_session logs most failures inside its own try/
            # except, but anything that escapes (e.g. a bug that
            # raises BEFORE the try block) used to be swallowed
            # silently, making "no data" failures invisible in
            # Railway.  Log with full traceback so the root cause
            # shows in the log stream.
            logger.exception("Session crashed unexpectedly — will reconnect")
        if stop.is_set():
            break
        session_duration = time.monotonic() - session_start
        if session_duration > 60:
            # A session that ran more than a minute counts as healthy.
            # Reset backoff so a later failure starts fast again.
            backoff = cfg.reconnect_initial_sec
        else:
            backoff = min(backoff * 2, cfg.reconnect_max_sec)
        logger.info("Reconnecting in %.1fs", backoff)
        try:
            await asyncio.wait_for(stop.wait(), timeout=backoff)
        except asyncio.TimeoutError:
            pass


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, stop: asyncio.Event) -> None:
    """Graceful shutdown — SIGINT (Ctrl-C) and SIGTERM (Railway stop)
    both trigger `stop` so the reconnect loop exits cleanly rather
    than getting `KeyboardInterrupt` propagating up through a pending
    websocket."""
    def _handler() -> None:
        if not stop.is_set():
            logger.info("Received shutdown signal — stopping.")
            stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _handler)


def main() -> int:
    try:
        cfg = config.load_config()
    except RuntimeError as err:
        print(f"FATAL: {err}", file=sys.stderr)
        return 2

    logging.basicConfig(
        level=getattr(logging, cfg.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger.info(
        "Baranof AIS worker starting (bbox=%s, retention=%dd)",
        cfg.bbox, cfg.history_retention_days,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop = asyncio.Event()
    _install_signal_handlers(loop, stop)
    try:
        loop.run_until_complete(_main_loop(cfg, stop))
    finally:
        loop.close()
    logger.info("Baranof AIS worker exited cleanly.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
