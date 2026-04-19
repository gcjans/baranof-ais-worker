# Baranof AIS Worker

Subscribes to [aisstream.io](https://aisstream.io) for a bounding box
covering SE Alaska + inside-passage approaches and writes vessel
positions + static data into the shared PostgreSQL used by
`dispatch_app` and `fleet-command`.

Runs as its own Railway service.  The worker is stateless beyond
the DB — safe to restart at any time.

## What it writes

| Table | Purpose | Row pattern |
|-------|---------|-------------|
| `ais_vessels` | Live state — one row per MMSI, upserted on every report | ~few thousand rows steady-state |
| `ais_position_history` | Rolling 30-day track for each vessel | pruned hourly in-process |

Schema is owned by `dispatch_app/dispatch_tool/database.py` —
`_init_db_pg` creates both tables with indexes.

## Runtime behaviour

1. Connect to `wss://stream.aisstream.io/v0/stream`
2. Send subscription (bbox + FilterMessageTypes) within 3 s of connect
3. Loop on `ws.recv()`, parsing each JSON message:
   - `PositionReport` / `StandardClassBPositionReport` →
     upsert `ais_vessels` + append `ais_position_history`
   - `ShipStaticData` → upsert `ais_vessels` (name, call sign,
     type, dimensions, destination), re-classify vessel_kind
   - everything else → skipped
4. Status/prune log every `AIS_PRUNE_INTERVAL_SEC`
5. Stall watchdog triggers reconnect if no message within
   `AIS_STALL_TIMEOUT_SEC`
6. Reconnect backoff: 2 s → 4 s → 8 s → … capped at
   `AIS_RECONNECT_MAX_SEC` (default 5 min); resets to initial on
   any session that lasts > 60 s

## Environment

| Var | Required | Default | Notes |
|-----|----------|---------|-------|
| `AIS_STREAM_API_KEY` | yes | — | from https://aisstream.io/apikeys |
| `DATABASE_URL` | yes | — | same PG instance as dispatch_app |
| `AIS_BBOX` | no | `[[54,-135],[58,-130]]` | JSON: `[[sw_lat, sw_lon],[ne_lat, ne_lon]]` |
| `AIS_RECONNECT_INITIAL_SEC` | no | `2.0` | |
| `AIS_RECONNECT_MAX_SEC` | no | `300.0` | |
| `AIS_STALL_TIMEOUT_SEC` | no | `120.0` | |
| `AIS_PRUNE_INTERVAL_SEC` | no | `3600.0` | |
| `AIS_HISTORY_RETENTION_DAYS` | no | `30` | |
| `AIS_LOG_LEVEL` | no | `INFO` | `DEBUG` floods with every message |

## Local dev

```bash
cd ais-worker
pip install -r requirements.txt

# Point at the production PG proxy (public URL, not .internal)
export DATABASE_URL="postgresql://postgres:PASS@metro.proxy.rlwy.net:PORT/railway"
export AIS_STREAM_API_KEY="your-dev-key"

python main.py
```

Expect the first `INFO` log line to say the bbox + "Awaiting
messages", then either `Status: counters=...` every hour OR an
exception with a reconnect log.

## Deploy

1. Push `ais-worker/` as its own GitHub repo (sibling to
   `baranof-dispatch` and `baranof-fleet-command`)
2. New Railway service → Deploy from GitHub → pick repo
3. Set env vars in Railway:
   - `AIS_STREAM_API_KEY` (already set by Geoff per earlier setup)
   - `DATABASE_URL` — copy from the existing dispatch_app service
4. Railway auto-deploys on push to `main`

No health check endpoint — Railway treats the process as the health
signal.  The worker exits non-zero only on missing env vars; transient
upstream failures reconnect internally.
