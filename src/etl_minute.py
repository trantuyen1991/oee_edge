"""
ETL job to compute 1-minute OEE buckets and upsert into fact_production_min.
- Align to minute boundaries (UTC)
- Watermark + backfill (idempotent)
- Use cumulative counters to compute deltas
- Compute runtime_sec from state timeline
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List
import pandas as pd
from loguru import logger
from dotenv import load_dotenv

from io_pg import get_pg_conn, upsert_fact_min, load_packaging_snapshot
from io_cas import get_cas_session, fetch_cumulative_points, fetch_state_timeline
from utils import minute_range_to_finalize, to_epoch_ms, floor_to_minute

# ---- Configuration placeholders ----
LINE_MAP = {
    # line_id : {"device_id": "<TB-Edge device UUID or string>", "packaging_id": default or None}
    105: {"device_id": "LINE_105_DEVICE_ID", "packaging_id": None},
    103: {"device_id": "LINE_103_DEVICE_ID", "packaging_id": None},
    # Add more lines here...
}

COUNTER_KEYS = {"good": "good_cum", "ng": "reject_cum"}
STATE_KEY = "state"  # RUN/STOP/...

DEFAULT_PLANNED_SEC = 60  # usually 60s unless planned stop window

def compute_minute_deltas(cum_series: List[tuple], minute_edges_ms: List[int]) -> Dict[int, int]:
    """
    Convert cumulative series into per-minute deltas using left/right boundary interpolation.
    Returns dict {minute_start_ms: delta}
    """
    if not cum_series:
        return {ms: 0 for ms in minute_edges_ms}
    # Convert to DataFrame for simpler alignment
    df = pd.DataFrame(cum_series, columns=["ts", "val"]).sort_values("ts")
    df = df.drop_duplicates(subset=["ts"], keep="last")
    df = df.set_index("ts")

    # Reindex on minute edges including one extra left boundary
    all_edges = sorted(minute_edges_ms + [minute_edges_ms[0] - 1])
    s = df["val"].astype(float)
    # Forward fill to edges
    s_ff = s.reindex(all_edges, method="pad")
    # Compute delta between consecutive minute edges
    deltas = {}
    for i in range(1, len(all_edges)):
        left = all_edges[i-1]
        right = all_edges[i]
        # The minute bucket corresponds to 'right' as start of minute bucket
        # (since we added left sentinel at (first-1))
        delta = max(0, int(round(s_ff.loc[right] - s_ff.loc[left])))
        deltas[right] = delta
    # Return only buckets that match minute_edges_ms
    return {ms: deltas.get(ms, 0) for ms in minute_edges_ms}

def compute_runtime_sec(state_timeline: List[tuple], minute_start_ms: int) -> int:
    """
    Given a list of (ts_ms, state_code) sorted ascending over the window,
    compute runtime seconds within that 60s minute.
    Runtime = seconds in state RUN (or other 'running' states if needed).
    """
    if not state_timeline:
        return 0
    # Build segments across the minute
    start = minute_start_ms
    end = minute_start_ms + 60000
    # Ensure first state exists at 'start'
    events = sorted(state_timeline, key=lambda x: x[0])
    if events[0][0] > start:
        # Prepend last known state as of 'start' if available (you may need a lookup)
        # For simplicity assume first state holds from 'start'
        events = [(start, events[0][1])] + events
    # Append end sentinel
    events.append((end, events[-1][1]))
    run_sec = 0
    for i in range(len(events)-1):
        st_ts, st = events[i]
        en_ts = events[i+1][0]
        # intersect with [start, end)
        a = max(st_ts, start)
        b = min(en_ts, end)
        if b > a and st == "RUN":
            run_sec += int((b - a) / 1000)
    return max(0, min(60, run_sec))

def main():
    load_dotenv()
    logger.add("logs/etl_minute.log", rotation="10 MB", retention=7, level="INFO")

    now_utc = datetime.now(timezone.utc)
    from_min_utc, to_min_utc = minute_range_to_finalize(now_utc)
    logger.info(f"Finalize range (UTC): {from_min_utc} .. {to_min_utc} (exclusive)")

    # Build minute edges
    minute_edges = []
    cur = from_min_utc
    while cur < to_min_utc:
        minute_edges.append(to_epoch_ms(cur))
        cur += timedelta(minutes=1)

    cas = get_cas_session()
    pg = get_pg_conn()
    pkg_snap = load_packaging_snapshot(pg)

    total_rows = 0
    try:
        for line_id, meta in LINE_MAP.items():
            device_id = meta["device_id"]
            # Read counters cumulative for window (we read a bit wider: +/- 2 minutes)
            ts_from_ms = minute_edges[0] - 120000
            ts_to_ms   = minute_edges[-1] + 120000

            good_series = fetch_cumulative_points(cas, device_id, COUNTER_KEYS["good"], ts_from_ms, ts_to_ms)
            ng_series   = fetch_cumulative_points(cas, device_id, COUNTER_KEYS["ng"],   ts_from_ms, ts_to_ms)

            # Deltas per minute
            good_delta = compute_minute_deltas(good_series, minute_edges)
            ng_delta   = compute_minute_deltas(ng_series,   minute_edges)

            # State timeline for runtime calc — ideally you fetch raw changes once for the whole range
            state_events = fetch_state_timeline(cas, device_id, ts_from_ms, ts_to_ms)

            rows = []
            for ms in minute_edges:
                ts_min = datetime.fromtimestamp(ms/1000, tz=timezone.utc)
                runtime_sec = compute_runtime_sec(state_events, ms)
                produced = good_delta.get(ms, 0) + ng_delta.get(ms, 0)

                # TODO: optionally infer packaging_id/process_order for this minute (from attributes/telemetry/dim join)
                packaging_id = meta.get("packaging_id")
                po = None

                rows.append({
                    "ts_min": ts_min,
                    "line_id": line_id,
                    "process_order": po,
                    "packaging_id": packaging_id,
                    "produced": produced,
                    "good": good_delta.get(ms, 0),
                    "ng": ng_delta.get(ms, 0),
                    "runtime_sec": runtime_sec,
                    "planned_sec": DEFAULT_PLANNED_SEC
                })

            affected = upsert_fact_min(pg, rows)
            total_rows += affected
            logger.info(f"Line {line_id}: upserted {affected} minute rows")

    except Exception as e:
        logger.exception(f"ETL failed: {e}")
        raise
    finally:
        try:
            pg.close()
        except Exception:
            pass
        try:
            cas.shutdown()
        except Exception:
            pass

    logger.success(f"ETL done. Total rows upserted: {total_rows}")

if __name__ == "__main__":
    main()
