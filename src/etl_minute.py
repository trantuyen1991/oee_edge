"""
ETL job to compute 1-minute OEE buckets and upsert into fact_production_min.
- Align to minute boundaries (UTC)
- Watermark + backfill (idempotent)
- Use cumulative counters to compute deltas
- Compute runtime_sec from state timeline
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple
import pandas as pd
from loguru import logger
from dotenv import load_dotenv

from io_cas import get_cas_session, fetch_cumulative_points, fetch_state_timeline, fetch_timeseries_numeric, fetch_timeseries_text
from io_pg import (
    get_pg_conn,
    upsert_fact_min,             # vẫn import, dù hiện tại chưa dùng
    load_packaging_snapshot,     # optional
    load_planned_reason_codes,   # NEW
    load_shifts_by_date          # NEW
)
from utils import (
    minute_range_to_finalize, to_epoch_ms, floor_to_minute,
    get_site_tz, minute_in_any_shift  # NEW
)
# ---- Configuration placeholders ----
# Map device_id -> line_id (bạn dán danh sách thật vào đây)
LINE_MAP = {
    # 'device_uuid': line_id
    "e5c11cf0-a27f-11f0-aba6-91052cba3a97": 101,
    "e5cb2f10-a27f-11f0-aba6-91052cba3a97": 102,
    "e5d4f310-a27f-11f0-aba6-91052cba3a97": 103,
    "e5dc9430-a27f-11f0-aba6-91052cba3a97": 104,
    "e5e7ded0-a27f-11f0-aba6-91052cba3a97": 105,
    "e5ef7f00-a27f-11f0-aba6-91052cba3a97": 106,
    "e5f880a0-a27f-11f0-aba6-91052cba3a97": 107,
    "e605c710-a27f-11f0-aba6-91052cba3a97": 108,
}
# Key names in ThingsBoard
K_PRODUCED = "producedCounterPC"     # cumulative
K_REJECT   = "rejectCounterPC"       # cumulative (nếu CHƯA có, comment lại)
K_STATE    = "machineState"          # reason_code (e.g., 9999=RUN)
K_PO       = "processOrderNr"        # text/string
K_PACK     = "packaging_id"          # numeric (nếu có)

RUN_CODE = 9999

COUNTER_KEYS = {"good": "good_cum", "ng": "reject_cum"}
STATE_KEY = "state"  # RUN/STOP/...

DEFAULT_PLANNED_SEC = 60  # usually 60s unless planned stop window

def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def seconds_of_code_in_minute(state_series: List[Tuple[int,int]], minute_start_ms: int, code: int) -> int:
    """Sum seconds in [minute, minute+60s) where machineState==code."""
    start = minute_start_ms; end = minute_start_ms + 60000
    if not state_series:
        return 0
    ev = sorted(state_series, key=lambda x: x[0])
    # Ensure value at 'start'
    if ev[0][0] > start:
        ev = [(start, ev[0][1])] + ev
    ev.append((end, ev[-1][1]))
    sec = 0
    for i in range(len(ev)-1):
        a_ts, a_code = ev[i]; b_ts, _ = ev[i+1]
        a = max(a_ts, start); b = min(b_ts, end)
        if b > a and a_code == code:
            sec += (b - a)//1000
    return max(0, min(60, sec))

def seconds_in_codes(state_series: List[Tuple[int,int]], minute_start_ms: int, codes: set) -> int:
    """Sum seconds where machineState is in given set."""
    total = 0
    for c in codes:
        total += seconds_of_code_in_minute(state_series, minute_start_ms, c)
    return max(0, min(60, total))

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

# def main():
#     load_dotenv()
#     logger.add("logs/etl_minute.log", rotation="10 MB", retention=7, level="INFO")

#     now_utc = datetime.now(timezone.utc)
#     from_min_utc, to_min_utc = minute_range_to_finalize(now_utc)
#     logger.info(f"Finalize range (UTC): {from_min_utc} .. {to_min_utc} (exclusive)")

#     # Build minute edges
#     minute_edges = []
#     cur = from_min_utc
#     while cur < to_min_utc:
#         minute_edges.append(to_epoch_ms(cur))
#         cur += timedelta(minutes=1)

#     cas = get_cas_session()
#     pg = get_pg_conn()
#     pkg_snap = load_packaging_snapshot(pg)
    
#     total_rows = 0
#     try:
#         for line_id, meta in LINE_MAP.items():
#             device_id = meta["device_id"]
#             # Read counters cumulative for window (we read a bit wider: +/- 2 minutes)
#             ts_from_ms = minute_edges[0] - 120000
#             ts_to_ms   = minute_edges[-1] + 120000

#             good_series = fetch_cumulative_points(cas, device_id, COUNTER_KEYS["good"], ts_from_ms, ts_to_ms)
#             ng_series   = fetch_cumulative_points(cas, device_id, COUNTER_KEYS["ng"],   ts_from_ms, ts_to_ms)

#             # Deltas per minute
#             good_delta = compute_minute_deltas(good_series, minute_edges)
#             ng_delta   = compute_minute_deltas(ng_series,   minute_edges)

#             # State timeline for runtime calc — ideally you fetch raw changes once for the whole range
#             state_events = fetch_state_timeline(cas, device_id, ts_from_ms, ts_to_ms)

#             rows = []
#             for ms in minute_edges:
#                 ts_min = datetime.fromtimestamp(ms/1000, tz=timezone.utc)
#                 runtime_sec = compute_runtime_sec(state_events, ms)
#                 produced = good_delta.get(ms, 0) + ng_delta.get(ms, 0)

#                 # TODO: optionally infer packaging_id/process_order for this minute (from attributes/telemetry/dim join)
#                 packaging_id = meta.get("packaging_id")
#                 po = None

#                 rows.append({
#                     "ts_min": ts_min,
#                     "line_id": line_id,
#                     "process_order": po,
#                     "packaging_id": packaging_id,
#                     "produced": produced,
#                     "good": good_delta.get(ms, 0),
#                     "ng": ng_delta.get(ms, 0),
#                     "runtime_sec": runtime_sec,
#                     "planned_sec": DEFAULT_PLANNED_SEC
#                 })

#             affected = upsert_fact_min(pg, rows)
#             total_rows += affected
#             logger.info(f"Line {line_id}: upserted {affected} minute rows")

#     except Exception as e:
#         logger.exception(f"ETL failed: {e}")
#         raise
#     finally:
#         try:
#             pg.close()
#         except Exception:
#             pass
#         try:
#             cas.shutdown()
#         except Exception:
#             pass

#     logger.success(f"ETL done. Total rows upserted: {total_rows}")

def main():
    load_dotenv()
    logger.add("logs/etl_minute.log", rotation="10 MB", retention=7, level="INFO")

    DRY_RUN = env_bool("DRY_RUN", True)   # >>> ADD
    if DRY_RUN:
        logger.info("DRY_RUN = True -> only logging, no database writes")
    else:
        logger.info("DRY_RUN = False -> UPSERT to PostgreSQL enabled")

    now_utc = datetime.now(timezone.utc)
    from_min_utc, to_min_utc = minute_range_to_finalize(now_utc)
    logger.info(f"[DRY-RUN] Minute window: {from_min_utc} .. {to_min_utc} (UTC)")

    # Build minute edges
    minute_edges = []
    cur = from_min_utc
    while cur < to_min_utc:
        minute_edges.append(to_epoch_ms(cur))
        cur += timedelta(minutes=1)

    cas = get_cas_session()

    # >>> ADD: PG connection + metadata (kết nối mở suốt vòng chạy)
    pg = get_pg_conn()
    site_tz = get_site_tz()
    planned_codes = load_planned_reason_codes(pg)
    logger.info(f"Loaded {len(planned_codes)} planned reason codes from PG.")

    from_local = from_min_utc.astimezone(site_tz).date()
    to_local   = (to_min_utc - timedelta(microseconds=1)).astimezone(site_tz).date()
    shifts_by_day = load_shifts_by_date(pg, from_local - timedelta(days=1), to_local)

    total_rows = 0  # >>> ADD (đếm tổng upsert)

    try:
        ts_from_ms = minute_edges[0] - 120000
        ts_to_ms   = minute_edges[-1] + 120000

        for dev_id, line_id in LINE_MAP.items():

            # --- fetch cumulative counters ---
            produced_series = fetch_timeseries_numeric(cas, dev_id, K_PRODUCED, ts_from_ms, ts_to_ms)
            # Nếu chưa có rejectCounterPC ở TB, comment dòng dưới, và ng=0
            reject_series   = fetch_timeseries_numeric(cas, dev_id, K_REJECT,   ts_from_ms, ts_to_ms)

            # --- compute deltas ---
            produced_delta = compute_minute_deltas(produced_series, minute_edges)
            reject_delta   = compute_minute_deltas(reject_series,   minute_edges) if reject_series else {ms:0 for ms in minute_edges}

            # --- fetch state timeline (numeric) ---
            state_series_raw = fetch_timeseries_numeric(cas, dev_id, K_STATE, ts_from_ms, ts_to_ms)
            state_series = [(ts, int(v)) for ts, v in state_series_raw]

            # --- optional metadata ---
            po_series   = fetch_timeseries_text(cas, dev_id, K_PO,   ts_from_ms, ts_to_ms)
            pack_series = fetch_timeseries_numeric(cas, dev_id, K_PACK, ts_from_ms, ts_to_ms)  # nếu có
            
            # >>> ADD: gom rows để upsert theo batch
            batch_rows = []

            # Build per-minute rows (LOG only)
            for ms in minute_edges:
                # ts_min = datetime.fromtimestamp(ms/1000, tz=timezone.utc)
                # After computing runtime_sec & produced/ng/good for minute 'ms'
                ts_min_utc = datetime.fromtimestamp(ms/1000, tz=timezone.utc)
                ts_min_local = ts_min_utc.astimezone(site_tz)

                # produced / good / ng
                produced = produced_delta.get(ms, 0)
                ng = reject_delta.get(ms, 0)
                good = max(0, produced - ng)

                # runtime (RUN_CODE seconds)
                runtime_sec = seconds_of_code_in_minute(state_series, ms, RUN_CODE)
                
                # Determine if this minute is inside any local shift
                shifts_today = shifts_by_day.get(ts_min_local.date(), [])
                shifts_prev  = shifts_by_day.get(ts_min_local.date() - timedelta(days=1), [])
                in_shift = minute_in_any_shift(ts_min_local, shifts_today, shifts_prev)

                if not in_shift:
                    planned_sec = 0
                else:
                    # planned_stop_sec = seconds where machineState ∈ planned_codes
                    planned_stop_sec = seconds_in_codes(state_series, ms, planned_codes) if planned_codes else 0
                    planned_sec = max(0, 60 - planned_stop_sec)

                # process_order & packaging_id tại phút (lấy giá trị gần nhất trước ms)
                po = None
                if po_series:
                    # lấy record có ts <= ms gần nhất
                    po = next((val for ts,val in reversed(po_series) if ts <= ms), po)

                packaging_id = None
                if pack_series:
                    packaging_id = next((int(val) for ts,val in reversed(pack_series) if ts <= ms), None)

                # Log luôn (kể cả khi sẽ upsert)
                logger.info(
                    f"[LINE {line_id}] ts_min={ts_min_utc.isoformat()} in_shift={in_shift} "
                    f"produced={produced} good={good} ng={ng} runtime_sec={runtime_sec} "
                    f"planned_sec={planned_sec} po={po} packaging_id={packaging_id}"
                )

                if not DRY_RUN:
                    batch_rows.append({
                        "ts_min": ts_min_utc,
                        "line_id": line_id,
                        "process_order": po,
                        "packaging_id": packaging_id,
                        "produced": produced,
                        "good": good,
                        "ng": ng,
                        "runtime_sec": runtime_sec,
                        "planned_sec": planned_sec
                    })

            if not DRY_RUN and batch_rows:
                # upsert theo batch (psycopg executemany) — 1 batch/line là đủ vì số bản ghi ít
                affected = upsert_fact_min(pg, batch_rows)
                total_rows += affected
                logger.info(f"UPSERT fact_production_min: line={line_id}, rows={affected}")

    except Exception as e:
        logger.exception(f"Dry-run ETL failed: {e}")
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
            
    if not DRY_RUN:
        logger.success(f"UPSERT done. Total rows upserted: {total_rows}")
    else:
        logger.success("DRY-RUN completed. No database writes.")

if __name__ == "__main__":
    main()
