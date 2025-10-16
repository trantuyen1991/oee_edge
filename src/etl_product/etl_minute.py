"""
ETL job to compute 1-minute OEE buckets and upsert into fact_production_min.
- Align to minute boundaries (UTC)
- Watermark + backfill (idempotent)
- Use cumulative counters to compute deltas
- Compute runtime_sec from state timeline
"""
from pathlib import Path                                     # path utilities
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
    load_shifts_by_date,          # NEW
    load_device_map,
    compute_from_to_auto
)

from utils import (
    minute_range_to_finalize, to_epoch_ms, floor_to_minute,
    get_site_tz, minute_in_any_shift  # NEW
)
import asyncio
import os
from src.api.post_etl_minute import  process_batch
from src.api.token_map import load_token_map

# ---- Configuration placeholders ----
ROOT = Path(__file__).resolve().parents[1]                   # project root: /home/admin/oee-edge
ENV_PATH = ROOT / ".env"                                     # expected .env path
load_dotenv(dotenv_path=ENV_PATH)                            # explicit load from root
print(f"[BOOT] .env loaded from: {ENV_PATH}, exists={ENV_PATH.exists()}")  # quick sanity log
# Key names in ThingsBoard
K_PRODUCED = os.getenv("KEY_PRODUCED", "producedCounterPC")
K_REJECT   = os.getenv("KEY_REJECT",   "rejectCounterPC")
K_STATE    = os.getenv("KEY_STATE",    "machineState")
K_PO       = os.getenv("KEY_PO",       "processOrderNr")
K_PACK     = os.getenv("KEY_PACK",     "packaging_id")
RUN_CODE   = int(os.getenv("RUN_CODE", "9999"))
BACKFILL_MIN = int(os.getenv("BACKFILL_MIN", "43200"))
COUNTER_KEYS = {"good": "good_cum", "ng": "reject_cum"}
STATE_KEY = "state"  # RUN/STOP/...

DEFAULT_PLANNED_SEC = 60  # usually 60s unless planned stop window

UPSERT_PG= os.getenv("UPSERT_PG", "true")
PUBLISH_TB= os.getenv("PUBLISH_TB", "true")

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

    df = pd.DataFrame(cum_series, columns=["ts", "val"]).sort_values("ts")
    df = df.drop_duplicates(subset=["ts"], keep="last").set_index("ts")

    all_edges = sorted(minute_edges_ms + [minute_edges_ms[0] - 1])
    s = df["val"].astype(float)
    s_ff = s.reindex(all_edges, method="pad")   # chỉ ffill

    deltas = {}
    for i in range(1, len(all_edges)):
        left = all_edges[i-1]
        right = all_edges[i]
        lv = s_ff.loc[left]
        rv = s_ff.loc[right]
        if pd.isna(lv) or pd.isna(rv):
            delta = 0                          # CHỐT: thiếu biên -> không đếm
        else:
            delta = max(0, int(round(rv - lv)))
        deltas[right] = delta

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
    os.makedirs("logs", exist_ok=True)
    logger.add(
        "logs/etl_minute_{time:YYYY-MM-DD}.log",
        rotation="00:00",
        retention="7 days",
        compression="gz",
        level="INFO",
        enqueue=True
    )

    DRY_RUN = env_bool("DRY_RUN", True)   # >>> ADD
    if DRY_RUN:
        logger.info("DRY_RUN = True -> only logging, no database writes")
    else:
        logger.info("DRY_RUN = False -> UPSERT to PostgreSQL enabled")

    
    cas = get_cas_session()

    # >>> ADD: PG connection + metadata (kết nối mở suốt vòng chạy)
    pg = get_pg_conn()
    site_tz = get_site_tz()
    device_map = load_device_map(pg)  # {device_uuid: (line_id, machine_id)}
    logger.info(f"Loaded {len(device_map)} devices from dim_device.")
    
    # PG_DSN = str(os.getenv("PG_DSN", "postgresql+psycopg://postgres:admin@127.0.0.1:5432/oee"))
    token_map = load_token_map(pg)  
    logger.info(f"Loaded {len(token_map)} devices from dim_device.")
    logger.info(f"First 5 token_map items: {list(token_map.items())[:5]}")      
    
    planned_codes = load_planned_reason_codes(pg)
    logger.info(f"Loaded {len(planned_codes)} planned reason codes from PG.")

    
    total_rows = 0  # >>> ADD (đếm tổng upsert)

    try: 
        for dev_id, (line_id, machine_id) in device_map.items():
            
            from_utc, to_utc, backfill_min, last_ts = compute_from_to_auto(pg, line_id, BACKFILL_MIN)
            logger.info("[LINE {}] last_ts ={} Auto backfill = {} min (cap={})", line_id, last_ts, backfill_min, BACKFILL_MIN)
            logger.info("[LINE {}] from_utc ={} to_utc = {}", line_id, from_utc, to_utc)
            ts_from_ms = to_epoch_ms(from_utc) - 120000 
            ts_to_ms   = to_epoch_ms(to_utc) + 120000

            # now_utc = datetime.now(timezone.utc)
            # from_min_utc, to_min_utc = minute_range_to_finalize(now_utc,backfill_min)
            # logger.info(f"[DRY-RUN] Minute window: {from_min_utc} .. {to_min_utc} (UTC)")

            from_local = from_utc.astimezone(site_tz).date()
            to_local   = (to_utc - timedelta(microseconds=1)).astimezone(site_tz).date()
            shifts_by_day = load_shifts_by_date(pg, from_local - timedelta(days=1), to_local)
            # Build minute edges
            minute_edges = []
            cur = from_utc
            while cur < to_utc:
                minute_edges.append(to_epoch_ms(cur))
                cur += timedelta(minutes=1)

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

                
                if po and (po.startswith("Bad status code:") or po == ""):
                    po = None
                    
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
                if UPSERT_PG:
                    # upsert theo batch (psycopg executemany) — 1 batch/line là đủ vì số bản ghi ít
                    affected = upsert_fact_min(pg, batch_rows,logger)
                    total_rows += affected
                    logger.info(f"UPSERT fact_production_min: line={line_id}, rows={affected}")
                if PUBLISH_TB:
                    
                    lines_data = [
                        {
                            "line_id": line_id,
                            "minute_end_utc_ms": to_epoch_ms(ts_min_utc),
                            "kpi": {
                                # "process_order": str(po),
                                # "packaging_id": int(packaging_id) if packaging_id is not None else 0,
                                "produced": int(produced),
                                "good": int(good),
                                "ng": int(ng),
                                "runtime_sec": int(runtime_sec),
                                "planned_sec": int(planned_sec),
                                "version": "1.0.0"
                            }
                        }
                    ]
                    logger.info(f"Publishing to TB: line={line_id}, rows={lines_data}")
                    asyncio.run(process_batch(lines_data, token_map, logger=logger))

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
