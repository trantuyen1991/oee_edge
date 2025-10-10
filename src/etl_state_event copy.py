"""
ETL job: Build fact_state_event from ThingsBoard Edge timeseries.
- Reads machineState (reason_code) from Cassandra (ts_kv_cf)
- Detects transitions (state changes) -> segments [start_ts, end_ts)
- Maps reason_code -> (reason_id, state_id) via PostgreSQL (dim_reason/dim_state)
- Computes shift membership (for future shift_id if needed)
- Upserts into fact_state_event with UNIQUE(line_id, start_ts)
- Idempotent; safe to re-run over overlapping windows
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple, Optional

from dotenv import load_dotenv
from loguru import logger

from io_cas import get_cas_session, fetch_timeseries_numeric, fetch_timeseries_text
from io_pg import (
    get_pg_conn,
    load_reason_lookup,          # NEW (you'll add this function below)
    upsert_state_event_batch,    # NEW (you'll add this function below)
    load_shifts_by_date,
    load_device_map,
    get_last_event_end_ts
)
from utils import (
    floor_to_minute,
    get_site_tz,
    minute_in_any_shift,
    to_epoch_ms,
    resolve_shift
)

# ==== CONFIG ====
ENTITY_KEY_STATE = os.getenv("KEY_STATE", "machineState")
ENTITY_KEY_PO    = os.getenv("KEY_PO",    "processOrderNr")
ENTITY_KEY_PACK  = os.getenv("KEY_PACK",  "packaging_id")

ENTITY_KEY_WD   = os.getenv("KEY_WATCHDOG", "watchDog")
COMM_LOSS_CODE  = int(os.getenv("COMM_LOSS_CODE", "9000"))
OFFLINE_GRACE_SEC = int(os.getenv("OFFLINE_GRACE_SEC", "30"))

BACKFILL_ON_START      = os.getenv("BACKFILL_ON_START", "false").lower() in ("1","true","yes","on")
BACKFILL_LOOKBACK_HOURS= int(os.getenv("BACKFILL_LOOKBACK_HOURS","24"))
EVENT_BLOCK_MIN        = int(os.getenv("EVENT_BLOCK_MIN","30"))

def backfill_windows(pg, site_tz):
    block = int(os.getenv("EVENT_BLOCK_MIN","30"))
    lookback_h = int(os.getenv("BACKFILL_LOOKBACK_HOURS","24"))
    watermark = int(os.getenv("WATERMARK_SEC","120"))

    now = datetime.now(timezone.utc)
    hard_to = floor_to_minute(now - timedelta(seconds=watermark))

    # build danh sách (dev_id -> start_from)
    starts: Dict[str, datetime] = {}
    with pg.cursor() as cur:
        for dev_id, (line_id, _) in device_map.items():
            cur.execute("SELECT MAX(end_ts) FROM fact_state_event WHERE line_id=%s", (line_id,))
            row = cur.fetchone()
            last_end = row[0]
            if last_end is None:
                starts[dev_id] = hard_to - timedelta(hours=lookback_h)
            else:
                starts[dev_id] = last_end - timedelta(minutes=5)  # overlap 5'
    # sinh các block
    sched = []
    for dev_id, start in starts.items():
        s = floor_to_minute(start)
        while s < hard_to:
            e = min(s + timedelta(minutes=block), hard_to)
            sched.append((dev_id, s, e))
            s = e
    return sched

def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def minute_window_for_events(now_utc: datetime) -> Tuple[datetime, datetime]:
    """
    Event job window:
      - Watermark: finalize until N-120s (same principle as minute job)
      - Process last 5 minutes (overlap) to be idempotent & handle late data
    """
    watermark_sec = int(os.getenv("WATERMARK_SEC", "120"))
    back_minutes  = int(os.getenv("EVENT_BACK_MIN", "5"))

    last_ok = floor_to_minute(now_utc - timedelta(seconds=watermark_sec))
    from_min = last_ok - timedelta(minutes=back_minutes - 1)
    to_min = last_ok + timedelta(minutes=1)
    return from_min, to_min

def compress_state_series(series: List[Tuple[int, int]],
                          start_ms: int,
                          end_ms:   int) -> List[Tuple[int, int, int]]:
    """
    Compress raw points (ts_ms, code) into non-overlapping segments within [start_ms, end_ms).
    Returns: list of (seg_start_ms, seg_end_ms, code)
    - If first point is after start_ms, assume its code holds from start_ms
    - If no data, returns []
    """
    if not series:
        return []

    ser = sorted(series, key=lambda x: x[0])
    # Ensure first at window start
    if ser[0][0] > start_ms:
        ser = [(start_ms, ser[0][1])] + ser
    # Append end sentinel
    if ser[-1][0] < end_ms:
        ser.append((end_ms, ser[-1][1]))

    segs: List[Tuple[int, int, int]] = []
    for i in range(len(ser) - 1):
        a_ts, a_code = ser[i]
        b_ts, _      = ser[i + 1]
        # intersect with window
        s = max(a_ts, start_ms)
        e = min(b_ts, end_ms)
        if e > s:
            if segs and segs[-1][2] == a_code and segs[-1][1] == s:
                # merge contiguous same-code
                segs[-1] = (segs[-1][0], e, a_code)
            else:
                segs.append((s, e, a_code))
    return segs

# --- NEW: Build online/offline windows from watchdog or state points
def build_online_windows(state_series: List[Tuple[int,int]],
                         wd_points: List[Tuple[int,float]],
                         s_ms: int, e_ms: int) -> List[Tuple[int,int,bool]]:
    """
    Trả về list (win_start_ms, win_end_ms, is_online) trong [s_ms,e_ms).
    Logic: nếu không thấy mốc nào trong > OFFLINE_GRACE_SEC => offline.
    Ưu tiên watchdog; nếu watchdog trống thì fallback dùng state_series.
    """
    grace = OFFLINE_GRACE_SEC * 1000
    marks = sorted(ts for ts,_ in wd_points) if wd_points else sorted(ts for ts,_ in state_series)
    if not marks:
        return [(s_ms, e_ms, False)]
    # giữ các dấu mốc nằm trong khoảng
    marks = [t for t in marks if s_ms <= t <= e_ms]
    if not marks:
        return [(s_ms, e_ms, False)]
    out = []
    # đoạn trước mốc đầu tiên
    if marks[0] > s_ms:
        out.append((s_ms, marks[0], False))
    # giữa các mốc
    for i in range(len(marks)-1):
        a, b = marks[i], marks[i+1]
        if b - a > grace:
            out.append((a, a+grace, True))
            out.append((a+grace, b, False))
        else:
            out.append((a, b, True))
    # đoạn sau mốc cuối
    last = marks[-1]
    if e_ms - last > grace:
        out.append((last, last+grace, True))
        out.append((last+grace, e_ms, False))
    else:
        out.append((last, e_ms, True))
    # gộp kề nhau
    merged = []
    for s,e,fl in out:
        if not merged: merged.append((s,e,fl)); continue
        ps,pe,pf = merged[-1]
        if pf==fl and s<=pe:
            merged[-1] = (ps, max(pe,e), pf)
        else:
            merged.append((s,e,fl))
    return [(max(s_ms,s), min(e_ms,e), fl) for s,e,fl in merged if min(e_ms,e) > max(s_ms,s)]

def choose_latest_before(ms: int, points: List[Tuple[int, Any]]) -> Optional[Any]:
    """
    Given points [(ts_ms, val)...], pick the latest val where ts_ms <= ms.
    """
    for ts, val in reversed(sorted(points, key=lambda x: x[0])):
        if ts <= ms:
            return val
    return None

def process_one_device_window(dev_id: str,
                              line_id: int,
                              machine_id: Optional[int],
                              from_min_utc: datetime,
                              to_min_utc: datetime,
                              cas, pg, site_tz, reason_lookup, shifts_by_day) -> int:
    """Return number of rows upserted."""
    start_ms = to_epoch_ms(from_min_utc)
    end_ms   = to_epoch_ms(to_min_utc)

    state_points = fetch_timeseries_numeric(cas, dev_id, ENTITY_KEY_STATE, start_ms-120000, end_ms+120000)
    state_series = [(ts, int(v)) for ts, v in state_points]
    if not state_series:
        logger.info(f"[LINE {line_id}] no state points in window -> skip")
        return 0

    wd_points    = fetch_timeseries_numeric(cas, dev_id, ENTITY_KEY_WD,   start_ms-120000, end_ms+120000)
    po_points    = fetch_timeseries_text(  cas, dev_id, ENTITY_KEY_PO,   start_ms-120000, end_ms+120000)
    pack_points  = fetch_timeseries_numeric(cas, dev_id, ENTITY_KEY_PACK,start_ms-120000, end_ms+120000)

    segs = compress_state_series(state_series, start_ms, end_ms)
    online_windows = build_online_windows(state_series, wd_points, start_ms, end_ms)

    def intersect(a1,a2,b1,b2):
        s = max(a1,b1); e = min(a2,b2)
        return (s,e) if e > s else None

    rows = []
    for s_ms, e_ms, code in segs:
        for ow_s, ow_e, is_on in online_windows:
            inter = intersect(s_ms, e_ms, ow_s, ow_e)
            if not inter: continue
            seg_s, seg_e = inter

            eff_code = code if is_on else COMM_LOSS_CODE
            start_ts = datetime.fromtimestamp(seg_s/1000, tz=timezone.utc)
            end_ts   = datetime.fromtimestamp(seg_e/1000, tz=timezone.utc)

            reason_id, state_id = reason_lookup.get(eff_code, (None, None))

            start_local = start_ts.astimezone(site_tz)
            shifts_today = shifts_by_day.get(start_local.date(), [])
            shifts_prev  = shifts_by_day.get(start_local.date() - timedelta(days=1), [])
            shift_tuple  = resolve_shift(start_local, shifts_today, shifts_prev)  # (date_str, no, shift_id) or None
            shift_id     = shift_tuple[2] if shift_tuple else None

            po = choose_latest_before(seg_s, po_points) if po_points else None
            packaging_id = choose_latest_before(seg_s, pack_points) if pack_points else None
            try: packaging_id = int(packaging_id) if packaging_id is not None else None
            except: packaging_id = None

            rows.append({
                "line_id": line_id,
                "machine_id": machine_id,
                "state_id": state_id,
                "reason_id": reason_id,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "shift_id": shift_id,
                "po": po,
                "packaging_id": packaging_id,
                "note": None
            })

    if not rows:
        return 0
    if DRY_RUN:
        for r in rows:
            logger.info(f"[LINE {line_id}] {r['start_ts'].isoformat()} -> {r['end_ts'].isoformat()} "
                        f"reason={r['reason_id']} state={r['state_id']} shift_id={r['shift_id']}")
        return 0
    affected = upsert_state_event_batch(pg, rows)
    return affected

def main():
    load_dotenv()
    os.makedirs("logs", exist_ok=True)
    # logger.add("logs/etl_state_event.log", rotation="10 MB", retention=7, level="INFO")
    logger.add(
        "logs/etl_state_event_{time:YYYY-MM-DD}.log",  # hoặc etl_minute_{time:YYYY-MM-DD}.log
        rotation="00:00",       # tách file mỗi ngày lúc 00:00 (theo local time)
        retention="7 days",     # chỉ giữ 7 ngày gần nhất (tự xóa file cũ)
        compression="gz",       # nén file cũ .gz để tiết kiệm dung lượng
        level="INFO",
        enqueue=True            # an toàn khi chạy qua systemd / multi-thread
    )

    DRY_RUN = env_bool("DRY_RUN", False)  # default ghi DB, đổi true nếu muốn chỉ log
    if DRY_RUN:
        logger.info("DRY_RUN=True -> only logging (no DB writes)")
    else:
        logger.info("DRY_RUN=False -> UPSERT fact_state_event enabled")

    now_utc = datetime.now(timezone.utc)
    from_min_utc, to_min_utc = minute_window_for_events(now_utc)
    logger.info(f"Event window (UTC): {from_min_utc} .. {to_min_utc}")

    # Build absolute millisecond boundaries for reading
    start_ms = to_epoch_ms(from_min_utc)
    end_ms   = to_epoch_ms(to_min_utc)

    site_tz = get_site_tz()
    cas = get_cas_session()
    pg  = get_pg_conn()
    device_map = load_device_map(pg)  # {device_uuid: (line_id, machine_id)}
    logger.info(f"Loaded {len(device_map)} devices from dim_device.")

    # load reason_code -> (reason_id, state_id)
    reason_lookup = load_reason_lookup(pg)
    logger.info(f"Loaded reason lookup: {len(reason_lookup)} codes.")

    # load shift calendar covering [from..to] (+ previous day for wrap)
    from_local = from_min_utc.astimezone(site_tz).date()
    to_local   = (to_min_utc - timedelta(microseconds=1)).astimezone(site_tz).date()
    shifts_by_day = load_shifts_by_date(pg, from_local - timedelta(days=1), to_local)

    total_segments = 0
    total_upserts  = 0

    try:
        for dev_id, (line_id, machine_id) in device_map.items():
            # 1) read machineState for window
            state_points = fetch_timeseries_numeric(cas, dev_id, ENTITY_KEY_STATE, start_ms - 120000, end_ms + 120000)
            state_series = [(ts, int(v)) for ts, v in state_points]

            if not state_series:
                logger.info(f"[LINE {line_id}] no state points in window -> skip")
                continue

            # (optional) read context
            po_points   = fetch_timeseries_text(cas,   dev_id, ENTITY_KEY_PO,   start_ms - 120000, end_ms + 120000)
            pack_points = fetch_timeseries_numeric(cas, dev_id, ENTITY_KEY_PACK, start_ms - 120000, end_ms + 120000)

            # 2) compress into segments
            online_windows = build_online_windows(wd_points, start_ms, end_ms)
            segs = compress_state_series(state_series, start_ms, end_ms)  # [(s,e,code)]
            total_segments += len(segs)

            def intersect(a_s,a_e, b_s,b_e):  # trả (start,end) giao nhau
                s = max(a_s,b_s); e = min(a_e,b_e)
                return (s,e) if e > s else None
            # 3) build rows for upsert
            rows = []
            for s_ms, e_ms, code in segs:
                inter = intersect(s_ms, e_ms, ow_s, ow_e)
                if not inter: continue
                seg_s, seg_e = inter
                eff_code = code if is_on else COMM_LOSS_CODE

                start_ts = datetime.fromtimestamp(s_ms / 1000, tz=timezone.utc)
                end_ts   = datetime.fromtimestamp(e_ms / 1000, tz=timezone.utc)

                # lookup reason/state
                reason_id, state_id = reason_lookup.get(code, (None, None))

                # shift detection (optional to compute shift_id in future)
                start_local = start_ts.astimezone(site_tz)
                shifts_today = shifts_by_day.get(start_local.date(), [])
                shifts_prev  = shifts_by_day.get(start_local.date() - timedelta(days=1), [])
                # _in_shift = minute_in_any_shift(start_local, shifts_today, shifts_prev)  # bool; keep for debug

                shift_tuple = resolve_shift(start_local, shifts_today, shifts_prev)  # (shift_date_str, shift_no, shift_id) or None
                _in_shift = shift_tuple is not None
                shift_id = shift_tuple[2] if shift_tuple else None
                # context at start
                po = choose_latest_before(s_ms, po_points) if po_points else None
                packaging_id = choose_latest_before(s_ms, pack_points) if pack_points else None
                if packaging_id is not None:
                    try:
                        packaging_id = int(packaging_id)
                    except Exception:
                        packaging_id = None

                logger.info(
                    f"[LINE {line_id}] seg {start_ts.isoformat()} -> {end_ts.isoformat()} "
                    f"code={code} reason_id={reason_id} state_id={state_id} in_shift={_in_shift} "
                    f"po={po} packaging_id={packaging_id}"
                )

                rows.append({
                    "line_id": line_id,
                    "machine_id": machine_id,   # NOW filled from dim_device (can be None)
                    "state_id": state_id,
                    "reason_id": reason_id,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "shift_id": shift_id,
                    "po": po,
                    "packaging_id": packaging_id,
                    "note": None
                })

            # 4) UPSERT batch
            if not DRY_RUN and rows:
                # affected = upsert_state_event_batch(pg, rows)
                total_upserts += affected
                logger.info(f"[LINE {line_id}] upserted {affected} event rows")

    except Exception as e:
        logger.exception(f"State event ETL failed: {e}")
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

    logger.success(f"State event ETL done. Segments={total_segments}, Upserts={total_upserts}, DRY_RUN={DRY_RUN}")

if __name__ == "__main__":
    main()
