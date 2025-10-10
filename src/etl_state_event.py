"""
ETL job: Build fact_state_event from ThingsBoard Edge timeseries.
- Reads machineState (reason_code) from Cassandra (ts_kv_cf)
- Detects transitions (state changes) -> segments [start_ts, end_ts)
- Maps reason_code -> (reason_id, state_id) via PostgreSQL (dim_reason/dim_state)
- Computes shift membership (for future shift_id if needed)
- Upserts into fact_state_event with UNIQUE(line_id, start_ts)
- Idempotent; safe to re-run over overlapping windows
"""
from pathlib import Path                                     # path utilities   
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
    get_last_event_end_ts,
    load_states
)
from utils import (
    floor_to_minute,
    get_site_tz,
    minute_in_any_shift,
    to_epoch_ms,
    resolve_shift
)

# ==== CONFIG ====
ROOT = Path(__file__).resolve().parents[1]                   # project root: /home/admin/oee-edge
ENV_PATH = ROOT / ".env"                                     # expected .env path
load_dotenv(dotenv_path=ENV_PATH)                            # explicit load from root
print(f"[BOOT] .env loaded from: {ENV_PATH}, exists={ENV_PATH.exists()}")  # quick sanity log 

ENTITY_KEY_STATE = os.getenv("KEY_STATE", "machineState")
ENTITY_KEY_PO    = os.getenv("KEY_PO",    "processOrderNr")
ENTITY_KEY_PACK  = os.getenv("KEY_PACK",  "packaging_id")

ENTITY_KEY_WD   = os.getenv("KEY_WATCHDOG", "watchDog")
COMM_LOSS_CODE  = int(os.getenv("COMM_LOSS_CODE", "9000"))
OFFLINE_GRACE_SEC = int(os.getenv("OFFLINE_GRACE_SEC", "30"))

BACKFILL_ON_START      = os.getenv("BACKFILL_ON_START", "false").lower() in ("1","true","yes","on")
BACKFILL_LOOKBACK_HOURS= int(os.getenv("BACKFILL_LOOKBACK_HOURS","24"))
EVENT_BLOCK_MIN        = int(os.getenv("EVENT_BLOCK_MIN","30"))

# --- Fallback for missing reason mapping ---
UNKNOWN_REASON_ID = int(os.getenv("UNKNOWN_REASON_ID", "999000"))
UNKNOWN_STATE_ID  = int(os.getenv("UNKNOWN_STATE_ID",  "0"))
UNKNOWN_REASON_CODE = os.getenv("UNKNOWN_REASON_CODE", "-1")

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
# def build_online_windows(state_series: List[Tuple[int,int]],
#                          wd_points: List[Tuple[int,float]],
#                          s_ms: int, e_ms: int) -> List[Tuple[int,int,bool]]:
#     """
#     Trả về list (win_start_ms, win_end_ms, is_online) trong [s_ms,e_ms).
#     Logic: nếu không thấy mốc nào trong > OFFLINE_GRACE_SEC => offline.
#     Ưu tiên watchdog; nếu watchdog trống thì fallback dùng state_series.
#     """
#     grace = OFFLINE_GRACE_SEC * 1000
#     marks = sorted(ts for ts,_ in wd_points) if wd_points else sorted(ts for ts,_ in state_series)
#     if not marks:
#         return [(s_ms, e_ms, False)]
#     # giữ các dấu mốc nằm trong khoảng
#     marks = [t for t in marks if s_ms <= t <= e_ms]
#     if not marks:
#         return [(s_ms, e_ms, False)]
#     out = []
#     # đoạn trước mốc đầu tiên
#     if marks[0] > s_ms:
#         out.append((s_ms, marks[0], False))
#     # giữa các mốc
#     for i in range(len(marks)-1):
#         a, b = marks[i], marks[i+1]
#         if b - a > grace:
#             out.append((a, a+grace, True))
#             out.append((a+grace, b, False))
#         else:
#             out.append((a, b, True))
#     # đoạn sau mốc cuối
#     last = marks[-1]
#     if e_ms - last > grace:
#         out.append((last, last+grace, True))
#         out.append((last+grace, e_ms, False))
#     else:
#         out.append((last, e_ms, True))
#     # gộp kề nhau
#     merged = []
#     for s,e,fl in out:
#         if not merged: merged.append((s,e,fl)); continue
#         ps,pe,pf = merged[-1]
#         if pf==fl and s<=pe:
#             merged[-1] = (ps, max(pe,e), pf)
#         else:
#             merged.append((s,e,fl))
#     return [(max(s_ms,s), min(e_ms,e), fl) for s,e,fl in merged if min(e_ms,e) > max(s_ms,s)]

def build_online_windows(state_series, wd_points, s_ms, e_ms):
    """
    Trả về danh sách (start_ms, end_ms, is_online).

    FIX: Không tạo các đoạn offline "ảo" ở rìa block.
    Quy ước:
      - Dùng watchdog (wd_points) làm mốc; nếu không có thì fallback sang state_series.
      - Nếu không có dữ liệu trong cả block -> coi offline toàn block.
      - Giữa hai mốc liên tiếp: nếu khoảng cách > grace => tách thành (online grace) + (offline phần còn lại).
      - Đầu/đuôi block: LUÔN nối với trạng thái online nếu trong block có dữ liệu.
    """
    grace = OFFLINE_GRACE_SEC * 1000  # ví dụ 30s -> 30000ms

    # Lấy danh sách mốc thời gian từ watchdog trước; nếu không có thì dùng state_series
    if wd_points:
        marks = sorted(ts for ts, _ in wd_points)
    else:
        marks = sorted(ts for ts, _ in state_series)

    # Không có mốc nào -> offline toàn block
    if not marks:
        return [(s_ms, e_ms, False)]

    # Giữ các mốc nằm TRONG block
    marks = [t for t in marks if s_ms <= t <= e_ms]
    if not marks:
        return [(s_ms, e_ms, False)]

    out = []

    # Tạo các đoạn giữa các mốc liên tiếp
    for i in range(len(marks) - 1):
        a, b = marks[i], marks[i + 1]
        if b - a > grace:
            # phần đầu coi online (đến hết grace)
            out.append((a, a + grace, True))
            # phần sau là offline
            out.append((a + grace, b, False))
        else:
            # cả đoạn coi online
            out.append((a, b, True))

    # --- FIX: không tạo offline ảo ở rìa block ---
    # Nếu có dữ liệu trong block, đầu & cuối block nối online.
    if out and out[0][0] > s_ms:
        out.insert(0, (s_ms, out[0][0], True))
    if out and out[-1][1] < e_ms:
        out.append((out[-1][1], e_ms, True))

    # Gộp các đoạn kề nhau cùng trạng thái
    merged = []
    for s, e, on in out:
        if not merged:
            merged.append((s, e, on))
            continue
        ps, pe, pon = merged[-1]
        if pon == on and s <= pe:
            merged[-1] = (ps, max(pe, e), pon)
        else:
            merged.append((s, e, on))

    return merged


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
                              cas, pg, site_tz, reason_lookup, shifts_by_day, DRY_RUN, state_lookup) -> int:
    """Return number of rows upserted."""
    start_ms = to_epoch_ms(from_min_utc)
    end_ms   = to_epoch_ms(to_min_utc)

    # 1) read machineState for window
    state_points = fetch_timeseries_numeric(cas, dev_id, ENTITY_KEY_STATE, start_ms-120000, end_ms+120000)
    state_series = [(ts, int(v)) for ts, v in state_points]
    if not state_series:
        logger.info(f"[LINE {line_id}] no state points in window -> skip")
        return 0 , 0
    
    # (optional) read context
    wd_points    = fetch_timeseries_numeric(cas, dev_id, ENTITY_KEY_WD,   start_ms-120000, end_ms+120000)
    po_points    = fetch_timeseries_text(  cas, dev_id, ENTITY_KEY_PO,   start_ms-120000, end_ms+120000)
    pack_points  = fetch_timeseries_numeric(cas, dev_id, ENTITY_KEY_PACK,start_ms-120000, end_ms+120000)
    note         = None
    # 2) compress into segments
    segs = compress_state_series(state_series, start_ms, end_ms)
    online_windows = build_online_windows(state_series, wd_points, start_ms, end_ms)

    def intersect(a1,a2,b1,b2):
        s = max(a1,b1); e = min(a2,b2)
        return (s,e) if e > s else None
    # 3) build rows for upsert
    rows = []
    
    for s_ms, e_ms, code in segs:
        for ow_s, ow_e, is_on in online_windows:
            inter = intersect(s_ms, e_ms, ow_s, ow_e)
            if not inter: continue
            seg_s, seg_e = inter

            eff_code = code if is_on else COMM_LOSS_CODE
            start_ts = datetime.fromtimestamp(seg_s/1000, tz=timezone.utc)
            end_ts   = datetime.fromtimestamp(seg_e/1000, tz=timezone.utc)

            start_local = start_ts.astimezone(site_tz)
            shifts_today = shifts_by_day.get(start_local.date(), [])
            shifts_prev  = shifts_by_day.get(start_local.date() - timedelta(days=1), [])
            shift_tuple  = resolve_shift(start_local, shifts_today, shifts_prev)  # (date_str, no, shift_id) or None
            shift_id     = shift_tuple[2] if shift_tuple else None

            po = choose_latest_before(seg_s, po_points) if po_points else None
            packaging_id = choose_latest_before(seg_s, pack_points) if pack_points else None
            try: packaging_id = int(packaging_id) if packaging_id is not None else None
            except: packaging_id = None
            
            reason_id, state_id = reason_lookup.get(eff_code, (None, None))
            if reason_id is None or state_id is None:
                reason_id = UNKNOWN_REASON_ID
                state_id = UNKNOWN_STATE_ID
                note = f"UNKNOWN reason_code={eff_code}"
                logger.warning(f"[LINE {line_id}] missing mapping for reason_code={eff_code} -> fallback to "
                            f"(reason_id={UNKNOWN_REASON_ID}, state_id={UNKNOWN_STATE_ID})")

            # --- Resolve state_code from dim_state for logging/note ---
            state_code = state_lookup.get(state_id, "UNKNOWN")

            note_text = note or ""
            note_text = f"{state_code} ({note_text})" if note_text else state_code

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
                "note": note_text
            })
            
    if not rows:
        return 0, len(segs)
    if DRY_RUN:
        for r in rows:
            logger.info(f"[LINE {line_id}] {r['start_ts'].isoformat()} -> {r['end_ts'].isoformat()} "
                        f"reason={r['reason_id']} state={r['state_id']} shift_id={r['shift_id']}")
        return 0 , len(segs)
    affected = upsert_state_event_batch(pg, rows)
    return affected, len(segs)

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
    site_tz = get_site_tz()
    cas = get_cas_session()
    pg  = get_pg_conn()

    device_map = load_device_map(pg)  # {uuid: (line_id, machine_id)}
    reason_lookup = load_reason_lookup(pg)
    state_lookup = {s['state_id']: s['state_code'] for s in load_states(pg)}  # chạy 1 lần ngoài vòng loop chính

    from_local = (now_utc - timedelta(hours=BACKFILL_LOOKBACK_HOURS)).astimezone(site_tz).date()
    to_local   = now_utc.astimezone(site_tz).date()
    shifts_by_day = load_shifts_by_date(pg, from_local - timedelta(days=1), to_local)

    total_segments = 0
    total_upserts  = 0

    try:
        if BACKFILL_ON_START:
            logger.info(f"Backfill enabled: lookback={BACKFILL_LOOKBACK_HOURS}h, block={EVENT_BLOCK_MIN}min")
            watermark = int(os.getenv("WATERMARK_SEC","120"))
            hard_to   = floor_to_minute(now_utc - timedelta(seconds=watermark))
            for dev_id, (line_id, machine_id) in device_map.items():
                last_end = get_last_event_end_ts(pg, line_id)
                if last_end is None:
                    start_from = hard_to - timedelta(hours=BACKFILL_LOOKBACK_HOURS)
                else:
                    start_from = floor_to_minute(last_end - timedelta(minutes=5))  # overlap 5'

                s = start_from
                while s < hard_to:
                    e = min(s + timedelta(minutes=EVENT_BLOCK_MIN), hard_to)
                    up , total_segments = process_one_device_window(dev_id, line_id, machine_id, s, e,
                                                   cas, pg, site_tz, reason_lookup, shifts_by_day, DRY_RUN, state_lookup)
                    total_upserts += up
                    logger.info(f"[LINE {line_id}] backfill block {s}..{e} -> upserts={up}")
                    s = e
        else:
            # window ngắn như trước
            from_min_utc, to_min_utc = minute_window_for_events(now_utc)
            for dev_id, (line_id, machine_id) in device_map.items():
                up, total_segments  = process_one_device_window(dev_id, line_id, machine_id, from_min_utc, to_min_utc,
                                               cas, pg, site_tz, reason_lookup, shifts_by_day, DRY_RUN, state_lookup)
                total_upserts += up

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
