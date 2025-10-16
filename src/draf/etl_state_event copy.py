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

from io_cas import (
    get_cas_session, 
    fetch_timeseries_numeric, 
    fetch_timeseries_text,
    fetch_state_with_quality,
    get_partitions
    )
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
def build_online_windows_from_quality(cas, dev_id: str, s_ms: int, e_ms: int)-> list[tuple[int,int,bool]]:
    """
    Dựng (start, end, is_online) dựa trên 'điểm xấu' thực sự:
      - Bad điểm: long_v is NULL hoặc str_v bắt đầu bằng 'bad'.
      - Offline = các khoảng [bad_ts .. next_good_ts).
      - Nếu không có bad nào trong block -> toàn bộ online.
      - Đọc dư mép trái 2 phút để bắt chuyển trạng thái đúng tại đầu block.
    """
    left = s_ms - 120_000   # đọc dư mép trái
    rows = fetch_state_with_quality(cas, dev_id, ENTITY_KEY_STATE, left, e_ms)  # [(ts, long_v, str_v)] đã sort

    if not rows:
        # Không có dữ liệu quality -> mặc định online để tránh âm tính giả
        return [(s_ms, e_ms, True)]

    def is_bad(long_v, str_v):
        if long_v is None:
            return True
        if str_v:
            sv = str(str_v).strip().lower()
            if sv.startswith("bad"):
                return True
        return False

    # Trình tự: tìm các "bad streak" rồi lấy phần bù là online
    wins = []
    cur = s_ms
    bad_start = None

    for ts, lv, sv in rows:
        bad = is_bad(lv, sv)

        # Bỏ mọi điểm xảy ra trước s_ms (chỉ dùng để xác định trạng thái đang dở)
        if ts < s_ms:
            # nếu đang ở bad trước s_ms thì mở bad từ s_ms
            if bad:
                bad_start = s_ms
            continue

        if bad and bad_start is None:
            # đóng đoạn online trước đó (nếu có)
            if ts > cur:
                wins.append((cur, ts, True))
            bad_start = ts

        elif (not bad) and (bad_start is not None):
            # đóng đoạn offline đến điểm tốt
            wins.append((bad_start, ts, False))
            cur = ts
            bad_start = None

    # Kết thúc block
    if bad_start is not None:
        wins.append((bad_start, e_ms, False))
    else:
        if cur < e_ms:
            wins.append((cur, e_ms, True))

    # Gom các đoạn liên tiếp có cùng flag
    merged = []
    for a, b, flag in wins:
        if not merged or flag != merged[-1][2] or a > merged[-1][1]:
            merged.append([a, b, flag])
        else:
            merged[-1][1] = max(merged[-1][1], b)
    return [(a, b, f) for a, b, f in merged]

def choose_latest_before(ms: int, points: List[Tuple[int, Any]]) -> Optional[Any]:
    """
    Given points [(ts_ms, val)...], pick the latest val where ts_ms <= ms.
    """
    for ts, val in reversed(sorted(points, key=lambda x: x[0])):
        if ts <= ms:
            return val
    return None

def _clip_str(val, maxlen):
    if val is None:
        return None
    s = str(val)
    return s if len(s) <= maxlen else s[:maxlen]
def _sanitize_po(val: Optional[str], maxlen: int = 64) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    # Bỏ mọi chuỗi Bad… từ TB/OPC UA
    if s.lower().startswith("bad"):
        return None
    # Clip chiều dài để tránh tràn DB
    return s if len(s) <= maxlen else s[:maxlen]
def _safe_po(val):
    """Return sanitized PO; only keep meaningful strings."""
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ("default", "none", "null"):
        return None
    if s.lower().startswith("bad"):  # từ TB khi OPC lỗi
        return None
    return s[:64]  # clip an toàn
def get_last_event_before(pg, line_id: int, ts_utc: datetime):
    dt_naive = ts_utc.astimezone(timezone.utc).replace(tzinfo=None)
    with pg.cursor() as cur:
        cur.execute("""
            SELECT start_ts, end_ts, reason_id, state_id
            FROM fact_state_event
            WHERE line_id = %s AND end_ts <= %s
            ORDER BY end_ts DESC
            LIMIT 1
        """, (line_id, dt_naive))
        r = cur.fetchone()
        if not r:
            return None
        cols = [desc[0] for desc in cur.description]
        return dict(zip(cols, r))

def process_one_device_window(dev_id: str,
                              line_id: int,
                              machine_id: Optional[int],
                              from_min_utc: datetime,
                              to_min_utc: datetime,
                              cas, pg, site_tz, reason_lookup, shifts_by_day, DRY_RUN, state_lookup) -> int:
    """Return number of rows upserted."""
    start_ms = to_epoch_ms(from_min_utc)
    end_ms   = to_epoch_ms(to_min_utc)
    if end_ms <= start_ms:
        logger.warning(f"[LINE {line_id}] skip inverted window {from_min_utc} -> {to_min_utc}")
        return 0, 0
     # === NEW: Seed continuity from previous event in DB ===
    seed = get_last_event_before(pg, line_id, from_min_utc)
    logger.info(f"[LINE {line_id}] from_min_utc {from_min_utc} get_last_event_before {seed}")
    seed_reason, seed_state, seed_end, seed_start = None, None, None, None
    if seed:
        seed_reason, seed_state, seed_end, seed_start = (
            seed["reason_id"], seed["state_id"], seed["end_ts"], seed["start_ts"]
        )
        # chỉ dùng seed nếu event kết thúc gần cửa sổ (ví dụ trong 30 phút)
        gap = (from_min_utc - seed_end.replace(tzinfo=timezone.utc)).total_seconds()
        if gap > 1800:
            seed = None
        else:
            logger.debug(f"[LINE {line_id}] Seed continuity: "
                         f"reason={seed_reason}, state={seed_state}, "
                         f"seed_end={seed_end}, gap={gap/60:.1f} min")
    # 1) read machineState for window
    SEED_LOOKBACK_MIN = int(os.getenv("QUALITY_SEED_LOOKBACK_MIN", "720"))
    seed_from = start_ms - SEED_LOOKBACK_MIN * 60_000

    # logger.debug(f"[DEBUG] Query window UTC: {datetime.fromtimestamp(start_ms/1000, tz=timezone.utc)} -> {datetime.fromtimestamp(end_ms/1000, tz=timezone.utc)}")
    rows_q = fetch_state_with_quality(cas, dev_id, ENTITY_KEY_STATE, seed_from, end_ms)  # [(ts, long_v, str_v)]
    rows_q.sort(key=lambda x: x[0])

    # Tạo seed_value = trạng thái ngay tại start_ms
    seed_val = None
    for ts, lv, sv in reversed(rows_q):
        if ts <= start_ms:
            seed_val = (COMM_LOSS_CODE if lv is None else int(lv))
            break
    if seed_val is None:
        # Không có gì trước start_ms → coi OFFLINE (có thể cho phép bật/tắt qua env)
        if os.getenv("QUALITY_NO_DATA_IS_OFFLINE", "1") in ("1","true","yes","on"):
            seed_val = COMM_LOSS_CODE

    # Xây series từ seed + các điểm trong [start_ms, end_ms)
    state_series: List[tuple[int,int]] = []
    if seed_val is not None:
        state_series.append((start_ms, seed_val))
    for ts, lv, _ in rows_q:
        if ts >= start_ms and ts < end_ms:
            code = COMM_LOSS_CODE if lv is None else int(lv)
            # Bỏ bớt các điểm trùng code để nhẹ series (tuỳ chọn)
            if not state_series or state_series[-1][1] != code:
                state_series.append((ts, code))

    if not state_series:
        logger.info(f"[LINE {line_id}] no state points or seed in window -> skip")
        return 0, 0

        # === NEW: stitch seed with current window if same state ===
    if seed and state_series:
        first_code = state_series[0][1]
        # Nếu code đầu cửa sổ giống với trạng thái trước đó -> nối tiếp
        if first_code == seed_reason:
            logger.debug(f"[LINE {line_id}] Stitching continuity with previous event")
            # thêm một điểm giả tại thời điểm seed_end để nối liền
            state_series.insert(0, (int(seed_end.timestamp() * 1000), seed_reason))

    # (optional) read context
    po_points    = fetch_timeseries_text(  cas, dev_id, ENTITY_KEY_PO,   start_ms-120000, end_ms+120000)
    pack_points  = fetch_timeseries_numeric(cas, dev_id, ENTITY_KEY_PACK,start_ms-120000, end_ms+120000)
    note         = None

    # 2) compress into segments
    # segs = compress_state_series(state_series, start_ms, end_ms)
    segs = compress_state_series(series=state_series, start_ms=start_ms, end_ms=end_ms)
    online_windows = build_online_windows_from_quality(cas, dev_id, start_ms, end_ms)
    # logger.info(f"[LINE {line_id}] segs={segs} online_windows={online_windows}")


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
            po = _safe_po(po)
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
            note_text = f"{state_code}" if note_text else state_code # ({note_text})

            rows.append({
                "line_id": line_id,
                "machine_id": machine_id,
                "state_id": state_id,
                "reason_id": reason_id,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "shift_id": shift_id,
                "po": _sanitize_po(po,64),
                "packaging_id": packaging_id,
                "note": state_code
            })

    # === NEW: extend first segment if same state as previous event ===
    if seed and rows:
        first = rows[0]
        if first["reason_id"] == seed_reason and first["state_id"] == seed_state:
            first["start_ts"] = seed_start
            logger.debug(f"[LINE {line_id}] Extended first segment start_ts "
                         f"back to {seed_start} (continuity merge)")
        
    if not rows:
        return 0, len(segs)
    if  DRY_RUN:
        for r in rows:
            logger.info(f"[LINE {line_id}] {r['start_ts'].isoformat()} -> {r['end_ts'].isoformat()} "
                        f"reason={r['reason_id']} state={r['state_id']} shift_id={r['shift_id']} note={r['note']}")
        return 0 , len(segs)

    affected = 0
    affected = upsert_state_event_batch(pg, rows)
    return affected, len(segs)

def to_naive_utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt

def to_naive_local(dt: datetime, site_tz) -> datetime:
    return dt.astimezone(site_tz).replace(tzinfo=None) if dt.tzinfo else dt

def delete_pg(pg, line_id, start_from, hard_to, site_tz):
    # Cửa sổ xóa theo 2 “hệ quy chiếu”
    f_utc = to_naive_utc(start_from)
    t_utc = to_naive_utc(hard_to)
    f_loc = to_naive_local(start_from, site_tz)
    t_loc = to_naive_local(hard_to, site_tz)

    deleted_total = 0
    with pg.cursor() as cur:
        # 1) Xóa theo UTC-naive (chuẩn mới)
        cur.execute("""
            DELETE FROM fact_state_event
            WHERE line_id = %s
            AND start_ts < %s   -- to_ts
            AND end_ts   > %s   -- from_ts
        """, (line_id, t_utc, f_utc))
        deleted_total += cur.rowcount

        # 2) Xóa theo LOCAL-naive (legacy đã lưu theo giờ VN)
        cur.execute("""
            DELETE FROM fact_state_event
            WHERE line_id = %s
            AND start_ts < %s
            AND end_ts   > %s
        """, (line_id, t_loc, f_loc))
        deleted_total += cur.rowcount

    pg.commit()
    logger.info(f"[LINE {line_id}] deleted {deleted_total} events "
                f"(UTC window {f_utc}→{t_utc}, LOCAL window {f_loc}→{t_loc})")

# etl_state_event.py (hoặc io_pg.py nếu bạn để helper PG ở đó)
from datetime import timezone, timedelta

def get_last_event_end_ts_v1(pg, line_id: int, site_tz, hard_to_utc) -> datetime | None:
    """
    Trả về mốc end_ts *aware UTC* của event cuối cùng cho line_id, chịu được dữ liệu
    đã từng lưu theo LOCAL-naive (UTC+7) và dữ liệu mới theo UTC-naive.
    - Đọc top 5 end_ts mới nhất (naive).
    - Tạo 2 diễn giải cho mỗi end_ts: (UTC-naive) và (LOCAL-naive→UTC).
    - Ưu tiên ứng viên <= hard_to_utc + 1 phút; chọn cái muộn nhất.
    - Nếu không có ứng viên hợp lệ, chọn cái gần hard_to_utc nhất.
    - Luôn clamp về <= hard_to_utc.
    """
    with pg.cursor() as cur:
        cur.execute("""
            SELECT end_ts
            FROM fact_state_event
            WHERE line_id = %s
            ORDER BY end_ts DESC
            LIMIT 5
        """, (line_id,))
        rows = cur.fetchall()

    if not rows:
        return None

    candidates = []
    for (end_naive,) in rows:
        if end_naive is None:
            continue
        # Giải thích 1: end_ts là UTC-naive
        e_utc = end_naive.replace(tzinfo=timezone.utc)
        candidates.append(("utc", e_utc))
        # Giải thích 2: end_ts là LOCAL-naive (di sản), đổi về UTC
        e_loc = end_naive.replace(tzinfo=site_tz).astimezone(timezone.utc)
        candidates.append(("local", e_loc))

    if not candidates:
        return None

    EPS = timedelta(minutes=1)
    # Ưu tiên ứng viên không vượt quá hard_to + EPS
    valid = [(tag, dt) for tag, dt in candidates if dt <= hard_to_utc + EPS]
    if valid:
        chosen = max(valid, key=lambda x: x[1])[1]
    else:
        # Không có ứng viên hợp lệ: lấy cái gần hard_to nhất (nhưng vẫn clamp)
        chosen = min(candidates, key=lambda x: abs((x[1] - hard_to_utc).total_seconds()))[1]

    return min(chosen, hard_to_utc)

def main():
    # ----STEP-00------------ LOGGER CONFIGURATION ----------------
    load_dotenv()
    os.makedirs("logs", exist_ok=True)
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
        # ---------------- BACKFILL ON START (gọn – chuẩn UTC) ----------------
        if BACKFILL_ON_START:
            # 1) Thời gian chuẩn UTC
            watermark = int(os.getenv("WATERMARK_SEC", "120"))               # lag để tránh dữ liệu đang đến
            hard_to  = datetime.utcnow().replace(tzinfo=timezone.utc)        # luôn UTC aware
            # hard_to  = floor_to_minute(now_utc - timedelta(seconds=watermark))

            logger.info(f"Backfill enabled: lookback={BACKFILL_LOOKBACK_HOURS}h, watermark={watermark}s "
                        f"(UTC hard_to={hard_to}, Local hard_to={hard_to.astimezone(site_tz)})")

            # 2) Tiện ích chuyển về naive-UTC khi nói chuyện với Postgres (timestamp without time zone)
            def to_naive_utc(dt: datetime) -> datetime:
                return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt

            total_segments = 0
            total_upserts  = 0

            # 3) Quét từng device/line
            for dev_id, (line_id, machine_id) in device_map.items():
                # 3.1) Lấy mốc cuối cùng đã ghi trong DB (naive → hiểu là UTC)
                last_end = get_last_event_end_ts(pg, line_id)  # có thể None
                if last_end is not None:
                    # chuẩn hóa thành UTC aware và kẹp không vượt hard_to
                    last_end_utc = (last_end.replace(tzinfo=timezone.utc)
                                    if last_end.tzinfo is None else last_end.astimezone(timezone.utc))
                    last_end_utc = min(last_end_utc, hard_to)
                else:
                    last_end_utc = None

                # 3.2) Xác định from/to cho backfill
                #    - Nếu chưa từng có dữ liệu → backfill full lookback 48h
                #    - Nếu đã có → backfill từ (last_end - 5 phút) đến hard_to để bù trễ/ngắt quãng
                # if last_end_utc is None:
                start_from = hard_to - timedelta(hours=BACKFILL_LOOKBACK_HOURS)
                # else:
                #     start_from = floor_to_minute(last_end_utc - timedelta(minutes=5))  # overlap nhẹ

                # Guard: nếu vì lý do nào đó from >= to (ngược), kéo lùi 1h cho an toàn
                if start_from >= hard_to:
                    logger.warning(f"[LINE {line_id}] start_from >= hard_to ({start_from} >= {hard_to}) → adjust by -1h")
                    start_from = floor_to_minute(hard_to - timedelta(hours=1))

                # Log song song UTC & Local để dễ so sánh với UI
                logger.info(
                    f"[LINE {line_id}] Backfill window: "
                    f"UTC {start_from} → {hard_to} | "
                    f"Local {start_from.astimezone(site_tz)} → {hard_to.astimezone(site_tz)}"
                )

                # 3.3) Dọn chồng chéo trong khoảng [start_from, hard_to) trước khi insert lại
                if not DRY_RUN:
                    delete_pg(pg, line_id, start_from, hard_to, site_tz)
                # 3.4) Xử lý 1 phát toàn cửa sổ (không chia block)
                up, segs = process_one_device_window(
                    dev_id, line_id, machine_id,
                    from_min_utc=start_from,
                    to_min_utc=hard_to,
                    cas=cas, pg=pg, site_tz=site_tz,
                    reason_lookup=reason_lookup, shifts_by_day=shifts_by_day,
                    DRY_RUN=DRY_RUN, state_lookup=state_lookup
                )
                total_upserts  += up
                total_segments += segs

            logger.info(f"Backfill done. Segments={total_segments}, Upserts={total_upserts}, DRY_RUN={DRY_RUN}")
            return
        # ---------------- END BACKFILL ON START ----------------
        else:
            # ----- STREAMING WINDOW (per-line), không dùng minute_window_for_events -----
            watermark_sec = int(os.getenv("WATERMARK_SEC", "120"))
            overlap_min   = int(os.getenv("EVENT_BACK_MIN", "5"))  # 5–6 phút là hợp lý

            now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
            hard_to = floor_to_minute(now_utc - timedelta(seconds=watermark_sec))  # mốc chốt dữ liệu an toàn

            total_segments = 0
            for dev_id, (line_id, machine_id) in device_map.items():
                # 1) lấy event cuối trước hard_to để quyết định from_min_utc
                # last_end = get_last_event_end_ts(pg, line_id)  # naive (UTC)
                last_end = get_last_event_end_ts_v1(pg, line_id, site_tz, hard_to)  # mới
                logger.debug(f"get_last_event_end_ts: last_end {last_end}")
                if last_end:
                    last_end_utc = (last_end.replace(tzinfo=timezone.utc)
                                    if last_end.tzinfo is None else last_end.astimezone(timezone.utc))
                    last_end_utc = min(last_end_utc, hard_to)
                    from_min_utc = floor_to_minute(last_end_utc - timedelta(minutes=overlap_min))
                else:
                    # lần đầu chưa có dữ liệu: lấy lookback rộng (ví dụ 48h) để dựng liên tục
                    lookback_h = int(os.getenv("BACKFILL_LOOKBACK_HOURS", "48"))
                    from_min_utc = hard_to - timedelta(hours=lookback_h)

                # Guard nếu lỡ ngược
                if from_min_utc >= hard_to:
                    from_min_utc = floor_to_minute(hard_to - timedelta(minutes=overlap_min))

                # 2) XÓA overlap trước khi ghi (UTC-naive + dọn legacy local-naive)
                if not DRY_RUN:
                    delete_pg(pg, line_id, from_min_utc, hard_to, site_tz)

                # 3) Xử lý 1 phát theo cửa sổ đã tính (đã có seed continuity trong process_one_device_window)
                up, segs = process_one_device_window(
                    dev_id, line_id, machine_id,
                    from_min_utc=from_min_utc,
                    to_min_utc=hard_to,
                    cas=cas, pg=pg, site_tz=site_tz,
                    reason_lookup=reason_lookup, shifts_by_day=shifts_by_day,
                    DRY_RUN=DRY_RUN, state_lookup=state_lookup
                )
                total_upserts += up
                total_segments += segs

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
