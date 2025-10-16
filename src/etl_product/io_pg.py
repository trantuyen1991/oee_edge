# English comments per your style
from typing import Optional, Dict, Any, List, Tuple, Set
import os
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timedelta, timezone, date
import logging

try:
    # psycopg3 style error (psycopg 3.x)
    from psycopg.errors import StringDataRightTruncation, DataError as PsyDataError
except Exception:  # pragma: no cover
    # psycopg2 fallback (nếu đang dùng psycopg2)
    from psycopg2.errors import StringDataRightTruncation  # type: ignore
    from psycopg2 import DataError as PsyDataError  # type: ignore

def get_pg_conn():
    """
    Create a new PostgreSQL connection using env vars.
    """
    conn = psycopg.connect(
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASS"),
        autocommit=True,
    )
    return conn

UPSERT_FACT_MIN = """
-- Upsert 1-minute bucket into fact_production_min
INSERT INTO fact_production_min (       -- Insert new row into the fact table
    ts_min,                              -- Start time of 1-min bucket (UTC)
    line_id,                             -- Line identifier
    process_order,                       -- PO at that time
    packaging_id,                        -- Packaging FK
    produced,                            -- Total produced in this minute
    good,                                -- Good quantity in this minute
    ng,                                  -- Reject quantity in this minute
    runtime_sec,                         -- Runtime seconds in this minute
    planned_sec                          -- Planned seconds (usually 60)
) VALUES (
    %(ts_min)s,                          -- :utc minute boundary
    %(line_id)s,                         -- :line id
    %(process_order)s,                   -- :po
    %(packaging_id)s,                    -- :packaging
    %(produced)s,                        -- :produced
    %(good)s,                            -- :good
    %(ng)s,                              -- :ng
    %(runtime_sec)s,                     -- :runtime
    %(planned_sec)s                      -- :planned
)
ON CONFLICT (ts_min, line_id)            -- If the same minute+line already exists
DO UPDATE SET
    process_order = EXCLUDED.process_order,   -- overwrite context fields
    packaging_id  = EXCLUDED.packaging_id,
    produced      = EXCLUDED.produced,        -- overwrite numeric results
    good          = EXCLUDED.good,
    ng            = EXCLUDED.ng,
    runtime_sec   = EXCLUDED.runtime_sec,
    planned_sec   = EXCLUDED.planned_sec;
"""
def clamp_str(v, n):
    if v is None:
        return None
    return str(v)[:n]

def upsert_fact_min(conn, rows: List[Dict[str, Any]], logger) -> int:
    """
    Upsert a list of minute rows into fact_production_min.
    Returns number of affected rows.
    """
    if not rows:
        return 0
    with conn.cursor() as cur:
        sql = UPSERT_FACT_MIN
        # Sanity check
        assert isinstance(rows, list) and all(isinstance(r, dict) for r in rows)
        # (Optional) log gọn, không đụng vào rows
        # logger.debug(f"Upsert {len(rows)} rows into fact_production_min" )
        # logger.opt(lazy=True).debug("First row: {}", lambda: rows[0] if rows else None)
        # logger.opt(lazy=True).debug(f"SQL:\n{sql}" )
            
        for r in rows:
            r["process_order"] = clamp_str(r.get("process_order"), 50)   # VARCHAR(50)
        BATCH_SIZE = 1000
        for i in range(0, len(rows), BATCH_SIZE):
            cur.executemany(sql, rows[i:i+BATCH_SIZE])     
        # cur.executemany(sql, rows)
    return len(rows)

def load_packaging_snapshot(conn) -> Dict[int, Dict[str, Any]]:
    """
    Load minimal packaging dim snapshot (id -> dict) for quick lookup.
    Extend if you need more fields.
    """
    sql = "SELECT packaging_id, ideal_rate_per_min FROM dim_packaging"
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        return {r["packaging_id"]: r for r in cur.fetchall()}

def load_planned_reason_codes(conn) -> Set[int]:
    """
    Load all reason_code that are considered planned stop.
    Planned if dim_reason.is_planned = true OR dim_state.is_planned = true.
    Return set of integer reason codes (machineState values).
    """
    sql = """
    SELECT DISTINCT r.reason_code
    FROM dim_reason r
    JOIN dim_state s ON s.state_id = r.state_id
    WHERE COALESCE(r.is_planned, FALSE) = TRUE
       OR COALESCE(s.is_planned, FALSE) = TRUE
    """
    codes: Set[int] = set()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        for row in cur.fetchall():
            try:
                codes.add(int(row["reason_code"]))
            except (TypeError, ValueError):
                # ignore non-numeric reason_code (if any)
                pass
    return codes

def load_shifts_by_date(conn, start_date: date, end_date: date) -> Dict[date, List[Tuple[int, str, str]]]:
    """
    Load shift calendar in [start_date, end_date], grouped by date.
    Returns: { date: [(shift_no, start_time_str, end_time_str), ...] }
    Time strings are 'HH:MM:SS' in local site time.
    """
    sql = """
    SELECT shift_date, shift_no, start_time::text AS start_time, end_time::text AS end_time
    FROM dim_shift_calendar
    WHERE shift_date >= %s AND shift_date <= %s
    ORDER BY shift_date, shift_no
    """
    by_day: Dict[date, List[Tuple[int, str, str]]] = {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (start_date, end_date))
        for r in cur.fetchall():
            d = r["shift_date"]
            by_day.setdefault(d, []).append((r["shift_no"], r["start_time"], r["end_time"]))
    return by_day

def load_reason_lookup(conn) -> Dict[int, Tuple[Optional[int], Optional[int]]]:
    """
    Build mapping: reason_code(int) -> (reason_id, state_id)
    """
    sql = "SELECT reason_id, state_id, reason_code FROM dim_reason"
    mp: Dict[int, Tuple[Optional[int], Optional[int]]] = {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        for r in cur.fetchall():
            try:
                code = int(r["reason_code"])
            except (TypeError, ValueError):
                continue
            mp[code] = (r["reason_id"], r["state_id"])
    return mp

UPSERT_STATE_EVENT = """
INSERT INTO fact_state_event (
    line_id, machine_id, state_id, reason_id,
    start_ts, end_ts, shift_id, po, packaging_id, note
) VALUES (
    %(line_id)s, %(machine_id)s, %(state_id)s, %(reason_id)s,
    %(start_ts)s, %(end_ts)s, %(shift_id)s, %(po)s, %(packaging_id)s, %(note)s
)
ON CONFLICT (line_id, start_ts)
DO UPDATE SET
    end_ts       = GREATEST(fact_state_event.end_ts, EXCLUDED.end_ts),
    state_id     = COALESCE(EXCLUDED.state_id, fact_state_event.state_id),
    reason_id    = COALESCE(EXCLUDED.reason_id, fact_state_event.reason_id),
    po           = COALESCE(EXCLUDED.po, fact_state_event.po),
    packaging_id = COALESCE(EXCLUDED.packaging_id, fact_state_event.packaging_id),
    note         = COALESCE(EXCLUDED.note, fact_state_event.note);
"""

# def upsert_state_event_batch(conn, rows: List[Dict[str, Any]]) -> int:
#     """
#     Upsert a batch of state event rows using UNIQUE(line_id, start_ts).
#     """
#     if not rows:
#         return 0
#     with conn.cursor() as cur:
#         cur.executemany(UPSERT_STATE_EVENT, rows)
#     return len(rows)

def load_device_map(conn) -> Dict[str, Tuple[int, int | None]]:
    """
    Load TB device mapping: {device_uuid(str): (line_id, machine_id or None)}
    """
    sql = "SELECT device_id::text AS device_id, line_id, machine_id FROM dim_device"
    mapping: Dict[str, Tuple[int, int | None]] = {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        for r in cur.fetchall():
            mapping[r["device_id"]] = (int(r["line_id"]), r["machine_id"])
    return mapping

# --- NEW: get last end_ts for a line_id (for backfill bootstrap)
def get_last_event_end_ts(conn, line_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(end_ts) FROM fact_state_event WHERE line_id=%s", (line_id,))
        row = cur.fetchone()
        return row[0]  # may be None

def load_states(conn):
    """Return list of dicts: [{'state_id': 1, 'state_code': 'RUN'}, ...]"""
    sql = "SELECT state_id, state_code FROM dim_state;"
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()               # rows: list of tuples
    return [{'state_id': r[0], 'state_code': r[1]} for r in rows]

def _get_varchar_limits(conn, table: str) -> Dict[str, Optional[int]]:
    """
    Read VARCHAR length limits from information_schema for a given table.
    Returns {column_name: max_length or None (for TEXT)}.
    """
    sql = """
    SELECT column_name, data_type, character_maximum_length
    FROM information_schema.columns
    WHERE table_name = %s
    """
    limits: Dict[str, Optional[int]] = {}
    with conn.cursor() as cur:
        cur.execute(sql, (table,))
        for col, dtype, charlen in cur.fetchall():
            if dtype in ("character varying", "varchar"):
                limits[col] = int(charlen) if charlen is not None else None
            elif dtype == "text":
                limits[col] = None  # no hard limit
    return limits


def _row_len_report(row: Dict[str, Any], limits: Dict[str, Optional[int]]) -> List[str]:
    """
    Build a per-column length report for strings vs limits.
    """
    report = []
    for k, v in row.items():
        if k not in limits:
            continue
        maxlen = limits[k]
        if v is None:
            continue
        if isinstance(v, (str, bytes)):
            length = len(v) if isinstance(v, str) else len(v.decode(errors="ignore"))
            if (maxlen is not None) and (length > maxlen):
                report.append(f"{k} length {length} > limit {maxlen} | sample='{str(v)[:120]}'")
            else:
                report.append(f"{k} length {length}" + ("" if maxlen is None else f" (limit {maxlen})"))
    return report


def _binary_search_bad_row(conn, rows: List[Dict[str, Any]], logger: logging.Logger) -> Tuple[int, Exception]:
    """
    Binary search to find the first offending row that triggers DB error.
    Returns (index_in_rows, exception).
    """
    lo, hi = 0, len(rows) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        # thử insert 1 row tại mid
        try:
            with conn.cursor() as cur:
                cur.executemany(UPSERT_STATE_EVENT, [rows[mid]])
            conn.rollback()  # rollback test insert
            # nếu không lỗi -> lỗi nằm ở nửa trên
            lo = mid + 1
        except Exception as ex:  # có lỗi -> thu hẹp xuống nửa dưới (bao gồm mid)
            hi = mid - 1
            bad_idx = mid
            bad_ex = ex
            # nếu dải chỉ còn 1 phần tử
            if lo > hi:
                return bad_idx, bad_ex
    # fallback (không nên xảy ra)
    return -1, RuntimeError("Unable to isolate bad row")


def upsert_state_event_batch(
    conn,
    rows: List[Dict[str, Any]],
    logger: Optional[logging.Logger] = None,
    *,
    table_name: str = "fact_state_event",
    chunk_size: int = 2000,
    validate_schema: bool = True
) -> int:
    """
    Upsert a batch of state event rows using UNIQUE(line_id, start_ts).
    - Chunked executemany để dễ cô lập lỗi.
    - Khi gặp lỗi (ví dụ StringDataRightTruncation), chạy binary-search để tìm row vi phạm
      rồi log chi tiết: cột nào dài bao nhiêu / giới hạn bao nhiêu, sample giá trị.

    Parameters
    ----------
    conn : psycopg connection
    rows : list of dict
    logger : logging.Logger
    table_name : str
        Tên bảng để truy vấn metadata (giới hạn VARCHAR).
    chunk_size : int
        Kích thước lô cho executemany.
    validate_schema : bool
        Nếu True, đọc information_schema để thu thập giới hạn VARCHAR và log cảnh báo trước.

    Returns
    -------
    int : số row dự kiến upsert (nếu thành công toàn bộ).
    """
    if not rows:
        return 0

    _logger = logger or logging.getLogger(__name__)

    # Đọc giới hạn VARCHAR để hiển thị report khi có lỗi
    limits: Dict[str, Optional[int]] = {}
    if validate_schema:
        try:
            limits = _get_varchar_limits(conn, table_name)
            _logger.debug(f"[UPSERT_STATE_EVENT] Loaded column limits for {table_name}: {limits}")
        except Exception as ex:
            _logger.warning(f"[UPSERT_STATE_EVENT] Cannot load varchar limits for {table_name}: {ex}")
            limits = {}

    # Insert theo lô để dễ khoanh vùng lỗi
    total = 0
    for i in range(0, len(rows), chunk_size):
        batch = rows[i:i + chunk_size]
        try:
            with conn.cursor() as cur:
                cur.executemany(UPSERT_STATE_EVENT, batch)
            total += len(batch)
        except (StringDataRightTruncation, PsyDataError) as ex:
            _logger.error(
                f"[UPSERT_STATE_EVENT] Batch failed at rows[{i}:{i+len(batch)}], size={len(batch)}. "
                f"Error={type(ex).__name__}: {ex}"
            )
            # Cố gắng xác định cụ thể row lỗi bằng binary-search
            bad_rel_idx, bad_ex = _binary_search_bad_row(conn, batch, _logger)
            if bad_rel_idx >= 0:
                bad_abs_idx = i + bad_rel_idx
                bad_row = batch[bad_rel_idx]
                _logger.error(f"[UPSERT_STATE_EVENT] Offending row index={bad_abs_idx} (relative={bad_rel_idx})")

                # In báo cáo độ dài từng cột string so với limit
                if limits:
                    report = _row_len_report(bad_row, limits)
                    for line in report:
                        _logger.error(f"[UPSERT_STATE_EVENT] {line}")
                else:
                    # Không có metadata -> in sample các trường string
                    for k, v in bad_row.items():
                        if isinstance(v, (str, bytes)):
                            length = len(v) if isinstance(v, str) else len(v.decode(errors="ignore"))
                            sample = (v if isinstance(v, str) else v.decode(errors="ignore"))[:120]
                            _logger.error(f"[UPSERT_STATE_EVENT] {k} length={length} sample='{sample}'")

                # Gợi ý nhanh: nếu có cột 'note' hay 'po'
                for hint_col in ("note", "po", "reason_code", "state_code"):
                    if hint_col in bad_row:
                        val = bad_row[hint_col]
                        if isinstance(val, (str, bytes)):
                            s = val if isinstance(val, str) else val.decode(errors="ignore")
                            _logger.error(f"[UPSERT_STATE_EVENT] hint {hint_col}='{s[:180]}'")

            # Nâng lỗi để caller quyết định rollback/stop
            raise
        except Exception as ex:
            # Bắt mọi lỗi khác để log đàng hoàng
            _logger.exception(
                f"[UPSERT_STATE_EVENT] Unexpected error at rows[{i}:{i+len(batch)}], size={len(batch)}: {ex}"
            )
            raise

    return total

# from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
SITE_TZ = ZoneInfo(os.getenv("SITE_TIMEZONE", "Asia/Ho_Chi_Minh"))
def get_last_ts_min(pg: psycopg.Connection, line_id: int) -> datetime | None:
    """
    Get the latest processed minute (UTC, floor to minute) from fact_production_min for a line.
    
    Args:
        pg: psycopg connection.
        line_id: Production line id.

    Returns:
        datetime | None: Max(ts_min) in UTC if exists else None.
    """
    sql = "SELECT MAX(ts_min) FROM fact_production_min WHERE line_id = %(line_id)s"
    with pg.cursor() as cur:
        cur.execute(sql, {"line_id": line_id})
        row = cur.fetchone()
        ts = row[0] if row and row[0] is not None else None
        if ts is None:
            return None
        # ts trả về là naive (timestamp without time zone)
        if ts.tzinfo is None:
            # GIẢI THÍCH: giá trị thực tế lưu theo giờ địa phương
            ts = ts.replace(tzinfo=SITE_TZ)
        else:
            ts = ts.astimezone(SITE_TZ)
        # Chuyển về UTC để tính toán chuẩn
        ts_utc = ts.astimezone(timezone.utc)
        return ts_utc.replace(second=0, microsecond=0)

def compute_from_to_auto(pg: psycopg.Connection, line_id: int, cap_min: int) -> tuple[datetime, datetime, int, Optional[datetime]]:
    """
    Compute processing window [from_utc, to_utc) using fact_production_min watermark.
    - Uses 'cap_min' as the MAX backfill (your current BACKFILL_MIN).
    - If there is a gap smaller than cap, only process that gap.
    - If there's no history, process exactly 'cap_min' minutes.

    Args:
        pg: psycopg connection.
        line_id: Production line id.
        cap_min: Maximum minutes to backfill (use BACKFILL_MIN).

    Returns:
        (from_utc, to_utc, backfill_min)
    """
    # Use floored "now" to keep minute edges stable
    to_utc = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)

    last_ts = get_last_ts_min(pg, line_id)
    if last_ts is None:
        backfill_min = cap_min
    else:
        # Compute gap in minutes from last processed minute up to 'to_utc'
        gap = int((to_utc - last_ts).total_seconds() // 60)
        if gap < 1:
            # Nothing new; still process at least 1 minute to keep pipeline alive
            gap = 1
        # Clamp by cap
        backfill_min = min(cap_min, gap)

    from_utc = to_utc - timedelta(minutes=backfill_min)
    return from_utc, to_utc, backfill_min, last_ts
