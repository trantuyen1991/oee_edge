
from typing import Optional, Dict, Any, Iterable, List, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
# import psycopg2
# from psycopg2.extras import execute_values
# --- PostgreSQL drivers ---
# Ưu tiên psycopg3 nếu có (vì là bản mới)
# try:
#     import psycopg
#     from psycopg import extras as pg3_extras
# except ImportError:
#     pg3_extras = None
#     psycopg = None

# # Fallback cho psycopg2 (v2.x)
# try:
#     import psycopg2
#     from psycopg2 import extras as pg2_extras
#     from psycopg2.extras import execute_values  # optional: backward compatibility
# except ImportError:
#     pg2_extras = None
#     psycopg2 = None

def get_pg_engine(dsn: str, pool_size: int = 5, max_overflow: int = 5, timeout: int = 30) -> Engine:
    """
    Create and return a SQLAlchemy Engine for PostgreSQL.

    Args:
        dsn (str): PostgreSQL DSN, e.g. "postgresql+psycopg://user:pass@host:5432/db".
        pool_size (int): Base size of the connection pool.
        max_overflow (int): Additional connections allowed above pool_size.
        timeout (int): Connection timeout in seconds.

    Returns:
        sqlalchemy.engine.Engine: Configured engine instance.

    Example:
        engine = get_pg_engine(cfg.pg_dsn)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    """
    engine = create_engine(
        dsn,
        poolclass=QueuePool,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,          # validates connections before using
        pool_recycle=1800,           # recycle after 30 minutes
        connect_args={"connect_timeout": timeout},
        future=True,
    )
    return engine


def close_pg_engine(engine: Optional[Engine]) -> None:
    """
    Dispose the SQLAlchemy engine safely.

    Args:
        engine (Optional[Engine]): Engine to dispose.

    Returns:
        None

    Example:
        close_pg_engine(engine)
    """
    if engine:
        engine.dispose()


def pg_smoke_test(engine: Engine) -> None:
    """
    Run a lightweight connectivity test and raise on failure.

    Args:
        engine (Engine): Engine to test.

    Returns:
        None

    Example:
        pg_smoke_test(engine)
    """
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

def get_last_event(pg_engine, line_id: int) -> Optional[Dict[str, Any]]:
    sql = text("""
        -- Lấy event cuối cùng cho line_id = :line_id, trả về epoch ms
        SELECT event_id, line_id, machine_id, state_id, reason_id,
        -- EXTRACT(EPOCH FROM start_ts) * 1000::bigint AS start_ms,  -- convert to ms
        -- EXTRACT(EPOCH FROM end_ts)   * 1000::bigint AS end_ms,    -- convert to ms
        (start_ts AT TIME ZONE 'Asia/Ho_Chi_Minh') AS start_ts,  -- giả định giá trị đang ở giờ VN
        (end_ts   AT TIME ZONE 'Asia/Ho_Chi_Minh') AS end_ts,    -- trả về TIMESTAMPTZ (UTC instant)
        duration_sec, shift_id, po, packaging_id, note            -- fields khác giữ nguyên
        FROM fact_state_event
        WHERE line_id = :line_id                                     -- lọc theo line
        ORDER BY end_ts DESC                                         -- lấy event mới nhất
        LIMIT 1;                                                     -- chỉ 1 dòng
    """)
    with pg_engine.connect() as conn:
        row = conn.execute(sql, {"line_id": line_id}).mappings().first()
        return dict(row) if row else None


UpRow = Dict[str, Any]

# UPSERT_SQL = """
# INSERT INTO fact_state_event
# (hash_key,device_uuid, line_id, machine_id, state_id, reason_id,
#  start_ts, end_ts, shift_id, po, packaging_id, note, updated_at)
# VALUES (
#     %s, %s, %s, %s, %s, %s,
#     %s, %s, %s, %s, %s, %s, now()
# )
# ON CONFLICT (hash_key) DO UPDATE SET
#   line_id       = EXCLUDED.line_id,
#   machine_id    = EXCLUDED.machine_id,
#   state_id      = EXCLUDED.state_id,
#   reason_id     = EXCLUDED.reason_id,
#   start_ts      = EXCLUDED.start_ts,
#   end_ts        = EXCLUDED.end_ts,
#   shift_id      = EXCLUDED.shift_id,
#   po            = EXCLUDED.po,
#   packaging_id  = EXCLUDED.packaging_id,
#   note          = EXCLUDED.note,
#   updated_at    = now()
# """

# def upsert_events(
#     pg_engine,
#     rows: List[UpRow],
#     batch_size: int = 1000,
#     dry_run: bool = False,
#     logger=None
# ) -> int:
#     # if not rows:
#     #     return 0

#     # cols = [
#     #     "hash_key","device_uuid","line_id","machine_id","state_id","reason_id",
#     #     "start_ts","end_ts","duration_sec","shift_id","po","packaging_id","note"
#     # ]

#     # total = 0
#     # with pg_engine.begin() as conn:
#     #     raw = conn.connection  # psycopg2 connection
#     #     cur = raw.cursor()
#     #     for i in range(0, len(rows), batch_size):
#     #         chunk = rows[i:i+batch_size]
#     #         values = [
#     #             tuple(r.get(c) for c in cols)
#     #             for r in chunk
#     #         ]
#     #         first = values[0] if values else None
#     #         if dry_run:
#     #             if logger:
#     #                 logger.info("DRY-RUN upsert {} rows: First row {}", len(values), first)
#     #             continue
#     #         logger.debug("PostpreSQL upsert {} rows: First row {}", len(values), first)
#     #         execute_values(cur, UPSERT_SQL, values, page_size=len(values))
#     #         total += len(values)
#     #     if not dry_run:
#     #         raw.commit()
#     # return total
#     if not rows:
#         logger.debug("upsert_events 0 row -> Skip")
#         return 0

#     cols = [
#         "hash_key","device_uuid","line_id","machine_id","state_id","reason_id",
#         "start_ts","end_ts","shift_id","po","packaging_id","note"
#     ]

#     total = 0
#     with pg_engine.begin() as conn:
#         # lấy **psycopg v3** connection
#         raw = conn.connection
#         with raw.cursor() as cur:
#             for i in range(0, len(rows), batch_size):
#                 chunk = rows[i:i+batch_size]
#                 values = [tuple(r.get(c) for c in cols) for r in chunk]

#                 if dry_run:
#                     if logger:
#                         logger.info("DRY-RUN upsert {} rows; First row {}", len(values), values[0] if values else None)
#                     continue
#                 logger.debug("PostpreSQL upsert {} rows; First row {}", len(values), values[0] if values else None)
#                 # psycopg v3: executemany là đủ nhanh cho ~1k rows/batch
#                 cur.executemany(UPSERT_SQL, values)
#                 total += len(values)
#     return total

# --- psycopg2 (v2) optional ---
try:
    import psycopg2  # noqa
    from psycopg2.extras import execute_values as pg2_execute_values  # type: ignore
except Exception:
    pg2_execute_values = None

# --- psycopg (v3) optional ---
try:
    import psycopg  # noqa
    from psycopg.extras import execute_values as pg3_execute_values  # type: ignore
except Exception:
    pg3_execute_values = None

# Câu lệnh upsert dùng VALUES %s (hợp lệ cho cả execute_values v2/v3)
UPSERT_SQL = """
INSERT INTO fact_state_event
(hash_key, device_uuid, line_id, machine_id, state_id, reason_id,
 start_ts, end_ts, shift_id, po, packaging_id, note) 
VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s
)
ON CONFLICT (hash_key) DO UPDATE SET
  line_id       = EXCLUDED.line_id,
  machine_id    = EXCLUDED.machine_id,
  state_id      = EXCLUDED.state_id,
  reason_id     = EXCLUDED.reason_id,
  start_ts      = EXCLUDED.start_ts,
  end_ts        = EXCLUDED.end_ts,
  shift_id      = EXCLUDED.shift_id,
  po            = EXCLUDED.po,
  packaging_id  = EXCLUDED.packaging_id,
  note          = EXCLUDED.note
"""

# def upsert_events(pg_engine, rows, batch_size=1000, dry_run=False, logger=None) -> int:
#     if not rows:
#         return 0

#     _ROW_COLS = [
#         "hash_key", "line_id", "machine_id", "state_id", "reason_id",
#         "start_ts", "end_ts", "shift_id", "po", "packaging_id", "note"
#     ]

#     total = 0
#     # with pg_engine.begin() as conn:           # SQLAlchemy Connection (tự commit khi không lỗi)
#     #     raw = conn.connection                  # DBAPI connection (psycopg2 hoặc psycopg3)
#     #     with raw.cursor() as cur:
#     #         for i in range(0, len(rows), batch_size):
#     #             chunk = rows[i:i+batch_size]
#     #             # map dict -> tuple đúng thứ tự cột
#     #             values = [tuple(r.get(c) for c in _ROW_COLS) for r in chunk]

#     #             if dry_run:
#     #                 if logger:
#     #                     logger.info("DRY-RUN upsert {} rows; First row {}", len(values), values[0] if values else None)
#     #                 continue

#     #             # Nhận diện driver & chọn đúng execute_values
#     #             logger.debug("Detected connection driver: {}", type(raw))
#     #             module = raw.__class__.__module__
#     #             logger.debug("Detected DBAPI module: {}", module)

#     #             if "psycopg2" in module and pg2_extras is not None:
#     #                 pg2_extras.execute_values(cur, UPSERT_SQL, values, page_size=len(values))
#     #                 logger.debug("STEP-11: psycopg2 execute_values")

#     #             elif "psycopg" in module and pg3_extras is not None:
#     #                 pg3_extras.execute_values(cur, UPSERT_SQL, values, page_size=len(values))
#     #                 logger.debug("STEP-11: psycopg3 execute_values")

#     #             else:
#     #                 cur.executemany(
#     #                     UPSERT_SQL.replace("VALUES %s",
#     #                                     "VALUES (" + ",".join(["%s"]*len(_ROW_COLS)) + ")"),
#     #                     values
#     #                 )
#     #                 logger.debug("STEP-11: cur.executemany (fallback)")

#     #             total += len(values)

#     with pg_engine.begin() as conn:                  # transaction do SQLAlchemy quản lý
#         # Bóc DBAPI connection thật
#         raw = conn.connection
#         if hasattr(raw, "driver_connection"):
#             raw = raw.driver_connection
#         elif hasattr(raw, "connection"):
#             raw = raw.connection

#         logger.debug("Detected connection driver: {}", type(raw))
#         logger.debug("Detected DBAPI module: {}", raw.__class__.__module__)

#         with raw.cursor() as cur:
#             for i in range(0, len(rows), batch_size):
#                 chunk  = rows[i:i+batch_size]
#                 values = [tuple(r.get(c) for c in _ROW_COLS) for r in chunk]

#                 if dry_run:
#                     logger.info("DRY-RUN upsert {} rows; First row {}", len(values), values[0] if values else None)
#                     continue

#                 module = raw.__class__.__module__
#                 if "psycopg2" in module and pg2_extras is not None:
#                     pg2_extras.execute_values(cur, UPSERT_SQL, values, page_size=len(values))
#                     logger.debug("STEP-11: psycopg2 execute_values")

#                 elif "psycopg" in module and "psycopg2" not in module and pg3_execute_values is not None:
#                     pg3_execute_values(cur, UPSERT_SQL, values, page_size=len(values))
#                     logger.debug("STEP-11: psycopg3 execute_values")

#                 else:
#                     cur.executemany(
#                         UPSERT_SQL.replace("VALUES %s",
#                                         "VALUES (" + ",".join(["%s"]*len(_ROW_COLS)) + ")"),
#                         values
#                     )
#                     logger.debug("STEP-11: cur.executemany (fallback)")


#                 total += len(values)
#     if logger:
#         logger.info("STEP-11: Upserted rows -> {}", total)
#     return total

def upsert_events(
    pg_engine: Engine,
    rows: List[Dict[str, Any]],
    batch_size: int = 1000,
    dry_run: bool = False,
    logger=None,
) -> int:
    if not rows:
        return 0
    _ROW_COLS = [
        "hash_key", "device_uuid", "line_id", "machine_id", "state_id", "reason_id",
        "start_ts", "end_ts", "shift_id", "po", "packaging_id", "note"
    ]
    total = 0
    with pg_engine.begin() as conn:  # SQLAlchemy transaction; auto-commit on success
        raw = conn.connection          # unwrap DBAPI connection (psycopg2/psycopg3)
        if logger:
            logger.debug("db_pg:upsert_events: Detected connection driver: {}", type(raw))
            logger.debug("db_pg:upsert_events: Detected DBAPI module: {}", raw.__class__.__module__.split(".")[0])

        with raw.cursor() as cur:
            for i in range(0, len(rows), batch_size):
                chunk = rows[i:i+batch_size]

                # map dict -> tuple theo đúng thứ tự cột + cột updated_at (server side now() trong ON CONFLICT)
                values = [
                    tuple(r.get(c) for c in _ROW_COLS)
                    for r in chunk
                ]

                if dry_run:
                    if logger:
                        logger.info("DRY-RUN upsert {} rows; First row {}", len(values), values[0] if values else None)
                    continue

                # --- chọn nhánh nhanh nhất có thể ---
                module = raw.__class__.__module__
                try:
                    if module.startswith("psycopg2") and pg2_execute_values is not None:
                        # psycopg2
                        pg2_execute_values(cur, UPSERT_SQL, values, page_size=len(values))
                        if logger: logger.debug("STEP-11: psycopg2 execute_values")
                    elif module.startswith("psycopg") and "psycopg2" not in module and pg3_execute_values is not None:
                        # psycopg3
                        pg3_execute_values(cur, UPSERT_SQL, values, page_size=len(values))
                        if logger: logger.debug("STEP-11: psycopg3 execute_values")
                    else:
                        # Fallback: executemany VALUES (...)
                        sql = UPSERT_SQL.replace(
                            "VALUES %s",
                            "VALUES (" + ",".join(["%s"] * len(_ROW_COLS)) + ")"
                        )
                        cur.executemany(sql, values)
                        if logger: logger.debug("STEP-11: cur.executemany (fallback)")
                except Exception as ex:
                    # throw kèm log để dễ lần dấu
                    if logger:
                        logger.exception("Upsert error on batch ({} rows): {}", len(values), ex)
                    raise

                total += len(values)

    if logger:
        logger.info("STEP-11: Upserted rows -> {}", total)
    return total