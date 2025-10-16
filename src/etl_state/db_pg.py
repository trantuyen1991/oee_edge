import time
from typing import Optional, Dict, Any, Iterable, List, Tuple
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool


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

_ROW_COLS = [
        "hash_key", "device_uuid", "line_id", "machine_id", "state_id", "reason_id",
        "start_ts", "end_ts", "shift_id", "po", "packaging_id", "note"
    ]

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

def _row_tuple(r: Dict[str, Any]) -> tuple:
    """Convert dictionary -> tuple theo thứ tự _ROW_COLS"""
    return tuple(r.get(c) for c in _ROW_COLS)

def upsert_events(pg_engine: Engine, rows: List[Dict[str, Any]], page_size: int = 1000, dry_run= False, logger = None) -> int:
    """
    Fast UPSERT (INSERT ON CONFLICT) sử dụng psycopg3 và executemany()
    Args:
        pg_engine: SQLAlchemy engine (postgresql+psycopg)
        rows: Danh sách các dict chứa dữ liệu cần upsert
        page_size: Số bản ghi mỗi batch
    Returns:
        Tổng số bản ghi đã ghi
    """
    if not rows:
        logger.info("STEP-11: No rows to upsert.")
        return 0

    total = 0
    start_all = time.time()

    with pg_engine.begin() as sa_conn:
        raw = sa_conn.connection.driver_connection
        driver = type(raw).__module__
        logger.debug(f"STEP-11: Using DBAPI driver: {driver}")
        logger.debug(f"STEP-11: Raw connection type: {type(raw)}")

        for i in range(0, len(rows), page_size):
            batch = rows[i:i + page_size]
            values = [_row_tuple(r) for r in batch]
            t0 = time.time()
            try:
                if dry_run:
                    logger.info(
                        f"STEP-11: DRY_RUN: First row -> upserted {values[0] if values else None} rows "
                )
                else:
                    with raw.cursor() as cur:
                        cur.executemany(UPSERT_SQL, values)
                batch_time = time.time() - t0
                logger.info(
                    f"STEP-11: Batch {i//page_size+1:03d} -> upserted {len(values)} rows "
                    f"in {batch_time:.3f}s"
                )
            except Exception as ex:
                logger.exception(
                    f"STEP-11: Upsert error on batch {i//page_size+1} ({len(values)} rows): {ex}"
                )
                raise
            total += len(values)
            
    elapsed = time.time() - start_all
    logger.info(f"✅ STEP-11: Upsert completed: {total} rows in {elapsed:.2f}s")
    return total
