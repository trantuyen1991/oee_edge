
from typing import Optional, Dict, Any, Iterable, List, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from psycopg2.extras import execute_values
import psycopg2

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
        SELECT event_id, line_id, machine_id, state_id, reason_id,
               start_ts, end_ts, duration_sec, shift_id, po, packaging_id, note
        FROM fact_state_event
        WHERE line_id = :line_id
        ORDER BY end_ts DESC
        LIMIT 1
    """)
    with pg_engine.connect() as conn:
        row = conn.execute(sql, {"line_id": line_id}).mappings().first()
        return dict(row) if row else None


UpRow = Dict[str, Any]

UPSERT_SQL = """
INSERT INTO fact_state_event
(hash_key, line_id, machine_id, state_id, reason_id,
 start_ts, end_ts, duration_sec, shift_id, po, packaging_id, note, watchdog, updated_at)
VALUES %s
ON CONFLICT (hash_key) DO UPDATE SET
  line_id       = EXCLUDED.line_id,
  machine_id    = EXCLUDED.machine_id,
  state_id      = EXCLUDED.state_id,
  reason_id     = EXCLUDED.reason_id,
  start_ts      = EXCLUDED.start_ts,
  end_ts        = EXCLUDED.end_ts,
  duration_sec  = EXCLUDED.duration_sec,
  shift_id      = EXCLUDED.shift_id,
  po            = EXCLUDED.po,
  packaging_id  = EXCLUDED.packaging_id,
  note          = EXCLUDED.note,
  watchdog      = EXCLUDED.watchdog,
  updated_at    = now()
"""

def upsert_events(
    pg_engine,
    rows: List[UpRow],
    batch_size: int = 1000,
    dry_run: bool = False,
    logger=None
) -> int:
    if not rows:
        return 0

    cols = [
        "hash_key","line_id","machine_id","state_id","reason_id",
        "start_ts","end_ts","duration_sec","shift_id","po","packaging_id","note"
    ]

    total = 0
    with pg_engine.begin() as conn:
        raw = conn.connection  # psycopg2 connection
        cur = raw.cursor()
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i+batch_size]
            values = [
                tuple(r.get(c) for c in cols)
                for r in chunk
            ]
            if dry_run:
                if logger:
                    first = values[0] if values else None
                    logger.info("DRY-RUN upsert {} rows: First row {}", len(values), first)
                continue
            execute_values(cur, UPSERT_SQL, values, page_size=len(values))
            total += len(values)
        if not dry_run:
            raw.commit()
    return total
