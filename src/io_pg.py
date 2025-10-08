# English comments per your style
from typing import Optional, Dict, Any, List, Tuple, Set
import os
import psycopg
from psycopg.rows import dict_row
from datetime import date

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

def upsert_fact_min(conn, rows: List[Dict[str, Any]]) -> int:
    """
    Upsert a list of minute rows into fact_production_min.
    Returns number of affected rows.
    """
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(UPSERT_FACT_MIN, rows)
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