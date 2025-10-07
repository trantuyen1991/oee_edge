# English comments per your style
from typing import Optional, Dict, Any, List, Tuple
import os
import psycopg
from psycopg.rows import dict_row

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
