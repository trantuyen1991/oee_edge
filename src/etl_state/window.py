# window.py
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine
from timebox import floor_to_minute

def get_last_event_end_ts(engine: Engine, machine_id: int) -> Optional[datetime]:
    """
    Get the latest event end timestamp (UTC) for a given machine.

    Args:
        engine (Engine): SQLAlchemy Engine connected to PostgreSQL.
        machine_id (int): Machine identifier (FK from dim_machine).

    Returns:
        Optional[datetime]: Last recorded end_ts (UTC) or None if no data.

    Example:
        last_end = get_last_event_end_ts(pg_engine, 101)
    """
    sql = text("""
        SELECT MAX(end_ts) AS last_end_utc     -- get the most recent UTC end_ts
        FROM public.fact_state_event           -- from event fact table
        WHERE machine_id = :mid                -- for this machine only
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"mid": machine_id}).mappings().first()
    return row["last_end_utc"] if row and row["last_end_utc"] else None

def compute_from_to(
    last_end_utc: Optional[datetime],
    hard_from_utc: datetime,
    hard_to_utc: datetime,
    overlap_min: int,
    max_backfill_h: int,
) -> Tuple[datetime, datetime]:
    """
    Compute the effective [from_utc, to_utc] window for one device.

    Args:
        last_end_utc (Optional[datetime]): Latest end_ts in fact_state_event, or None if no data.
        hard_from_utc (datetime): Global from_utc based on lookback window.
        hard_to_utc (datetime): Global to_utc (usually now floored to minute).
        overlap_min (int): Overlap minutes to re-read recent data to ensure continuity.
        max_backfill_h (int): Maximum hours allowed for backfill (safety cap).

    Returns:
        Tuple[datetime, datetime]: Final (from_utc, to_utc) window for this device.

    Example:
        from_utc, to_utc = compute_from_to(last_end, hard_from, hard_to, 3, 24)
    """
    # 1️⃣  Nếu chưa có event nào → bắt đầu theo lookback global
    if not last_end_utc:
        return hard_from_utc, hard_to_utc

    # 2️⃣  Cắt 3 phút overlap để đọc chồng nhẹ
    from_utc = floor_to_minute(last_end_utc - timedelta(minutes=overlap_min))

    # 3️⃣  Không được sớm hơn global hard_from
    if from_utc < hard_from_utc:
        from_utc = hard_from_utc

    # 4️⃣  Không được lùi quá max_backfill_h
    limit_from = hard_to_utc - timedelta(hours=max_backfill_h)
    if from_utc < limit_from:
        from_utc = limit_from

    return from_utc, hard_to_utc
