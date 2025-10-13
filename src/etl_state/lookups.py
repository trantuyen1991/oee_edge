# lookups.py
from __future__ import annotations
from typing import Dict, List, Tuple, Any
from datetime import date
from sqlalchemy import text
from sqlalchemy.engine import Engine

def load_states(engine: Engine) -> Dict[str, dict]:
    """
    Load machine states (dim_state) into a map keyed by state_code.

    Args:
        engine (Engine): SQLAlchemy Engine connected to PostgreSQL.

    Returns:
        Dict[str, dict]: Mapping state_code -> payload including state_id and flags.
            Example:
            {
              "RUN":  {"state_id": 1, "is_planned": False, "is_downtime": False, "sort_order": 10},
              "STOP": {"state_id": 2, "is_planned": False, "is_downtime": True,  "sort_order": 20},
              ...
            }

    Example:
        states = load_states(pg_engine)
    """
    sql = text("""
        SELECT
            state_id,           -- integer primary key of dim_state
            state_code,         -- short unique code, e.g. RUN/STOP/IDLE/OFFLINE
            is_planned,         -- whether this is a planned state (true/false)
            is_downtime,        -- whether this contributes to downtime (true/false)
            sort_order          -- ordering for UI/analytics
        FROM public.dim_state     -- source dimension table
    """)  # <-- all lines commented as requested
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()
    return {
        r["state_id"]: {
            "state_id": r["state_id"],
            "is_planned": bool(r["is_planned"]),
            "is_downtime": bool(r["is_downtime"]),
            "sort_order": r["sort_order"],
            "state_code": r["state_code"],
        }
        for r in rows
    }

def load_reasons(engine: Engine) -> Dict[str, dict]:
    """
    Load stop/idle reasons (dim_reason) into a map keyed by reason_code.

    Args:
        engine (Engine): SQLAlchemy Engine connected to PostgreSQL.

    Returns:
        Dict[str, dict]: Mapping reason_code -> payload including reason_id/state_id.
            Example:
            {
              "STOP_CONNECTIVITY_LOSS": {"reason_id": 101, "state_id": 4, "reason_name": "..."},
              ...
            }

    Example:
        reasons = load_reasons(pg_engine)
    """
    sql = text("""
        SELECT
            reason_id,          -- integer primary key of dim_reason
            state_id,           -- FK to dim_state.state_id
            reason_code,        -- short unique code for lookup
            reason_name         -- human-readable name
        FROM public.dim_reason   -- source dimension table for reasons
    """)  # comments per line
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()
    return {
        r["reason_id"]: {
            "reason_id": r["reason_id"],
            "state_id": r["state_id"],
            "reason_name": r["reason_name"],
            "reason_code": r["reason_code"],
        }
        for r in rows
    }

def load_device_map(engine: Engine) -> Dict[str, dict]:
    """
    Load device mapping (dim_device) into a map keyed by device_name.

    Args:
        engine (Engine): SQLAlchemy Engine connected to PostgreSQL.

    Returns:
        Dict[str, dict]: Mapping device_name -> payload for routing events.
            Example:
            {
              "L105_Filler": {"device_id": UUID(...), "line_id": 105, "machine_id": 1},
              ...
            }

    Example:
        device_map = load_device_map(pg_engine)
    """
    sql = text("""
        SELECT
            device_id,          -- UUID primary key for device
            device_name,        -- unique device name used by ThingsBoard/ETL
            line_id,            -- integer FK to dim_line.line_id
            machine_id          -- integer FK to dim_machine.machine_id
        FROM public.dim_device    -- device mapping table
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()
    return {
        r["device_name"]: {
            "device_id": r["device_id"],
            "line_id": r["line_id"],
            "machine_id": r["machine_id"],
        }
        for r in rows
    }

def load_shifts_by_date(engine: Engine, day_from: date, day_to: date) -> List[dict]:
    """
    Load shift calendar within [day_from, day_to] (inclusive) from dim_shift_calendar.

    Args:
        engine (Engine): SQLAlchemy Engine connected to PostgreSQL.
        day_from (date): Start date (inclusive).
        day_to (date): End date (inclusive).

    Returns:
        List[dict]: List of shift definitions per date.
            Example element:
            {
              "shift_date": date(2025,10,11),
              "shift_no": 1,
              "start_time": time(6,0,0),
              "end_time": time(14,0,0)
            }

    Example:
        shifts = load_shifts_by_date(pg_engine, date(2025,10,10), date(2025,10,12))
    """
    sql = text("""
        SELECT
            shift_date,         -- calendar date for the shift
            shift_no,           -- shift number within the date
            start_time,         -- start time (no timezone)
            end_time            -- end time (no timezone)
        FROM public.dim_shift_calendar  -- source shift calendar table
        WHERE shift_date BETWEEN :dfrom AND :dto  -- inclusive date filter
        ORDER BY shift_date, shift_no            -- deterministic order
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"dfrom": day_from, "dto": day_to}).mappings().all()
    return [dict(r) for r in rows]
