import os
from loguru import logger
from config import Config
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
def env_bool(name: str, default: bool = False) -> bool:
    """
    Read boolean from environment variables with common truthy values.
    Accepted truthy: 1, true, yes, y, on (case-insensitive).
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")

def env_str(name: str, default: str = "") -> str:
    """Read string from environment variables with default."""
    return os.getenv(name, default)

def adaptive_lookback(cfg:Config ,pg_conn, device_id, last_end_ts) -> int:
    """
    Dynamically adjust lookback_h based on the time gap
    between now() and latest event in fact_state_event.
    """
    # Default values from config
    default_lookback_h = cfg.etl_state.lookback_h
    max_lookback_h = cfg.etl_state.max_lookback_h
    min_lookback_h = cfg.etl_state.min_lookback_h

    # try:
    #     sql = text("""
    #         SELECT (end_ts AT TIME ZONE :timezone) AS end_ts
    #         FROM fact_state_event
    #         WHERE device_uuid = :device_uuid
    #         ORDER BY end_ts DESC
    #         LIMIT 1
    #     """)

    #     with pg_conn.connect() as conn:
    #         result = conn.execute(sql, {"device_uuid": device_id,"timezone": cfg.app.timezone})  # truyền dict mapping 
    #         last_end_ts = result.scalar()  # lấy giá trị đơn lẻ
    #         logger.debug("last_end_ts ={}", last_end_ts)
    # except Exception as e:
    #     logger.warning(f"Failed to get last_end_ts: {e}")
    #     return default_lookback_h

    if not last_end_ts:
        logger.info(f"No previous event for {device_id}, using default {default_lookback_h}h lookback.")
        return max_lookback_h

    now_utc = datetime.now(timezone.utc)
    logger.debug("last_end_ts ={}", last_end_ts)
    logger.debug("now_utc ={}", now_utc)
    gap_h = (now_utc - last_end_ts).total_seconds() / 3600

    if gap_h < 1:
        lookback_h = min_lookback_h
    elif gap_h > max_lookback_h:
        lookback_h = max_lookback_h
    else:
        lookback_h = int(gap_h) + 1  # round up to full hour

    logger.info(f"Adaptive lookback_h={lookback_h}h (gap={gap_h:.1f}h, last_end={last_end_ts})")
    return lookback_h
        
