from datetime import datetime, timedelta, timezone
import pytz
import os
from typing import Tuple

def floor_to_minute(dt_utc: datetime) -> datetime:
    """Return dt floored to :00 seconds in UTC."""
    return dt_utc.replace(second=0, microsecond=0, tzinfo=timezone.utc)

def minute_range_to_finalize(now_utc: datetime) -> Tuple[datetime, datetime]:
    """
    Decide which minute buckets to finalize using watermark/backfill.
    Example:
      - WATERMARK_SEC=120: finalize minute N when now >= N+120s
      - BACKFILL_MIN=3: recompute N-2..N (3 minutes) each run
    Returns (from_inclusive, to_exclusive) in UTC minute boundaries.
    """
    wm = int(os.getenv("WATERMARK_SEC", "120"))
    bf = int(os.getenv("BACKFILL_MIN", "3"))

    # Determine the last fully-watermarked minute
    last_ok = floor_to_minute(now_utc - timedelta(seconds=wm))
    # Recompute previous bf minutes
    from_min = last_ok - timedelta(minutes=bf - 1)
    to_min = last_ok + timedelta(minutes=1)  # exclusive
    return from_min, to_min

def to_epoch_ms(dt_utc: datetime) -> int:
    """UTC datetime -> epoch milliseconds."""
    return int(dt_utc.timestamp() * 1000)
