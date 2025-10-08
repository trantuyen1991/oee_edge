from datetime import datetime, timedelta, timezone, time, date
import pytz
import os
from typing import List, Tuple, Dict

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

def get_site_tz():
    return pytz.timezone(os.getenv("SITE_TIMEZONE", "Asia/Ho_Chi_Minh"))

def parse_hms(hms: str) -> time:
    # 'HH:MM:SS' -> time
    hh, mm, ss = hms.split(":")
    return time(int(hh), int(mm), int(ss))

def minute_in_any_shift(local_minute_start: datetime,
                        shifts_today: List[Tuple[int, str, str]],
                        shifts_prev: List[Tuple[int, str, str]]) -> bool:
    """
    A minute belongs to a shift if it intersects any (start_time, end_time) window.
    Handles wrap-over-midnight shifts by checking both 'today' and 'yesterday' definitions.
    """
    def in_windows(dt: datetime, windows: List[Tuple[int, str, str]]) -> bool:
        for _, st, en in windows:
            t1 = parse_hms(st); t2 = parse_hms(en)
            start_dt = dt.replace(hour=t1.hour, minute=t1.minute, second=t1.second, microsecond=0)
            end_dt = dt.replace(hour=t2.hour, minute=t2.minute, second=t2.second, microsecond=0)
            if t2 <= t1:
                # wrap midnight: end on next day
                end_dt = end_dt + timedelta(days=1)
            # Check overlap between [dt, dt+60s) and [start_dt, end_dt)
            a1, a2 = dt, dt + timedelta(minutes=1)
            b1, b2 = start_dt, end_dt
            if min(a2, b2) > max(a1, b1):
                return True
        return False

    return in_windows(local_minute_start, shifts_today) or in_windows(local_minute_start - timedelta(days=1), shifts_prev)