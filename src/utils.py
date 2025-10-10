from datetime import datetime, timedelta, timezone, time, date
import pytz
import os
from typing import List, Tuple, Dict, Optional

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

def resolve_shift(local_dt: datetime,
                  shifts_today: List[Tuple[int, str, str]],
                  shifts_prev:  List[Tuple[int, str, str]]) -> Optional[Tuple[str, int, int]]:
    """
    Return (shift_date_str 'YYYY-MM-DD', shift_no, shift_id_int) if local_dt belongs to any shift window,
    else None. Handles wrap-midnight by checking previous day's windows too.
    shift_id = yyyymmdd*10 + shift_no
    """
    def parse_hms(hms: str):
        hh, mm, ss = map(int, hms.split(":"))
        return hh, mm, ss

    def find_in_windows(ref_dt: datetime, windows):
        for no, st, en in windows:
            hh1, mm1, ss1 = parse_hms(st)
            hh2, mm2, ss2 = parse_hms(en)
            start_dt = ref_dt.replace(hour=hh1, minute=mm1, second=ss1, microsecond=0)
            end_dt   = ref_dt.replace(hour=hh2, minute=mm2, second=ss2, microsecond=0)
            if (hh2, mm2, ss2) <= (hh1, mm1, ss1):
                end_dt = end_dt + timedelta(days=1)
            if start_dt <= ref_dt < end_dt:
                d = start_dt.date().isoformat()
                yyyymmdd = int(start_dt.strftime("%Y%m%d"))
                return d, no, yyyymmdd*10 + int(no)
        return None

    # Try today
    hit = find_in_windows(local_dt, shifts_today)
    if hit:
        return hit
    # Try wrap from yesterday
    hit = find_in_windows(local_dt - timedelta(days=1), shifts_prev)
    return hit