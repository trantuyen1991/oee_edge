# helpers/partition_utils.py
from datetime import datetime, timedelta, timezone

def floor_to_month_utc(ts_ms: int) -> int:
    """
    Floor a timestamp (ms) to the start of its month (00:00 UTC of day 1).

    Args:
        ts_ms (int): Epoch milliseconds.

    Returns:
        int: Epoch ms of month-start.
    Example:
        >>> floor_to_month_utc(1760234567890)
        1760208000000
    """
    dt = datetime.utcfromtimestamp(ts_ms / 1000.0).replace(tzinfo=timezone.utc)
    floored = datetime(dt.year, dt.month, 1, 0, 0, 0, tzinfo=timezone.utc)
    return int(floored.timestamp() * 1000)

def ceil_to_next_month_utc(ts_ms: int) -> int:
    """
    Ceil a timestamp (ms) to the start of the next month (UTC).

    Args:
        ts_ms (int): Epoch milliseconds.

    Returns:
        int: Epoch ms of the next month start.
    """
    dt = datetime.utcfromtimestamp(ts_ms / 1000.0).replace(tzinfo=timezone.utc)
    if dt.month == 12:
        next_month = datetime(dt.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        next_month = datetime(dt.year, dt.month + 1, 1, tzinfo=timezone.utc)
    return int(next_month.timestamp() * 1000)
