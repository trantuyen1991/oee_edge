# timebox.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Tuple

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore


@dataclass(frozen=True)
class SiteClock:
    """Snapshot of global time context for one ETL run."""
    now_utc: datetime       # Current UTC time (aware)
    now_site: datetime      # Current site-local time (aware)
    site_tz: str            # IANA timezone name (e.g., "Asia/Ho_Chi_Minh")


def get_site_clock(site_tz: str) -> SiteClock:
    """
    Build the site clock using the provided IANA timezone.

    Args:
        site_tz (str): IANA timezone name (e.g., "Asia/Ho_Chi_Minh").

    Returns:
        SiteClock: Snapshot containing now_utc, now_site, and site_tz.

    Example:
        clk = get_site_clock("Asia/Ho_Chi_Minh")
    """
    tz = ZoneInfo(site_tz)
    now_utc = datetime.now(timezone.utc)
    now_site = now_utc.astimezone(tz)
    return SiteClock(now_utc=now_utc, now_site=now_site, site_tz=site_tz)


def floor_to_minute(dt: datetime) -> datetime:
    """
    Floor a timezone-aware datetime to minute precision (zero seconds & microseconds).

    Args:
        dt (datetime): Aware datetime.

    Returns:
        datetime: Aware datetime floored to the nearest previous minute.

    Example:
        floor_to_minute(clk.now_utc)
    """
    return dt.replace(second=0, microsecond=0)


def ceil_to_minute(dt: datetime) -> datetime:
    """
    Ceil a timezone-aware datetime to minute precision.

    Args:
        dt (datetime): Aware datetime.

    Returns:
        datetime: Aware datetime ceiled to the next minute if needed.

    Example:
        ceil_to_minute(clk.now_utc)
    """
    floored = floor_to_minute(dt)
    return floored if dt == floored else floored + timedelta(minutes=1)


def site_window_from_lookback(now_site: datetime, lookback_h: int) -> Tuple[datetime, datetime]:
    """
    Compute a local-site time window [from_local, to_local] using a lookback (hours).

    Args:
        now_site (datetime): Current site-local time (aware).
        lookback_h (int): Number of hours to look back from now_site.

    Returns:
        Tuple[datetime, datetime]: (from_local, to_local) in site timezone, minute-aligned.

    Example:
        from_local, to_local = site_window_from_lookback(clk.now_site, 24)
    """
    to_local = floor_to_minute(now_site)
    from_local = to_local - timedelta(hours=lookback_h)
    return from_local, to_local


def to_epoch_ms(dt_utc: datetime) -> int:
    """
    Convert an aware UTC datetime to epoch milliseconds.

    Args:
        dt_utc (datetime): UTC datetime (aware).

    Returns:
        int: Epoch milliseconds.

    Example:
        ms = to_epoch_ms(clk.now_utc)
    """
    if dt_utc.tzinfo != timezone.utc:
        raise ValueError("to_epoch_ms expects a UTC-aware datetime")
    return int(dt_utc.timestamp() * 1000)


def site_to_utc(dt_site: datetime, site_tz: str) -> datetime:
    """
    Convert a site-local aware datetime to UTC aware datetime.

    Args:
        dt_site (datetime): Site-local aware datetime.
        site_tz (str): IANA site timezone.

    Returns:
        datetime: UTC-aware datetime.

    Example:
        utc_from = site_to_utc(from_local, "Asia/Ho_Chi_Minh")
    """
    tz = ZoneInfo(site_tz)
    if dt_site.tzinfo is None:
        dt_site = dt_site.replace(tzinfo=tz)  # assume naive is site time
    return dt_site.astimezone(timezone.utc)
