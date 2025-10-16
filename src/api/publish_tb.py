import os
import time
import logging
from typing import Dict, Any, Optional

import httpx  # pip install httpx

logger = logging.getLogger("oee.publish_tb")

TBTB_BASE_URL = os.getenv("TBTB_BASE_URL", "http://127.0.0.1:18080")
TB_TIMEOUT_S = float(os.getenv("TB_TIMEOUT_S", "5"))
TB_MAX_RETRIES = int(os.getenv("TB_MAX_RETRIES", "3"))
TB_DEFAULT_TOKEN = os.getenv("TB_DEFAULT_TOKEN", "")

UPSERT_PG = os.getenv("UPSERT_PG", "true").lower() == "true"
PUBLISH_TB = os.getenv("PUBLISH_TB", "true").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"


async def _post_with_retry(url: str, payload: Dict[str, Any]) -> int:
    """
    POST JSON to ThingsBoard with small retry and exponential backoff.
    Args:
        url: Full TB endpoint.
        payload: JSON body to send.
    Returns:
        HTTP status code (int).
    Raises:
        RuntimeError if all attempts fail.
    Example:
        await _post_with_retry("http://host:8080/api/v1/<token>/telemetry", {"ts": 123, "values": {...}})
    """
    delay = 0.5
    last_err: Optional[str] = None
    for attempt in range(1, TB_MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=TB_TIMEOUT_S) as client:
                resp = await client.post(url, json=payload)
            if resp.status_code < 400:
                logger.info("TB POST ok %s (code=%s, sizeB=%s)", url, resp.status_code, len(resp.content or b""))
                return resp.status_code
            last_err = f"status={resp.status_code} body={resp.text[:300]}"
            logger.warning("TB POST failed attempt %d/%d: %s", attempt, TB_MAX_RETRIES, last_err)
        except Exception as ex:
            last_err = str(ex)
            logger.exception("TB POST error attempt %d/%d", attempt, TB_MAX_RETRIES)
        if attempt < TB_MAX_RETRIES:
            time.sleep(delay)
            delay *= 2
    raise RuntimeError(f"Failed TB POST after {TB_MAX_RETRIES} attempts: {last_err}")


def _endpoint(token: str) -> str:
    """Build telemetry endpoint for a device token."""
    return f"{TBTB_BASE_URL.rstrip('/')}/api/v1/{token}/telemetry"


async def publish_minute_facts(
    token: str,
    minute_end_utc_ms: int,
    values: Dict[str, Any],
) -> None:
    """
    Publish minute-level KPI facts to ThingsBoard.

    Args:
        token: Device access token on TB.
        minute_end_utc_ms: Epoch ms (end of minute) – used as idempotent ts.
        values: Dict of KPI keys (oee, availability, performance, quality, good_count, reject_count, runtime_sec, downtime_sec, ...)

    Example:
        await publish_minute_facts(token, 1739560140000, {"oee":0.71,"good_count":120})
    """
    if DRY_RUN or not PUBLISH_TB:
        logger.info("[DRY or PUBLISH_TB=false] Skip publish_minute_facts ts=%s values=%s", minute_end_utc_ms, values)
        return

    url = _endpoint(token)
    payload = {"ts": minute_end_utc_ms, "values": values}
    await _post_with_retry(url, payload)


async def publish_latest_state(
    token: str,
    ts_utc_ms: int,
    state: str | None,
    reason_id: Optional[int],
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Publish latest machine state (RUN/STOP/OFFLINE) to ThingsBoard.

    Args:
        token: Device access token on TB.
        ts_utc_ms: Epoch ms at which state is observed.
        state: "RUN" | "STOP" | "OFFLINE".
        reason_id: Optional reason code.
        extra: Optional extra fields, e.g., {"speed": 115, "is_offline": False}
    """
    if DRY_RUN or not PUBLISH_TB:
        logger.info("[DRY or PUBLISH_TB=false] Skip publish_latest_state ts=%s state=%s reason=%s extra=%s",
                    ts_utc_ms, state, reason_id, extra)
        return

    url = _endpoint(token)
    values = {"state": state}
    if reason_id is not None:
        values["reason_id"] = str(reason_id)
    if extra:
        values.update(extra)

    payload = {"ts": ts_utc_ms, "values": values}
    await _post_with_retry(url, payload)
