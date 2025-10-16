# src/etl/etl_minute.py
import asyncio
# import logging
import os
from typing import Dict, Any

from src.api.publish_tb import publish_minute_facts, publish_latest_state, UPSERT_PG, PUBLISH_TB, DRY_RUN
# from etl.token_map import load_token_map   # nếu bạn muốn map từ PG

# logger = logging.getLogger("oee.etl_minute")

PG_DSN = os.getenv("PG_DSN", "postgresql://user:pass@localhost:5432/oee")

async def process_one_line_minute(
    line_id: int,
    minute_end_utc_ms: int,
    kpi: Dict[str, Any],
    state: str | None,
    reason_id: int | None,
    token: str,
    logger,
) -> None:
    """
    Full minute pipeline for one line:
    1) Upsert facts into PostgreSQL (SoT).
    2) Publish telemetry to ThingsBoard (subset for dashboard).
    """
    # 1) Upsert vào PG (SoT)
    # if not DRY_RUN and UPSERT_PG:
    #     try:
    #         # TODO: your upsert here (psycopg3). Ensure idempotent by (line_id, minute_end_utc)
    #         logger.info("Upsert PG line=%s ts=%s kpi=%s", line_id, minute_end_utc_ms, kpi)
    #     except Exception as ex:
    #         logger.exception("Upsert PG failed line=%s ts=%s", line_id, minute_end_utc_ms)
    #         # tuỳ chiến lược: continue publish or stop. Khuyến nghị vẫn publish nếu PG chỉ là SoT nhưng UI cần realtime.

    # 2) Publish TB (subset)
    try:
        # Minute facts (góp cho chart/kpi)
        await publish_minute_facts(token, minute_end_utc_ms, kpi)

        # Latest state (mỗi phút hoặc on-change)
        # await publish_latest_state(token, minute_end_utc_ms, state, reason_id, extra=None)
    except Exception:
        logger.exception("Publish TB failed line=%s ts=%s", line_id, minute_end_utc_ms)


async def process_batch(lines_data: list[dict], token_map: dict[int, str], logger, stagger_ms: int = 250) -> None:
    """
    Process a batch for many lines of the same minute.
    Args:
        lines_data: list of dicts, each: {
            "line_id": 107,
            "minute_end_utc_ms": 1739560140000,
            "kpi": {"oee":0.71, "good_count":120, ...},
            "state": "RUN",
            "reason_id": 1001 | None
        }
        token_map: {line_id: token}
        stagger_ms: delay between lines to avoid post spikes
    """
    for idx, row in enumerate(lines_data):
        line_id = row["line_id"]
        token = token_map.get(line_id, "")
        if not token:
            logger.warning("No TB token for line_id=%s. Skipping publish.", line_id)
            continue
        
        logger.info(f"Processing line_id={line_id}, minute_end_utc_ms={row['minute_end_utc_ms']} token={token}")
        await process_one_line_minute(
            line_id=line_id,
            minute_end_utc_ms=row["minute_end_utc_ms"],
            kpi=row["kpi"],
            state=row["state"] if "state" in row else None,
            reason_id=row.get("reason_id") if "reason_id" in row else None,
            token=token,
            logger=logger,
        )

        # stagger nhẹ để tránh spike
        if idx < len(lines_data) - 1 and PUBLISH_TB and not DRY_RUN:
            await asyncio.sleep(stagger_ms / 1000.0)
