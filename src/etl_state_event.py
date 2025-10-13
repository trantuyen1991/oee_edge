"""
ETL job: Build fact_state_event from ThingsBoard Edge timeseries.
- Reads machineState (reason_code) from Cassandra (ts_kv_cf)
- Detects transitions (state changes) -> segments [start_ts, end_ts)
- Maps reason_code -> (reason_id, state_id) via PostgreSQL (dim_reason/dim_state)
- Computes shift membership (for future shift_id if needed)
- Upserts into fact_state_event with UNIQUE(line_id, start_ts)
- Idempotent; safe to re-run over overlapping windows
"""
from pathlib import Path                                     # path utilities   
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple, Optional

from dotenv import load_dotenv
from loguru import logger

from io_cas import (
    get_cas_session, 
    fetch_timeseries_numeric, 
    fetch_timeseries_text,
    fetch_state_with_quality,
    get_partitions
    )
from io_pg import (
    get_pg_conn,
    load_reason_lookup,          # NEW (you'll add this function below)
    upsert_state_event_batch,    # NEW (you'll add this function below)
    load_shifts_by_date,
    load_device_map,
    get_last_event_end_ts,
    load_states
)
from utils import (
    floor_to_minute,
    get_site_tz,
    minute_in_any_shift,
    to_epoch_ms,
    resolve_shift
)

# ==== CONFIG ====
ROOT = Path(__file__).resolve().parents[1]                   # project root: /home/admin/oee-edge
ENV_PATH = ROOT / ".env"                                     # expected .env path
load_dotenv(dotenv_path=ENV_PATH)                            # explicit load from root
print(f"[BOOT] .env loaded from: {ENV_PATH}, exists={ENV_PATH.exists()}")  # quick sanity log 

ENTITY_KEY_STATE = os.getenv("KEY_STATE", "machineState")
ENTITY_KEY_PO    = os.getenv("KEY_PO",    "processOrderNr")
ENTITY_KEY_PACK  = os.getenv("KEY_PACK",  "packaging_id")

ENTITY_KEY_WD   = os.getenv("KEY_WATCHDOG", "watchDog")
COMM_LOSS_CODE  = int(os.getenv("COMM_LOSS_CODE", "9000"))
OFFLINE_GRACE_SEC = int(os.getenv("OFFLINE_GRACE_SEC", "30"))

BACKFILL_ON_START      = os.getenv("BACKFILL_ON_START", "false").lower() in ("1","true","yes","on")
BACKFILL_LOOKBACK_HOURS= int(os.getenv("BACKFILL_LOOKBACK_HOURS","24"))
EVENT_BLOCK_MIN        = int(os.getenv("EVENT_BLOCK_MIN","30"))

# --- Fallback for missing reason mapping ---
UNKNOWN_REASON_ID = int(os.getenv("UNKNOWN_REASON_ID", "999000"))
UNKNOWN_STATE_ID  = int(os.getenv("UNKNOWN_STATE_ID",  "0"))
UNKNOWN_REASON_CODE = os.getenv("UNKNOWN_REASON_CODE", "-1")

def main():
    # STEP-00 — Init logging & context
    load_dotenv()
    os.makedirs("logs", exist_ok=True)
    logger.add(
        "logs/etl_state_event_{time:YYYY-MM-DD}.log",  # hoặc etl_minute_{time:YYYY-MM-DD}.log
        rotation="00:00",       # tách file mỗi ngày lúc 00:00 (theo local time)
        retention="7 days",     # chỉ giữ 7 ngày gần nhất (tự xóa file cũ)
        compression="gz",       # nén file cũ .gz để tiết kiệm dung lượng
        level="INFO",
        enqueue=True            # an toàn khi chạy qua systemd / multi-thread
    )
    # ----STEP-01------------ SELECT LOG-MODE ----------------
    DRY_RUN = env_bool("DRY_RUN", False)  
    if DRY_RUN:
        logger.info("DRY_RUN=True -> only logging (no DB writes)")
    else:
        logger.info("DRY_RUN=False -> UPSERT fact_state_event enabled")
     # ----STEP-02------------ GET PARAMETER ----------------
    now_utc = datetime.now(timezone.utc)
    site_tz = get_site_tz()
    cas = get_cas_session()
    pg  = get_pg_conn()

    device_map = load_device_map(pg)  # {uuid: (line_id, machine_id)}
    reason_lookup = load_reason_lookup(pg)
    state_lookup = {s['state_id']: s['state_code'] for s in load_states(pg)}  # chạy 1 lần ngoài vòng loop chính

    from_local = (now_utc - timedelta(hours=BACKFILL_LOOKBACK_HOURS)).astimezone(site_tz).date()
    to_local   = now_utc.astimezone(site_tz).date()
    shifts_by_day = load_shifts_by_date(pg, from_local - timedelta(days=1), to_local)

    total_segments = 0
    total_upserts  = 0

    try:
        # ---------------- BACKFILL ON START (gọn – chuẩn UTC) ----------------
        if BACKFILL_ON_START:
            # 1) Thời gian chuẩn UTC
            watermark = int(os.getenv("WATERMARK_SEC", "120"))               # lag để tránh dữ liệu đang đến
            hard_to  = datetime.utcnow().replace(tzinfo=timezone.utc)        # luôn UTC aware
            # hard_to  = floor_to_minute(now_utc - timedelta(seconds=watermark))

            logger.info(f"Backfill enabled: lookback={BACKFILL_LOOKBACK_HOURS}h, watermark={watermark}s "
                        f"(UTC hard_to={hard_to}, Local hard_to={hard_to.astimezone(site_tz)})")

            # 2) Tiện ích chuyển về naive-UTC khi nói chuyện với Postgres (timestamp without time zone)
            def to_naive_utc(dt: datetime) -> datetime:
                return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt

            total_segments = 0
            total_upserts  = 0

            # 3) Quét từng device/line
            for dev_id, (line_id, machine_id) in device_map.items():
                # 3.1) Lấy mốc cuối cùng đã ghi trong DB (naive → hiểu là UTC)
                last_end = get_last_event_end_ts(pg, line_id)  # có thể None
                if last_end is not None:
                    # chuẩn hóa thành UTC aware và kẹp không vượt hard_to
                    last_end_utc = (last_end.replace(tzinfo=timezone.utc)
                                    if last_end.tzinfo is None else last_end.astimezone(timezone.utc))
                    last_end_utc = min(last_end_utc, hard_to)
                else:
                    last_end_utc = None

                # 3.2) Xác định from/to cho backfill
                #    - Nếu chưa từng có dữ liệu → backfill full lookback 48h
                #    - Nếu đã có → backfill từ (last_end - 5 phút) đến hard_to để bù trễ/ngắt quãng
                # if last_end_utc is None:
                start_from = hard_to - timedelta(hours=BACKFILL_LOOKBACK_HOURS)
                # else:
                #     start_from = floor_to_minute(last_end_utc - timedelta(minutes=5))  # overlap nhẹ

                # Guard: nếu vì lý do nào đó from >= to (ngược), kéo lùi 1h cho an toàn
                if start_from >= hard_to:
                    logger.warning(f"[LINE {line_id}] start_from >= hard_to ({start_from} >= {hard_to}) → adjust by -1h")
                    start_from = floor_to_minute(hard_to - timedelta(hours=1))

                # Log song song UTC & Local để dễ so sánh với UI
                logger.info(
                    f"[LINE {line_id}] Backfill window: "
                    f"UTC {start_from} → {hard_to} | "
                    f"Local {start_from.astimezone(site_tz)} → {hard_to.astimezone(site_tz)}"
                )

                # 3.3) Dọn chồng chéo trong khoảng [start_from, hard_to) trước khi insert lại
                if not DRY_RUN:
                    delete_pg(pg, line_id, start_from, hard_to, site_tz)
                # 3.4) Xử lý 1 phát toàn cửa sổ (không chia block)
                up, segs = process_one_device_window(
                    dev_id, line_id, machine_id,
                    from_min_utc=start_from,
                    to_min_utc=hard_to,
                    cas=cas, pg=pg, site_tz=site_tz,
                    reason_lookup=reason_lookup, shifts_by_day=shifts_by_day,
                    DRY_RUN=DRY_RUN, state_lookup=state_lookup
                )
                total_upserts  += up
                total_segments += segs

            logger.info(f"Backfill done. Segments={total_segments}, Upserts={total_upserts}, DRY_RUN={DRY_RUN}")
            return
        # ---------------- END BACKFILL ON START ----------------
        else:
            # ----- STREAMING WINDOW (per-line), không dùng minute_window_for_events -----
            watermark_sec = int(os.getenv("WATERMARK_SEC", "120"))
            overlap_min   = int(os.getenv("EVENT_BACK_MIN", "5"))  # 5–6 phút là hợp lý

            now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
            hard_to = floor_to_minute(now_utc - timedelta(seconds=watermark_sec))  # mốc chốt dữ liệu an toàn

            total_segments = 0
            for dev_id, (line_id, machine_id) in device_map.items():
                # 1) lấy event cuối trước hard_to để quyết định from_min_utc
                # last_end = get_last_event_end_ts(pg, line_id)  # naive (UTC)
                last_end = get_last_event_end_ts_v1(pg, line_id, site_tz, hard_to)  # mới
                logger.debug(f"get_last_event_end_ts: last_end {last_end}")
                if last_end:
                    last_end_utc = (last_end.replace(tzinfo=timezone.utc)
                                    if last_end.tzinfo is None else last_end.astimezone(timezone.utc))
                    last_end_utc = min(last_end_utc, hard_to)
                    from_min_utc = floor_to_minute(last_end_utc - timedelta(minutes=overlap_min))
                else:
                    # lần đầu chưa có dữ liệu: lấy lookback rộng (ví dụ 48h) để dựng liên tục
                    lookback_h = int(os.getenv("BACKFILL_LOOKBACK_HOURS", "48"))
                    from_min_utc = hard_to - timedelta(hours=lookback_h)

                # Guard nếu lỡ ngược
                if from_min_utc >= hard_to:
                    from_min_utc = floor_to_minute(hard_to - timedelta(minutes=overlap_min))

                # 2) XÓA overlap trước khi ghi (UTC-naive + dọn legacy local-naive)
                if not DRY_RUN:
                    delete_pg(pg, line_id, from_min_utc, hard_to, site_tz)

                # 3) Xử lý 1 phát theo cửa sổ đã tính (đã có seed continuity trong process_one_device_window)
                up, segs = process_one_device_window(
                    dev_id, line_id, machine_id,
                    from_min_utc=from_min_utc,
                    to_min_utc=hard_to,
                    cas=cas, pg=pg, site_tz=site_tz,
                    reason_lookup=reason_lookup, shifts_by_day=shifts_by_day,
                    DRY_RUN=DRY_RUN, state_lookup=state_lookup
                )
                total_upserts += up
                total_segments += segs

    except Exception as e:
        logger.exception(f"State event ETL failed: {e}")
        raise
    finally:
        try:
            pg.close()
        except Exception:
            pass
        try:
            cas.shutdown()
        except Exception:
            pass

    logger.success(f"State event ETL done. Segments={total_segments}, Upserts={total_upserts}, DRY_RUN={DRY_RUN}")

if __name__ == "__main__":
    main()
