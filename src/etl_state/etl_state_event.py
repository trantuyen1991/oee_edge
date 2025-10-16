from dotenv import load_dotenv
from logger_utils import init_logger
import argparse
import os
from typing import Dict, List, Any, Optional, Iterable, Tuple, Sequence, Iterator, Callable
from etl_utils import env_bool, env_str, adaptive_lookback
from config import load_config, Config
from db_pg import get_pg_engine, close_pg_engine, pg_smoke_test, get_last_event, upsert_events
from db_cas import get_cas_session, close_cas_session, cas_smoke_test
from lookups import load_states, load_reasons, load_device_map, load_shifts_by_date
from datetime import datetime, timezone, timedelta, time
from timebox import get_site_clock, site_window_from_lookback, site_to_utc, to_epoch_ms, utc_to_local_date
from window import get_last_event_end_ts, compute_from_to
from reader_cassandra import read_timeseries_for_device, discover_keys_for_device, sample_points,partitions_for_keys
import uuid
from derive_state_points import (
    normalize_timeseries, 
    derive_state_points,
    coalesce_segments,
    compress_segments,
    merge_with_history,
    attach_hash, rows_for_upsert,
    make_shift_lookup
    )
from zoneinfo import ZoneInfo
# from normalize import normalize_timeseries
def parse_args():
    """
    Parse command-line arguments for the ETL State Event job.

    Returns:
        argparse.Namespace:
            - dry_run (bool): When True, run in dry mode (no DB writes).
              Overrides the environment variable DRY_RUN.
              Useful for testing or debugging ETL flow without affecting data.
    Example:
        $ python etl_state_event.py --dry-run
    """
    parser = argparse.ArgumentParser(description="ETL State Event")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without DB writes (logging only). Overrides env DRY_RUN."
    )
    return parser.parse_args()
def _ensure_uuid(v):
    """Return uuid.UUID from str/UUID; raise on invalid."""
    return v if isinstance(v, uuid.UUID) else uuid.UUID(str(v))

def main():
    # ---------- STEP-00: LOGGER CONFIGURATION ----------
    load_dotenv()
        # Read LOG_LEVEL from ENV (default INFO)
    log_level = env_str("LOG_LEVEL", "INFO")
        # Init logger first so we can log decisions
    logger = init_logger(
        log_dir="logs",
        log_name_prefix="etl_state_event",
        retention_days=7,
        rotation="00:00",
        level=log_level
    )
    logger.info("STEP-00: === ETL State Event Job Started ===")
    # ---------- STEP-01: DETECT MODE ----------
    args = parse_args()
        # DRY_RUN: CLI overrides ENV
    dry_run_env = env_bool("DRY_RUN", False)
    DRY_RUN = True if args.dry_run else dry_run_env

    if DRY_RUN:
        logger.warning("STEP-00: DRY_RUN=True -> logging only (NO DB writes).")
    else:
        logger.info("STEP-00: DRY_RUN=False -> UPSERT to fact_state_event ENABLED.")
    # ---------- STEP-02: Load config (config-first) ----------
    cfg_path = os.getenv("ETL_CFG", "configs/etl_state.yaml")
    try:
        cfg = load_config(cfg_path)
        logger.debug("STEP-02: Config loaded from {}", cfg_path)
        logger.debug("STEP-02: Config detail: {}", cfg)
    except Exception as e:
        logger.error("STEP-02: Failed to load config from {}: {}", cfg_path, e)
        raise
    # ---------- STEP-03: OPEN CONNECTIONS ----------
    pg_engine = None
    cas_session = None
    try:
        # PostgreSQL
        pg_engine = get_pg_engine(cfg.pg_dsn)
        pg_smoke_test(pg_engine)
        logger.debug("STEP-03: PostgreSQL connected (engine ready).")

            # Cassandra
        contact_points = [x.strip() for x in cfg.cas_contact_points.split(",") if x.strip()]
        cas_session = get_cas_session(contact_points, cfg.cas_keyspace, cfg.cas_port)
        cas_smoke_test(cas_session)
        logger.debug("STEP-03: Cassandra connected (session ready).")
        # ---------- STEP-04: Load static lookups (one-shot) ----------
        states = load_states(pg_engine)
        reasons = load_reasons(pg_engine)
        devices = load_device_map(pg_engine)
        
        logger.debug("STEP-04: Lookups loaded successfully.")
        logger.debug("STEP-04: dim_state  -> total={} | first={}", len(states), next(iter(states.items()), None))
        logger.debug("STEP-04: dim_reason -> total={} | first={}", len(reasons), next(iter(reasons.items()), None))
        logger.debug("STEP-04: dim_device -> total={} | first={}", len(devices), next(iter(devices.items()), None))
        
        reason_to_state: Dict[int, Dict[str, Any]] = {}
        for rid, r in reasons.items():
            sid = r.get("state_id")
            reason_id = r.get("reason_id")
            state_code = states.get(sid, {}).get("state_code") if sid is not None else None
            reason_to_state[int(rid)] = {"reason_id": reason_id,"state_id": sid, "state_code": state_code}
            # logger.debug("STEP-04: reason_to_state -> {}", reason_to_state[rid])
        # ---------- STEP-05: Global timebox ----------
        # ---------- STEP-06: Per-device window ----------
        # ---------- STEP-07: Fetch raw data for each device ----------
        keys = [cfg.domain.tags.machine_state, cfg.domain.tags.po]
        clk = get_site_clock(cfg.app.timezone)
        # if cfg.domain.tags.watchdog:
        #     keys.append(cfg.domain.tags.watchdog)
        # if cfg.domain.tags.status_str:
        #     keys.append(cfg.domain.tags.status_str)
        # if cfg.domain.tags.po:
        #     keys.append(cfg.domain.tags.po)
        # if cfg.domain.tags.reset:
        #     keys.append(cfg.domain.tags.reset)
        logger.debug("STEP-07:  keys list {}", keys)
        
        for dev_name, meta in devices.items():
            mid = meta["device_id"]
            device_uuid = _ensure_uuid(meta["device_id"])
            
            last_end = get_last_event_end_ts(pg_engine, mid)
            if last_end is None:
                last_end_utc = None
            else:
                last_end_utc = site_to_utc(last_end, clk.site_tz) 
                
            lookback_h = adaptive_lookback(cfg, pg_engine, mid, last_end_utc)
            logger.debug("STEP-07:  Device {} adaptive_lookback: lookback_h ={}",dev_name, lookback_h)
            
            logger.debug("STEP-07: Site clock:  Device {} now_site={}, now_utc={}",dev_name, clk.now_site.isoformat(), clk.now_utc.isoformat())

            from_local, to_local = site_window_from_lookback(clk.now_site, lookback_h)
            logger.debug("STEP-07: Global window (site):  Device {} from_local={} -> to_local={}",dev_name, from_local, to_local)

                # Quy đổi sang UTC (để dùng cho bước watermark & đọc dữ liệu)
            from_utc = site_to_utc(from_local, clk.site_tz)
            to_utc   = site_to_utc(to_local,   clk.site_tz)
            
            logger.debug("STEP-07: Global window (UTC):  Device {} from_utc={} -> to_utc={}",dev_name, from_utc, to_utc)

                # Nếu muốn xem epoch ms (tham khảo)
            logger.debug("STEP-07: Global window epoch_ms:  Device {} from={} -> to={}",dev_name, to_epoch_ms(from_utc), to_epoch_ms(to_utc))
            
            
            from_utc_dev, to_utc_dev = compute_from_to(
                last_end_utc, from_utc, to_utc, cfg.app.overlap_min, lookback_h
            )

            logger.info("STEP-07: Compute_from_to -> Device {} → last_end={} | window {} → {}",
                        dev_name, last_end_utc, from_utc_dev, to_utc_dev)
                       
                # ví dụ tải lịch ca cho hôm nay và hôm qua
            day_to = utc_to_local_date(to_utc_dev)
            day_from = utc_to_local_date(from_utc_dev)
            shifts = load_shifts_by_date(pg_engine, day_from, day_to)
            logger.debug("STEP-07:  Device {} Shift rows loaded: {}",dev_name, len(shifts))
            if shifts:
                logger.debug("STEP-07:  Device {} First shift row: {}",dev_name, shifts[0])
                logger.debug("STEP-07:  Device {} End shift row: {}",dev_name, shifts[-1])
            
                # ... trong loop từng device, sau khi tính from_utc_dev, to_utc_dev:
            key_counts = discover_keys_for_device(cas_session, device_uuid, from_utc_dev, to_utc_dev, sample_limit_per_part=2000)
            logger.info("STEP-07: discover_keys_for_device -> Device {} -> keys present (approx): {}", dev_name, dict(key_counts.most_common(10)))
            
            raw = read_timeseries_for_device(
                cas_session,
                device_uuid,
                keys,
                from_utc_dev,
                to_utc_dev,
                page_size=5000
            )

                # Preview: in ra kích thước từng key và 1 điểm đầu
            for k, rows in raw.items():
                first = rows[-1] if rows else None
                logger.info("STEP-07: read_timeseries_for_device -> Device {} key '{}' → {:,} points | first={}", dev_name, k, len(rows), first)

            # ---------- STEP-08: derive_state_points(raw, ...) ----------
                 # ... STEP-08A
            timeline = normalize_timeseries(raw, keys)
            first = timeline[-1] if timeline else None
            logger.info("STEP-08A: Normalize timeseries Device {} Normalized timeline -> {} points | first={}", dev_name, len(timeline), first)
            for i, row in enumerate(timeline[:3]):
                logger.debug("STEP-08A: Device {}  Normalized timeline sample[{}] -> {}", dev_name, i, row)
                # ...  STEP-08B
            segments = derive_state_points(timeline, reason_to_state)
            logger.info("STEP-08B: Derived segments -> {}", len(segments))
            for i, seg in enumerate(segments[:3]):
                logger.debug("STEP-08B: Device {}  Derived segment sample[{}] -> {}",dev_name, i, seg)
            # if segments:
            #     first = segments[-1] if segments[-1] else None
            #     logger.debug("STEP-08B: First seg:  Device {} first={} ",dev_name, first)
                # ...  STEP-08C
            segments = coalesce_segments(segments, shift_boundaries=None)
            first_segments = segments[-1] if segments else None
            logger.info("STEP-08C: Coalesce segments ->  Device {} first_segments={}",dev_name, first_segments)
            # ---------- STEP-09: Compress + Duration ----------
            segments = compress_segments(segments)
            logger.info("STEP-09: Compressed segments -> Device {} len(segments)={}",dev_name, len(segments))
            if segments:
                first = segments[-1] if segments else None
                logger.debug( "STEP-09: First compressed segment: Device {} first={}",dev_name, first)
            # -------- STEP-10: Merge with history (overlap & extend) --------
            line_id = meta["line_id"]  # bạn đã có line_id trong devices map
            last_ev = get_last_event(pg_engine, line_id)
            logger.info("STEP-10: Get_last_event -> Device {} last_ev={} ",dev_name, last_ev )
                
            merged = merge_with_history(last_ev, segments, tolerance_sec=1)
            logger.info("STEP-10: Merged with history -> Device {} len(merged)={} len(segments) (was {})",dev_name, len(merged), len(segments))
            if merged:
                m0 = merged[-1]
                logger.debug(
                    "STEP-10: Device {}  After merge: first seg ={} ",dev_name, m0)
            segments = merged
            # -------- STEP-11A: attach hash_key cho segments đã merge --------
            segments = attach_hash(segments, device_uuid=str(device_uuid))

                # Chuẩn bị lookup/metadata
            device_meta = {
                "line_id": devices[dev_name]["line_id"],  # hoặc meta.get(...)
                "device_id": devices[dev_name]["device_id"],
                "machine_id": devices[dev_name].get("machine_id"),  # nếu có
            }
     
            def _ensure_tz(tz_like) -> ZoneInfo:
                # tz_like có thể là string "Asia/Ho_Chi_Minh" hoặc đã là tzinfo
                if isinstance(tz_like, str):
                    return ZoneInfo(tz_like)
                return tz_like  # đã là tzinfo

            site_tz = _ensure_tz(cfg.app.timezone)         # ví dụ "Asia/Ho_Chi_Minh"
            shift_lookup_fn = make_shift_lookup(shifts, site_tz)
            
                # build rows cho upsert
            rows = rows_for_upsert(
                segments=segments,
                device_meta=device_meta,
                states_lookup=reason_to_state,      # key: reason_id (int)
                shift_lookup_fn=shift_lookup_fn,     # hàm tra cứu
            )
            # logger.debug("STEP-11: rows_for_upsert -> Device {}  First={}",dev_name, rows[-1] if rows else None)
            for i, row in enumerate(rows[:3]):
                logger.debug("STEP-11: Device {}  Upsert sample[{}] -> {}",dev_name, i, row)
                # -------- STEP-11B: Upsert to PostgreSQL (idempotent) --------
            # -------- STEP-11B:  Upsert to PostgreSQL (idempotent) --------
            n = upsert_events(
                pg_engine,
                rows,
                page_size=1000,
                dry_run=DRY_RUN,
                logger=logger,
            )
            
            logger.info("STEP-11: Device {}  Upserted rows -> {}",dev_name, n)
    except Exception as e:
        logger.error("Connection error: {}", e)
        # Rethrow để systemd fail fast nếu không kết nối được
        raise
    finally:
        # TẠM THỜI đóng ngay sau smoke test (sau này giữ mở đến cuối job)
        close_cas_session(cas_session)
        close_pg_engine(pg_engine)

if __name__ == "__main__":
    main()