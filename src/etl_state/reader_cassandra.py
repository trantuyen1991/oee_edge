# reader_cassandra.py
from __future__ import annotations
from typing import Dict, List, Sequence, Tuple, Iterable
from datetime import datetime, timezone, timedelta
from cassandra.cluster import Session
from cassandra.query import PreparedStatement
from loguru import logger
import uuid
from collections import Counter
from partition_utils import floor_to_month_utc, ceil_to_next_month_utc
# --- Partition assumptions (ThingsBoard default: 1 day) ---
DAY_MS = 24 * 60 * 60 * 1000  # 86_400_000

def partitions_for_keys(
    session: Session,
    entity_type: str,
    entity_id: uuid.UUID,
    keys: Sequence[str],
    from_ms: int,
    to_ms: int,
) -> Dict[str, List[int]]:
    """
    Trả về danh sách partition thực tế theo từng key trong [from_ms, to_ms).
    Ưu tiên query theo cột `key` nếu schema có; nếu không có `key` thì fallback
    sang query theo (entity_type, entity_id) rồi lọc range.

    Returns:
        dict: key -> sorted(list(partition))
    """
    # Align về biên tháng UTC theo logic TB
    from_ms_aligned = floor_to_month_utc(from_ms)
    to_ms_aligned   = ceil_to_next_month_utc(to_ms)

    out: Dict[str, List[int]] = {k: [] for k in keys}

    # 1) Thử schema có cột `key`
    try:
        ps = session.prepare("""
            SELECT partition
            FROM ts_kv_partitions_cf
            WHERE entity_type = ? AND entity_id = ? AND key = ?
              AND partition >= ? AND partition < ?
            ALLOW FILTERING
        """)
        for k in keys:
            rows = session.execute(ps, (entity_type, entity_id, k, from_ms_aligned, to_ms_aligned))
            parts = sorted({r[0] for r in rows})       # <-- tuple -> r[0]
            out[k] = parts
        return out
    except Exception as e:
        logger.debug("Schema without `key` in ts_kv_partitions_cf? Falling back. Detail: {}", e)

    # 2) Fallback: không có cột `key`
    ps2 = session.prepare("""
        SELECT partition
        FROM ts_kv_partitions_cf
        WHERE entity_type = ? AND entity_id = ?
          AND partition >= ? AND partition < ?
    """)
    rows = session.execute(ps2, (entity_type, entity_id, from_ms_aligned, to_ms_aligned))
    parts_all = sorted({r[0] for r in rows})           # <-- tuple -> r[0]
    for k in keys:
        out[k] = parts_all
    return out

def prepare_stmt_ts_range(session: Session) -> PreparedStatement:
    """
    Prepare the CQL statement for reading ts_kv_cf range by partition.

    Returns:
        PreparedStatement: Prepared CQL with positional parameters.

    Example:
        ps = prepare_stmt_ts_range(session)
    """
    return session.prepare("""
        SELECT key, ts, bool_v, str_v, long_v, dbl_v
        FROM ts_kv_cf
        WHERE entity_type=? AND entity_id=? AND key=? AND partition=?
          AND ts>? AND ts<=? ALLOW FILTERING
    """)

def fetch_key_timeseries(
    session: Session,
    ps: PreparedStatement,
    device_id: uuid.UUID,
    key: str,
    partitions: Iterable[int],
    from_ms: int,
    to_ms: int,
    page_size: int = 5000,
) -> List[dict]:
    """
    Read timeseries points for a single key across given partitions and time range.

    Args:
        session (Session): Cassandra session.
        ps (PreparedStatement): Prepared statement from prepare_stmt_ts_range().
        device_id (str): ThingsBoard device UUID string.
        key (str): Telemetry key (e.g., 'machineState', 'producedCounterPC').
        partitions (Iterable[int]): Partition keys covering [from_ms, to_ms).
        from_ms (int): Start epoch ms (inclusive).
        to_ms (int): End epoch ms (exclusive).
        page_size (int): Paging size per query.

    Returns:
        List[dict]: Each item contains {'ts': int, 'bool_v':..., 'str_v':..., 'long_v':..., 'dbl_v':..., 'json_v':...}.

    Example:
        rows = fetch_key_timeseries(session, ps, device_uuid, "machineState", parts, from_ms, to_ms)
    """
    results: List[dict] = []
    for part in partitions:
        # logger.debug(
        #     "→ Executing prepared: partition={} ts>{} ts<={} (key='{}')",
        #     part, from_ms, to_ms, key
        # )
        rs = session.execute(
            ps,
            ('DEVICE',device_id, key, part, from_ms, to_ms),
            timeout=None
        )

        for row in rs:
            # row is tuple due to tuple_factory; map to dict explicitly
            results.append({
                "key":   row[0],
                "ts":    int(row[1]),
                "bool_v": row[2],
                "str_v":  row[3],
                "long_v": row[4],
                "dbl_v":  row[5],
            })

    return results

def read_timeseries_for_device(
    session: Session,
    device_id: uuid.UUID,
    keys: Sequence[str],
    from_utc: datetime,
    to_utc: datetime,
    page_size: int = 5000,
) -> Dict[str, List[dict]]:
    """
    Read multiple telemetry keys for a device within [from_utc, to_utc).

    Args:
        session (Session): Cassandra session.
        device_id (str): ThingsBoard device UUID string.
        keys (Sequence[str]): Telemetry keys to fetch.
        from_utc (datetime): UTC-aware datetime (inclusive).
        to_utc (datetime): UTC-aware datetime (exclusive).
        page_size (int): Cassandra page size.

    Returns:
        Dict[str, List[dict]]: Mapping key -> list of point dicts (sorted by ts ascending per partition; overall may need sort).

    Example:
        data = read_timeseries_for_device(cas, device_uuid, ["machineState","producedCounterPC"], from_utc, to_utc)
    """
    if from_utc.tzinfo != timezone.utc or to_utc.tzinfo != timezone.utc:
        raise ValueError("from_utc/to_utc must be UTC-aware datetime")

    from_ms = int(from_utc.timestamp() * 1000)
    to_ms = int(to_utc.timestamp() * 1000)
    
    ps = prepare_stmt_ts_range(session)
    
    parts_by_key = partitions_for_keys(session, "DEVICE", device_id, keys, from_ms, to_ms)
    out: Dict[str, List[dict]] = {}
    for k in keys:
        part_list = [int(p) for p in parts_by_key.get(k, [])]   # đảm bảo là int
        # logger.debug(
        #     "CQL >>> SELECT key, ts, bool_v, str_v, long_v, dbl_v "
        #     "FROM ts_kv_cf WHERE entity_type='DEVICE' AND entity_id={} AND key='{}' "
        #     "AND partition IN ({}) AND ts>={} AND ts<{} ALLOW FILTERING;",
        #     device_id, k, ",".join(map(str, part_list)), from_ms, to_ms
        # )

        rows = fetch_key_timeseries(session, ps, device_id, k, part_list, from_ms, to_ms, page_size=page_size)
        # rows = fetch_key_timeseries(session, ps, device_id, k, part_list, from_ms, to_ms, page_size=page_size)
        rows.sort(key=lambda r: r["ts"])
        out[k] = rows
        # logger.debug("Fetched key '{}'  points: {}  partitions={} ", k, len(rows), part_list)
    return out

def discover_keys_for_device(
    session: Session,
    device_id: uuid.UUID,
    from_utc: datetime,
    to_utc: datetime,
    sample_limit_per_part: int = 5000,
) -> Counter:
    """
    Scan partitions in [from,to) and return a Counter of keys found.
    Hữu ích khi bạn chưa chắc tên key (machineState, producedCounterPC, ...).

    Args:
        session (Session): Cassandra session.
        device_id (uuid.UUID): TB device id.
        from_utc (datetime): UTC inclusive.
        to_utc (datetime): UTC exclusive.
        sample_limit_per_part (int): hard cap rows/partition to avoid heavy scans.

    Returns:
        Counter: key -> approximate count within range.
    """
    from_ms = int(from_utc.timestamp() * 1000)
    to_ms   = int(to_utc.timestamp() * 1000)

    # Determine partitions (the helper from previous step)
    parts = partitions_for_keys(session, "DEVICE", device_id, ["machineState"], from_ms, to_ms)
    all_parts = sorted(set(sum(parts.values(), [])))  # flatten keys
    logger.debug("Partitions for discover: {}", all_parts)

    ps = session.prepare("""
        SELECT key, ts FROM ts_kv_cf
        WHERE entity_type='DEVICE' AND entity_id=? AND partition=?
          AND ts >= ? AND ts <= ?
        ALLOW FILTERING
    """)

    counts = Counter()
    for part in all_parts:
        # đảm bảo part là int, vì driver yêu cầu LongType
        if not isinstance(part, int):
            part = int(part)
        rows = session.execute(ps, (device_id, part, from_ms, to_ms))
        fetched = 0
        for row in rows:
            counts.update([row[0]])  # row[0] là key (vì tuple_factory)
            fetched += 1
            if fetched >= sample_limit_per_part:
                break
        logger.debug("Partition {} -> {} keys ({} rows)", part, len(counts), fetched)

    return counts

def sample_points(
    session: Session,
    device_id: uuid.UUID,
    key: str,
    from_utc: datetime,
    to_utc: datetime,
    limit: int = 10,
) -> list[dict]:
    """
    Trả về một số điểm đầu tiên trong [from_utc, to_utc) để eyeball.
    - Partition lấy từ ts_kv_partitions_cf theo đúng key (future-proof).
    - partition phải là int (bigint), không phải str/uuid.
    """
    if from_utc.tzinfo != timezone.utc or to_utc.tzinfo != timezone.utc:
        raise ValueError("from_utc/to_utc must be UTC-aware")

    from_ms = int(from_utc.timestamp() * 1000)
    to_ms   = int(to_utc.timestamp() * 1000)

    # Lấy partitions theo KEY (đã align theo tháng trong hàm này)
    parts_by_key = partitions_for_keys(
        session, "DEVICE", device_id, [key], from_ms, to_ms
    )
    parts = [int(p) for p in parts_by_key.get(key, [])]  # <-- ép về int
    logger.debug("sample_points: key='{}' partitions={}", key, parts)

    if not parts:
        return []

    ps = session.prepare("""
        SELECT ts, bool_v, str_v, long_v, dbl_v
        FROM ts_kv_cf
        WHERE entity_type='DEVICE' AND entity_id=? AND key=? AND partition=?
          AND ts >= ? AND ts < ?
        ALLOW FILTERING
    """)

    out: List[dict] = []
    for part in parts:
        rs = session.execute(ps, (device_id, key, part, from_ms, to_ms))
        rs.page_size = limit
        for row in rs:
            out.append({
                "ts": row[0],
                "bool_v": row[1],
                "str_v": row[2],
                "long_v": row[3],
                "dbl_v": row[4],
            })
            if len(out) >= limit:
                return out
    return out
