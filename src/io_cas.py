# io_cas.py
from typing import List, Tuple
import os
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime, timezone
import uuid  

ENTITY_TYPE = "DEVICE"  # fixed for device telemetry

def get_cas_session():
    """
    Create a Cassandra session using env vars.
    """
    contact_points = os.getenv("CAS_CONTACT_POINTS", "127.0.0.1").split(",")
    port = int(os.getenv("CAS_PORT", "9042"))
    keyspace = os.getenv("CAS_KEYSPACE", "thingsboard")
    cluster = Cluster(contact_points=contact_points, port=port)
    session = cluster.connect(keyspace)
    return session

# def get_partitions(session, entity_id: str, key: str, ts_from_ms: int, ts_to_ms: int) -> List[int]:
#     """
#     Read partitions for (entity_id, key) covering [ts_from_ms, ts_to_ms].
#     NOTE: entity_id in TB tables is type timeuuid, so convert from string.
#     """
#      # Convert to UUID if string
#     if isinstance(entity_id, str):
#         entity_id = uuid.UUID(entity_id)

#     q = SimpleStatement("""
#         SELECT partition
#         FROM ts_kv_partitions_cf
#         WHERE entity_id = %s 
#           AND entity_type = %s 
#           AND key = %s
#         ALLOW FILTERING
#     """)

#     rows = session.execute(q, (entity_id, ENTITY_TYPE, key))
#     parts = sorted({r.partition for r in rows})
#     return parts

from datetime import datetime, timezone

def _month_floor_epoch_ms(ts_ms: int) -> int:
    """Epoch ms tại 00:00:00 UTC ngày 1 của THÁNG chứa ts_ms."""
    dt = datetime.utcfromtimestamp(ts_ms / 1000.0).replace(tzinfo=timezone.utc)
    month0 = datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)
    return int(month0.timestamp() * 1000)

def _add_month(dt: datetime) -> datetime:
    """Cộng 1 tháng cho datetime UTC (giữ 00:00:00)."""
    y, m = dt.year, dt.month
    if m == 12:
        return datetime(y + 1, 1, 1, tzinfo=timezone.utc)
    return datetime(y, m + 1, 1, tzinfo=timezone.utc)

def get_partitions(session, entity_id: str, key: str, ts_from_ms: int, ts_to_ms: int) -> list[int]:
    """
    Trả về danh sách partition theo THÁNG (epoch ms tại ngày 1, 00:00 UTC)
    phủ khoảng [ts_from_ms, ts_to_ms).
    """
    if ts_to_ms <= ts_from_ms:
        return []

    start_epoch = _month_floor_epoch_ms(ts_from_ms)
    parts: list[int] = []

    dt = datetime.utcfromtimestamp(start_epoch / 1000.0).replace(tzinfo=timezone.utc)
    end_dt = datetime.utcfromtimestamp(ts_to_ms / 1000.0).replace(tzinfo=timezone.utc)

    while dt < end_dt:
        parts.append(int(dt.timestamp() * 1000))
        dt = _add_month(dt)

    return parts

def fetch_timeseries_numeric(session, entity_id: str, key: str, ts_from_ms: int, ts_to_ms: int) -> List[Tuple[int, float]]:
    """
    Fetch numeric values (double/long) from ts_kv_cf for a given key and window.
    Returns list of (ts_ms, value) sorted ascending.
    """
    import uuid
    if isinstance(entity_id, str):
        entity_id = uuid.UUID(entity_id)

    parts = get_partitions(session, entity_id, key, ts_from_ms, ts_to_ms)
    data: List[Tuple[int, float]] = []
    q = SimpleStatement("""
        SELECT ts, dbl_v, long_v
        FROM ts_kv_cf
        WHERE entity_id = %s 
            AND entity_type = %s 
            AND key = %s 
            AND partition = %s
            AND ts >= %s AND ts < %s
        ALLOW FILTERING
    """)
    for p in parts:
        rows = session.execute(q, (entity_id, ENTITY_TYPE, key, p, ts_from_ms, ts_to_ms))
        for r in rows:
            val = r.dbl_v if r.dbl_v is not None else (float(r.long_v) if r.long_v is not None else None)
            if val is not None:
                data.append((r.ts, float(val)))
    data.sort(key=lambda x: x[0])
    return data

def fetch_cumulative_points(session, device_id: str, key: str, ts_from_ms: int, ts_to_ms: int) -> List[Tuple[int, float]]:
    """
    Fetch cumulative timeseries for a key over [ts_from_ms, ts_to_ms].
    RETURN: list of (ts_ms, value) sorted by ts.
    NOTE: Replace the SELECT with your ThingsBoard Edge schema (ts table & partitioning).
    """
    # ---- DUMMY SQL-LIKE (replace with your TB-Cassandra SELECT) ----
    # query = SimpleStatement("SELECT ts, dbl_v FROM ts_kv_cf WHERE entity_id=? AND key=? AND ts>=? AND ts<? ALLOW FILTERING")
    # rows = session.execute(query, (device_id, key, ts_from_ms, ts_to_ms))
    # return sorted([(r.ts, r.dbl_v) for r in rows], key=lambda x: x[0])
    return []  # TODO: implement your real query

def fetch_timeseries_text(session, entity_id: str, key: str, ts_from_ms: int, ts_to_ms: int) -> List[Tuple[int, str]]:
    """
    Fetch text values (str_v/json_v) for a key and window.
    """
    import uuid
    if isinstance(entity_id, str):
        entity_id = uuid.UUID(entity_id)

    parts = get_partitions(session, entity_id, key, ts_from_ms, ts_to_ms)
    data: List[Tuple[int, str]] = []
    q = SimpleStatement("""
        SELECT ts, str_v, json_v
        FROM ts_kv_cf
        WHERE entity_id = %s 
            AND entity_type = %s 
            AND key = %s 
            AND partition = %s
            AND ts >= %s AND ts < %s
        ALLOW FILTERING
    """)
    for p in parts:
        rows = session.execute(q, (entity_id, ENTITY_TYPE, key, p, ts_from_ms, ts_to_ms))
        for r in rows:
            val = r.str_v if r.str_v is not None else r.json_v
            if val is not None:
                data.append((r.ts, val))
    data.sort(key=lambda x: x[0])
    return data

def fetch_state_timeline(session, device_id: str, ts_from_ms: int, ts_to_ms: int) -> List[Tuple[int, str]]:
    """
    Fetch state changes (ts_ms, state_code) over [ts_from_ms, ts_to_ms].
    RETURN: list of (ts_ms, 'RUN'|'STOP'|...) sorted by ts.
    """
    # ---- DUMMY ---- (replace with your real TB-Edge read)
    return []

def partitions_between(ts_from_ms: int, ts_to_ms: int) -> list[int]:
    """
    Return list of partition keys covering [ts_from_ms, ts_to_ms).
    Cassandra ts_kv_cf is partitioned by 1 week (default 604800000 ms).
    Adjust PARTITION_MS if your schema uses daily partitions.
    """
    PARTITION_MS = 604800000  # 7 days; set to 86400000 if using daily partitions
    p_from = (ts_from_ms // PARTITION_MS) * PARTITION_MS
    p_to = (ts_to_ms // PARTITION_MS) * PARTITION_MS
    return list(range(p_from, p_to + PARTITION_MS, PARTITION_MS))

def fetch_state_with_quality(cas, entity_id: str, key: str, start_ms: int, end_ms: int):
    """
    Trả về list các hàng thô trong khoảng [start_ms, end_ms] cho 1 key (vd: machineState),
    gồm (ts, long_v, str_v). Dùng chính logic phân mảnh/partition như các hàm fetch hiện tại.
    """
    rows = []
    # Giả sử bạn đã có helper đi theo partition (y hệt các hàm fetch trước)
    import uuid
    if isinstance(entity_id, str):
        entity_id = uuid.UUID(entity_id)

    for part in get_partitions(session = cas,entity_id = entity_id,key = key,ts_from_ms = start_ms, ts_to_ms = end_ms):
        # NOTE: lọc theo ts >= start_ms AND ts < end_ms khi duyệt kết quả
        query = """
        SELECT ts, long_v, str_v
        FROM ts_kv_cf
        WHERE entity_type='DEVICE'
          AND entity_id=%s
          AND key=%s
          AND partition=%s
        """
        rs = cas.execute(query, (entity_id, key, part))
        for r in rs:
            ts = int(r.ts)
            if start_ms <= ts < end_ms:
                # r.long_v có thể là None, r.str_v có thể chứa "Bad status code: ..."
                rows.append((ts, r.long_v, r.str_v))
    # Sắp theo thời gian
    rows.sort(key=lambda x: x[0])
    return rows
