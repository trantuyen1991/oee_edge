# English comments per your style
import os
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

def get_cas_session():
    """
    Create a Cassandra session using env vars.
    """
    contact_points = os.getenv("CAS_CONTACT_POINTS", "127.0.0.1").split(",")
    port = int(os.getenv("CAS_PORT", "9042"))
    cluster = Cluster(contact_points=contact_points, port=port)
    session = cluster.connect(os.getenv("CAS_KEYSPACE"))
    return session

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

def fetch_state_timeline(session, device_id: str, ts_from_ms: int, ts_to_ms: int) -> List[Tuple[int, str]]:
    """
    Fetch state changes (ts_ms, state_code) over [ts_from_ms, ts_to_ms].
    RETURN: list of (ts_ms, 'RUN'|'STOP'|...) sorted by ts.
    """
    # ---- DUMMY ---- (replace with your real TB-Edge read)
    return []
