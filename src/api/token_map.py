# src/etl/token_map.py
from typing import Dict
import psycopg
import logging

logger = logging.getLogger("oee.token_map")

def load_token_map(conn) -> Dict[int, str]:
    """
    Load map line_id -> oee_device_token from dim_device.
    Returns:
        dict like {101: "abcToken", 102: "defToken", ...}
    """
    sql = """
    SELECT line_id, oee_device_token
    FROM dim_device
    WHERE oee_device_token IS NOT NULL
    """
    token_map = {}
    # with psycopg.connect(conn_str) as conn:
    with conn.cursor() as cur:
        cur.execute(sql)
        for line_id, token in cur.fetchall():
            if token:
                token_map[int(line_id)] = token
    logger.info("Loaded %d device tokens from dim_device", len(token_map))
    return token_map
