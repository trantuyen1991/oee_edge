USE thingsboard;
SELECT partition
FROM ts_kv_partitions_cf
WHERE entity_type = 'DEVICE'
  AND entity_id   = e54f31a0-a2f7-11ef-8f0c-...UUID...
  AND key         = 'ProdCount'
ORDER BY partition DESC;
SELECT ts, long_v
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-...UUID...
  AND key = 'ProdCount'
  AND partition = 1759276800000     -- thay bằng giá trị ở bước 2.2
ORDER BY ts DESC
LIMIT 20;
SELECT ts, long_v
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-...UUID...
  AND key = 'ProdCount'
  AND partition = 1759276800000
  AND ts >= 1759939200000    -- fromTs (epoch ms, UTC)
  AND ts <  1759946400000    -- toTs (epoch ms, UTC)
ORDER BY ts ASC
LIMIT 100;
SELECT DISTINCT key
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-...UUID...
  AND partition = 1759276800000;
-- Bool telemetry (ví dụ: Sensor_signal)
SELECT ts, bool_v
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-...UUID...
  AND key='Sensor_signal'
  AND partition = 1759276800000
ORDER BY ts DESC
LIMIT 20;

-- Double (ví dụ analog)
SELECT ts, dbl_v
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-...UUID...
  AND key='Weight'
  AND partition = 1759276800000
ORDER BY ts DESC
LIMIT 20;

-- String / JSON
SELECT ts, str_v, json_v
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-...UUID...
  AND key='processOrderNr'
  AND partition = 1759276800000
ORDER BY ts DESC
LIMIT 20;
-- ví dụ từ 16h00 đến 18h00 UTC ngày 08/10/2025
SELECT ts, long_v
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-xxxxxxxxxxxx
  AND key='ProdCount'
  AND partition = 1759276800000
  AND ts >= 1759939200000
  AND ts <  1759946400000
LIMIT 100;
SELECT DISTINCT key
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-xxxxxxxxxxxx
  AND partition = 1759276800000;
SELECT ts, long_v, WRITETIME(long_v)
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-xxxxxxxxxxxx
  AND key='ProdCount'
  AND partition = 1759276800000
ORDER BY ts DESC
LIMIT 5;
SELECT ts, long_v, TTL(long_v)
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-xxxxxxxxxxxx
  AND key='ProdCount'
  AND partition = 1759276800000
LIMIT 5;
SELECT partition
FROM ts_kv_partitions_cf
WHERE entity_type = 'DEVICE'
  AND entity_id   = e54f31a0-a2f7-11ef-8f0c-...UUID...
  AND key         = 'ProdCount'
ORDER BY partition DESC;
SELECT ts, long_v
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-...UUID...
  AND key = 'ProdCount'
  AND partition = 1759276800000     -- thay bằng giá trị ở bước 2.2
ORDER BY ts DESC
LIMIT 20;
SELECT ts, long_v
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-...UUID...
  AND key = 'ProdCount'
  AND partition = 1759276800000
  AND ts >= 1759939200000    -- fromTs (epoch ms, UTC)
  AND ts <  1759946400000    -- toTs (epoch ms, UTC)
ORDER BY ts ASC
LIMIT 100;
SELECT DISTINCT key
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-...UUID...
  AND partition = 1759276800000;
-- Bool telemetry (ví dụ: Sensor_signal)
SELECT ts, bool_v
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-...UUID...
  AND key='Sensor_signal'
  AND partition = 1759276800000
ORDER BY ts DESC
LIMIT 20;

-- Double (ví dụ analog)
SELECT ts, dbl_v
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-...UUID...
  AND key='Weight'
  AND partition = 1759276800000
ORDER BY ts DESC
LIMIT 20;

-- String / JSON
SELECT ts, str_v, json_v
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-...UUID...
  AND key='processOrderNr'
  AND partition = 1759276800000
ORDER BY ts DESC
LIMIT 20;
SELECT ts, long_v, WRITETIME(long_v), TTL(long_v)
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-...UUID...
  AND key='ProdCount'
  AND partition=1759276800000
ORDER BY ts DESC
LIMIT 5;
-- KW_FIL105
SELECT ts, long_v FROM ts_kv_cf
WHERE entity_type='DEVICE' AND entity_id = <UUID_105> AND key='ProdCount' AND partition=<PART>
ORDER BY ts DESC LIMIT 20;

-- KW_FIL107
SELECT ts, long_v FROM ts_kv_cf
WHERE entity_type='DEVICE' AND entity_id = <UUID_107> AND key='ProdCount' AND partition=<PART>
ORDER BY ts DESC LIMIT 20;

-- KW_FIL102
SELECT ts, long_v FROM ts_kv_cf
WHERE entity_type='DEVICE' AND entity_id = <UUID_102> AND key='ProdCount' AND partition=<PART>
ORDER BY ts DESC LIMIT 20;
SELECT * FROM ts_kv_partitions_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-xxxxxxxxxxxx
  AND key='ProdCount';
SELECT ts, long_v
FROM ts_kv_cf
WHERE entity_type='DEVICE'
  AND entity_id = e54f31a0-a2f7-11ef-8f0c-xxxxxxxxxxxx
  AND key='ProdCount'
  AND partition = 1759276800000
ORDER BY ts DESC
LIMIT 20;
for part in [1756684800000, 1759276800000]:
    query = f"""
    SELECT ts, long_v FROM thingsboard.ts_kv_cf
    WHERE entity_type='DEVICE'
      AND entity_id={uuid}
      AND key='ProdCount'
      AND partition={part}
    """
    # fetch and combine results
