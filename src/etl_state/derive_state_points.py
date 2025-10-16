from __future__ import annotations
import os
import pandas as pd
from datetime import datetime, timedelta, timezone, date, time
# import logging
from typing import Dict, List, Any, Optional, Iterable, Tuple, Sequence, Iterator, Callable
from loguru import logger
import hashlib
from sqlalchemy import text

# ---- STEP-08A - Normalize and join series by timestamp ----
DEFAULT_EVENT_COLS = {
    "watchDog": False,      # nếu thiếu hẳn series -> mặc định False
    "po": None,             # nếu thiếu -> None
    "packaging_id": None,   # nếu thiếu -> None
}

def _val_of_point(p: Dict[str, Any]) -> Any:
    """Ưu tiên bool_v -> long_v -> dbl_v -> str_v (đúng với TB)."""
    if p.get("bool_v") is not None:
        return p["bool_v"]
    if p.get("long_v") is not None:
        return p["long_v"]
    if p.get("dbl_v") is not None:
        return p["dbl_v"]
    if p.get("str_v") is not None:
        return p["str_v"]
    return None

def _to_ms(x: Any) -> int:
    """ts trong raw là bigint (ms). Đảm bảo luôn int."""
    return int(x)

def normalize_timeseries(
    raw: Dict[str, List[Dict[str, Any]]],
    keys: Sequence[str],
    defaults: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Gộp tất cả key theo trục thời gian hợp nhất (union ts), sort tăng dần,
    rồi forward-fill cho từng key. Với key không có series -> dùng defaults.

    Output: List[ { 'ts': datetime(UTC), key1: v1, key2: v2, ... }, ... ]
    """
    defaults = {**DEFAULT_EVENT_COLS, **(defaults or {})}

    # 1) Lấy series (ts, val) cho từng key
    series: Dict[str, List[Tuple[int, Any]]] = {}
    for k in keys:
        pts = raw.get(k) or []
        kv: List[Tuple[int, Any]] = []
        for p in pts:
            ts = _to_ms(p.get("ts"))
            val = _val_of_point(p)
            if ts is not None:  # bỏ qua điểm sai
                kv.append((ts, val))
        kv.sort(key=lambda x: x[0])  # sort tăng dần theo ts
        series[k] = kv

    # 2) Union tất cả timestamp
    all_ts_set = set()
    for kv in series.values():
        for ts, _ in kv:
            all_ts_set.add(ts)
    # nếu KHÔNG có điểm nào hết, trả về []
    if not all_ts_set:
        return []

    all_ts = sorted(all_ts_set)

    # 3) Forward-fill per key
    #    Nếu một key hoàn toàn không có series → khởi tạo last = defaults[key] (nếu có)
    last_vals: Dict[str, Any] = {
        k: defaults.get(k, None) for k in keys
    }
    idx: Dict[str, int] = {k: 0 for k in keys}

    timeline: List[Dict[str, Any]] = []
    for ts in all_ts:
        row: Dict[str, Any] = {"ts": datetime.fromtimestamp(ts / 1000, tz=timezone.utc)}
        for k in keys:
            kv = series[k]
            i = idx[k]
            # đẩy con trỏ đến điểm mới nhất có ts <= hiện tại
            while i < len(kv) and kv[i][0] <= ts:
                last_vals[k] = kv[i][1]
                i += 1
            idx[k] = i
            row[k] = last_vals[k]
        timeline.append(row)

    # 4) Bảo đảm các cột đặc biệt luôn có mặt và đã fill default
    for row in timeline:
        for c, dval in DEFAULT_EVENT_COLS.items():
            if c not in row or row[c] is None:
                row[c] = dval

    return timeline

# ---- STEP-08B - Apply logic for OFFLINE/CHANGE DETECTION  ----
DEFAULT_RUN_CODE = "RUN"
DEFAULT_STOP_CODE = "STOP"
DEFAULT_OFFLINE_CODE = "OFFLINE"
DEFAULT_ALARM_CODE = "ALARM"          # nếu bạn có dùng
UNKNOWN_STATE_ID = 0                   # fallback
REQUIRED_EVENT_COLS = ["ts", "reason_id", "watchDog", "po", "packaging_id"]

def ensure_event_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Bảo đảm DataFrame có đủ các cột cần để upsert vào fact_state_event.
    Thiếu cột -> thêm với default; có cột nhưng NaN -> fill default.
    """
    defaults = {
        "watchDog": False,
        "po": None,
        "packaging_id": None,
    }

    for col, dval in defaults.items():
        if col not in df.columns:
            df[col] = dval
        else:
            df[col] = df[col].fillna(dval)

    # ts, reason_id do bạn dựng từ pipeline; ở đây chỉ đảm bảo tồn tại
    if "ts" not in df.columns:
        df["ts"] = None
    if "reason_id" not in df.columns:
        df["reason_id"] = None

    return df

UNKNOWN_REASON_ID = int(os.getenv("UNKNOWN_REASON_ID", "0000"))
OFFLINE_REASON_ID = int(os.getenv("OFFLINE_REASON_ID", "0"))
COMM_LOSS_CODE  = int(os.getenv("COMM_LOSS_CODE", "9000"))

def _infer_reason_id(row: Dict[str, Any]) -> int | None:
    """Infer reason_id based on machineState, watchDog, etc."""
    rid = row.get("machineState")
    if rid is None:
        return None
    if rid is not None:
        try:
            s = str(rid).strip()
            if s.lower().startswith("bad"):  # từ TB khi OPC lỗi
                return  COMM_LOSS_CODE
            else:
                return int(rid)
        except (ValueError, TypeError):
            return UNKNOWN_REASON_ID
    try:
        return int(rid)
    except (ValueError, TypeError):
        return UNKNOWN_REASON_ID

def _infer_packaging_id(row: Dict[str, Any]) -> int | None:
    """Infer packaging_id ."""
    rid = row.get("packaging_id")
    if rid is not None:
        try:
            s = str(rid).strip()
            if s.lower().startswith("bad"):  # từ TB khi OPC lỗi
                return  None
            else:
                return int(rid)
        except (ValueError, TypeError):
            return None

def _same_bucket(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """Two rows belong to the same segment if these key values are equal."""
    keys = ("machineState", "watchDog", "po", "packaging_id")
    return all(a.get(k) == b.get(k) for k in keys)

def _safe_int(x: Any) -> Optional[int]:
    """Convert to int if possible, otherwise None (to avoid Pylance warnings)."""
    try:
        return int(x)
    except (TypeError, ValueError):
        return None



def _sanitize_po(val: Optional[str], maxlen: int = 64) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    # Bỏ mọi chuỗi Bad… từ TB/OPC UA
    if s.lower().startswith("bad"):
        return None
    # Clip chiều dài để tránh tràn DB
    return s if len(s) <= maxlen else s[:maxlen]

def _safe_po(val):
    """Return sanitized PO; only keep meaningful strings."""
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ("default", "none", "null"):
        return None
    if s.lower().startswith("bad"):  # từ TB khi OPC lỗi
        return None
    return s[:64]  # clip an toàn


def derive_state_points(
    timeline: List[Dict[str, Any]], 
    states_lookup: Dict[int, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Phân tích chuỗi trạng thái (timeline) đã được chuẩn hóa và forward-fill, chuyển thành các đoạn trạng thái liên tục.

    Args:
        timeline (List[Dict[str, Any]]): Danh sách các dict trạng thái, mỗi dict chứa thông tin tại một thời điểm.
        states_lookup (Dict[int, Dict[str, Any]]): Bảng ánh xạ reason_id sang thông tin trạng thái (state_id, state_code,...).

    Returns:
        List[Dict[str, Any]]: Danh sách các đoạn trạng thái liên tục, mỗi đoạn gồm start_ts, end_ts, state_id, reason_id, po, packaging_id, note.

    Quy trình:
        - Duyệt qua timeline, gom các trạng thái liên tục giống nhau thành một đoạn.
        - Khi trạng thái thay đổi, kết thúc đoạn hiện tại và bắt đầu đoạn mới.
        - Mỗi đoạn lưu thông tin về thời gian, trạng thái, lý do, PO, bao bì, ghi chú.
    """
    if not timeline:
        # Nếu không có dữ liệu đầu vào, trả về danh sách rỗng
        return []

    segments: List[Dict[str, Any]] = []  # Danh sách kết quả các đoạn trạng thái

    # Khởi tạo đoạn đầu tiên từ trạng thái đầu tiên
    cur = dict(timeline[0])
    cur["start_ts"] = cur["ts"]

    # Duyệt qua các trạng thái tiếp theo trong timeline
    for row in timeline[1:]:
        # Nếu trạng thái không đổi (cùng bucket), tiếp tục kéo dài đoạn hiện tại
        if _same_bucket(cur, row):
            continue

        # Nếu trạng thái thay đổi, kết thúc đoạn hiện tại
        reason_id = _infer_reason_id(cur)
        state_id = states_lookup.get(reason_id, {}).get("state_id") if reason_id is not None else None
        note = states_lookup.get(reason_id, {}).get("state_code") if reason_id is not None else None
        packaging_id = _infer_packaging_id(cur)

        prev_ts = row["ts"]  # Thời điểm kết thúc đoạn hiện tại
        seg = {
            "start_ts": cur["start_ts"],
            "end_ts": prev_ts,
            "state_id": state_id,
            "reason_id": reason_id,
            "po": _safe_po(cur.get("processOrderNr")),
            "packaging_id": packaging_id,
            "note": note,
        }
        segments.append(seg)

        # Bắt đầu đoạn mới từ trạng thái hiện tại
        cur = dict(row)
        cur["start_ts"] = row["ts"]

    # Sau khi duyệt hết, chốt lại đoạn cuối cùng
    last_ts = timeline[-1]["ts"]
    if last_ts <= cur["start_ts"]:
        last_ts = datetime.now(tz=timezone.utc)
    reason_id = _infer_reason_id(cur)
    state_id = states_lookup.get(reason_id, {}).get("state_id") if reason_id is not None else None
    note = states_lookup.get(reason_id, {}).get("state_code") if reason_id is not None else None
    packaging_id = _infer_packaging_id(cur)
    seg = {
        "start_ts": cur["start_ts"],
        "end_ts": last_ts,
        "state_id": state_id,
        "reason_id": reason_id,
        "po": _safe_po(cur.get("processOrderNr")),
        "packaging_id": packaging_id,
        "note": note,
    }
    segments.append(seg)

    return segments

# ---- STEP-08C - Compress to intervals  ----
def _split_by_boundaries(segments: List[Dict[str, Any]], cuts: List[datetime]) -> List[Dict[str, Any]]:
    """
    Cắt các đoạn tại mỗi thời điểm trong cuts (được giả định UTC, đã sort).
    """
    if not cuts:
        return segments

    cuts = sorted(cuts)
    out: List[Dict[str, Any]] = []

    for seg in segments:
        start = seg["start_ts"]
        end = seg["end_ts"]
        # tìm các cut nằm trong (start, end)
        inner = [c for c in cuts if start < c < end]
        if not inner:
            out.append(seg)
            continue

        # cắt thành nhiều phần
        points = [start] + inner + [end]
        for i in range(len(points) - 1):
            part = dict(seg)
            part["start_ts"] = points[i]
            part["end_ts"] = points[i + 1]
            out.append(part)

    return out

def coalesce_segments(
    segments: List[Dict[str, Any]],
    shift_boundaries: Optional[List[datetime]] = None,
) -> List[Dict[str, Any]]:
    """
    Gộp các đoạn liên tiếp có cùng (state_id, reason_id, po, packaging_id, watchDog).
    Nếu truyền shift_boundaries, sẽ cắt đoạn đúng rìa ca trước rồi mới gộp.
    """
    if not segments:
        return []

    # 1) cắt theo rìa ca (nếu có)
    if shift_boundaries:
        segments = _split_by_boundaries(segments, shift_boundaries)

    # 2) gộp
    out: List[Dict[str, Any]] = []
    cur = dict(segments[0])

    def _sig(s: Dict[str, Any]) -> Tuple[Any, ...]:
        return (s.get("state_id"), s.get("reason_id"), s.get("po"), s.get("packaging_id"), s.get("watchDog"))

    for seg in segments[1:]:
        if _sig(seg) == _sig(cur) and seg["start_ts"] >= cur["end_ts"]:
            # liền kề và cùng “ý nghĩa” -> kéo dài
            cur["end_ts"] = seg["end_ts"]
        else:
            out.append(cur)
            cur = dict(seg)
    out.append(cur)
    return out

# ---- STEP-09: Compress points → events (run-length)  ----
def compress_segments(segments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    STEP-09: Merge consecutive segments having same (state_id, reason_id, po, packaging_id)
    and compute duration_sec for each.

    Args:
        segments: Raw segments list (already coalesced and sorted by start_ts)

    Returns:
        List[Dict[str, Any]]: Clean, compressed segments with duration_sec
    """
    if not segments:
        return []

    # Ensure chronological order
    segments.sort(key=lambda s: s["start_ts"])
    compressed: List[Dict[str, Any]] = []
    cur = dict(segments[0])

    for seg in segments[1:]:
        same_group = (
            cur.get("state_id") == seg.get("state_id")
            and cur.get("reason_id") == seg.get("reason_id")
            and cur.get("po") == seg.get("po")
            and cur.get("packaging_id") == seg.get("packaging_id")
        )

        # Nếu cùng nhóm thì gộp lại (mở rộng end_ts)
        if same_group:
            cur["end_ts"] = seg["end_ts"]
        else:
            # chốt đoạn cũ
            cur["duration_sec"] = (
                (cur["end_ts"] - cur["start_ts"]).total_seconds()
                if isinstance(cur["end_ts"], datetime)
                else 0
            )
            compressed.append(cur)
            # bắt đầu đoạn mới
            cur = dict(seg)

    # chốt đoạn cuối
    cur["duration_sec"] = (
        (cur["end_ts"] - cur["start_ts"]).total_seconds()
        if isinstance(cur["end_ts"], datetime)
        else 0
    )
    compressed.append(cur)

    return compressed

# ---- STEP-10: Merge with history (overlap & extend)  ----
# Hai segment được coi là "cùng bucket" nếu giống hệt 4 trường này
BUCKET_KEYS = ("state_id", "reason_id", "po", "packaging_id")

# def _same_bucket(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
#     return all(a.get(k) == b.get(k) for k in BUCKET_KEYS)

def _recalc_duration(seg: Dict[str, Any]) -> None:
    seg["duration_sec"] = int((seg["end_ts"] - seg["start_ts"]).total_seconds())

def _trim_head(seg: Dict[str, Any], new_start) -> None:
    """Cắt đầu segment về new_start (UTC aware)."""
    if new_start > seg["start_ts"]:
        seg["start_ts"] = new_start
        _recalc_duration(seg)

# def _ensure_utc(dt: datetime) -> datetime:
#     """Convert naive datetime to UTC-aware if needed."""
#     if dt is None:
#         return None
#     if dt.tzinfo is None:
#         return dt.replace(tzinfo=timezone.utc)
#     return dt.astimezone(timezone.utc)

# from datetime import timezone

def _ensure_utc(dt):
    # Convert to timezone-aware UTC
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt

def _to_epoch_ms(dt):
    # dt must be UTC-aware
    return int(dt.timestamp() * 1000)

def _norm_epoch_ms(dt):
    # full pipeline: dt(any) -> UTC-aware -> epoch_ms -> (optional rounding)
    dt = _ensure_utc(dt)
    # OPTIONAL: unify precision (drop microseconds noise)
    # return (_to_epoch_ms(dt) // 1000) * 1000  # align to nearest second
    return _to_epoch_ms(dt)

def _ms_to_dt(ms):
    # only if bạn cần quay lại datetime UTC-aware
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def merge_with_history(
    last_event: Optional[Dict[str, Any]],
    new_segments: List[Dict[str, Any]],
    tolerance_sec: int = 1,
) -> List[Dict[str, Any]]:
    """
    Nối đuôi với event cuối trong DB và xử lý overlap.

    Quy tắc:
      - Nếu DB rỗng: trả lại new_segments.
      - Nếu overlap: loại bỏ phần trùng bằng cách cắt đầu segment đầu tiên tới last_event.end_ts.
      - Nếu cùng bucket và sát nhau (|gap| <= tolerance): gộp thành 1 segment (start = last_event.start_ts).
      - Bỏ mọi segment có duration_sec <= 0 sau khi cắt.
    """
    segs = [dict(s) for s in new_segments]  # copy nông
    if not new_segments:
        if last_event:
            segs = [last_event]  # wrap single dict in a list
            segs[0]["end_ts"] = datetime.now(tz=timezone.utc)
        return segs
    
    if not last_event:
        return new_segments
    
    if last_event:
        last_event["start_ts"] = _ensure_utc(last_event.get("start_ts"))
        last_event["end_ts"] = _ensure_utc(last_event.get("end_ts"))
        
    for s in new_segments:
        s["start_ts"] = _ensure_utc(s.get("start_ts"))
        s["end_ts"] = _ensure_utc(s.get("end_ts"))
    
    # segs = [dict(s) for s in new_segments]  # copy nông
    tol = timedelta(seconds=tolerance_sec)

    first = segs[0]
    logger.debug("STEP-10: merge_with_history  -> last_event start_ts {}  end_ts {}",last_event["start_ts"], last_event["end_ts"])
    for s in segs:
        logger.debug("STEP-10: merge_with_history -> new_segments start_ts {}  end_ts {}",s["start_ts"], s["end_ts"])
    # logger.debug("STEP-10: merge_with_history-> new_segments start_ts {}  end_ts {}",new_segments[0]["start_ts"], new_segments[0]["end_ts"])
    # 1) Nếu last_event phủ hoàn toàn một phần các segment đầu → loại các segment bị "nuốt"
    i = 0
    while last_event and i < len(segs) and last_event["end_ts"] >= segs[i]["end_ts"]:
        i += 1
    segs = segs[i:]
    if not segs:
        return []
    first = segs[0]
    logger.debug("STEP-10: merge_with_history -> 1: last_event phủ hoàn toàn một phần các segment đầu")
    # 2) Còn overlap một phần với segment đầu → cắt đầu
    if last_event and first["start_ts"] < last_event["end_ts"]:
        first["start_ts"] = max(first["start_ts"], last_event["end_ts"])

    if not segs:
        return []
    logger.debug("STEP-10: merge_with_history -> 2: Còn overlap một phần với segment đầu → cắt đầu")
    first = segs[0]

    # 3) Nếu cùng bucket và sát nhau → gộp bằng cách mở rộng về start_ts của last_event
    MERGE_GAP_SEC = 2  # ví dụ 1-2 giây để hút nhiễu, khác với tolerance_sec của overlap

    gap = (first["start_ts"] - last_event["end_ts"])
    same_state = (last_event.get("state_id") == first.get("state_id"))
    if same_state and abs(gap.total_seconds()) <= MERGE_GAP_SEC:
        # nối liền mạch: cho phép chạm nhau hoặc lệch vài giây
        first["start_ts"] = min(first["start_ts"], last_event["start_ts"])

    # 4) Loại các segment rỗng sau khi cắt
    segs = [s for s in segs if s["end_ts"] > s["start_ts"]]
    return segs

# ---- STEP-11 — Build idempotent keys & batches  ----
def to_epoch_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)

def make_hash_key(device_uuid: str,
                  start_ts: datetime,
                  end_ts: datetime,
                  state_id: Optional[int],
                  reason_id: Optional[int],
                  po: Optional[str],
                  packaging_id: Optional[int]) -> str:
    """
    Khóa idempotent: ổn định theo (device, start_ms, end_ms, state_id, reason_id, po, packaging_id).
    Bạn có thể lược bỏ end_ts nếu muốn 'run-length update' – ở đây mình giữ cả 2 để an toàn.
    """
    parts = [
        str(device_uuid),
        str(to_epoch_ms(start_ts)),
        str(to_epoch_ms(end_ts)),
        str(state_id if state_id is not None else ""),
        str(reason_id if reason_id is not None else ""),
        str(po if po is not None else ""),
        str(packaging_id if packaging_id is not None else "")
    ]
    raw = "|".join(parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

# def attach_hash(segments: List[Dict[str, Any]],
#                 device_uuid: str) -> List[Dict[str, Any]]:
#     for s in segments:
#         s["hash_key"] = make_hash_key(
#             device_uuid=device_uuid,
#             start_ts=s["start_ts"],
#             end_ts=s["end_ts"],
#             state_id=s.get("state_id"),
#             reason_id=s.get("reason_id"),
#             po=s.get("po"),
#             packaging_id=s.get("packaging_id"),
#         )
#     return segments

def make_batches(rows: List[Dict[str, Any]], size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
    for i in range(0, len(rows), size):
        yield rows[i:i+size]

UPSERT_SQL = text("""
INSERT INTO fact_state_event (
    hash_key,
    line_id,
    machine_id,
    state_id,
    reason_id,
    start_ts,
    end_ts,
    duration_sec,
    shift_id,
    po,
    packaging_id,
    note
) VALUES (
    :hash_key,
    :line_id,
    :machine_id,
    :state_id,
    :reason_id,
    :start_ts,
    :end_ts,
    :duration_sec,
    :shift_id,
    :po,
    :packaging_id,
    :note
)
ON CONFLICT (hash_key) DO UPDATE SET
    -- giữ nguyên hash_key; cập nhật các cột có thể thay đổi (duration, shift, note…)
    state_id      = EXCLUDED.state_id,
    reason_id     = EXCLUDED.reason_id,
    start_ts      = EXCLUDED.start_ts,
    end_ts        = EXCLUDED.end_ts,
    duration_sec  = EXCLUDED.duration_sec,
    shift_id      = EXCLUDED.shift_id,
    po            = EXCLUDED.po,
    packaging_id  = EXCLUDED.packaging_id,
    note          = EXCLUDED.note
""")

# def rows_for_upsert(segments: List[Dict[str, Any]],
#                     device_meta: Dict[str, Any],
#                     states_lookup: Dict[int, Dict[str, Any]],
#                     shifts_lookup_fn) -> List[Dict[str, Any]]:
#     """
#     Map segment -> row cho fact_state_event.
#     device_meta: {'line_id': ..., 'device_id': ..., 'machine_id': ...}
#     states_lookup: map reason_id -> state info (để lấy note/state_code nếu muốn)
#     shifts_lookup_fn(dt) -> shift_id (trả None nếu không có)
#     """
#     out: List[Dict[str, Any]] = []
#     line_id = device_meta.get("line_id")
#     device_id = device_meta.get("device_id")
#     machine_id = device_meta.get("machine_id")  # có thể None

#     for s in segments:
#         start_ts = s["start_ts"]
#         end_ts = s["end_ts"]
#         duration = max(0, int((end_ts - start_ts).total_seconds()))

#         # note: lấy từ dim_state (state_code) hoặc theo reason map
#         note = None
#         rid = s.get("reason_id")
#         if rid is not None:
#             st = states_lookup.get(rid)
#             if st:
#                 # ví dụ bạn lưu 'state_code' trong dim_state
#                 note = st.get("state_code")

#         row = {
#             "hash_key": s["hash_key"],
#             "line_id": line_id,
#             "device_id": device_id,
#             "machine_id": machine_id,
#             "state_id": s.get("state_id"),
#             "reason_id": rid,
#             "start_ts": start_ts,      # SQLAlchemy tự bind datetime -> timestamptz/ts
#             "end_ts": end_ts,
#             "duration_sec": duration,
#             "shift_id": shifts_lookup_fn(start_ts) if shifts_lookup_fn else None,
#             "po": s.get("po"),
#             "packaging_id": s.get("packaging_id"),
#             "note": note,
#         }
#         out.append(row)
#     return out

def upsert_events(pg_engine, rows: List[Dict[str, Any]], batch_size: int = 1000, dry_run: bool = False, logger=None) -> int:
    total = 0
    if not rows:
        return 0
    with pg_engine.begin() as conn:
        for batch in make_batches(rows, batch_size):
            if dry_run:
                first = batch[0] if batch else None
                if logger: logger.info("DRY_RUN: upsert {} rows (skipped execute) First row {}", len(batch), first)
                total += len(batch)
                continue
            conn.execute(UPSERT_SQL, batch)  # executemany
            total += len(batch)
    return total

Row = Dict[str, Any]
Segment = Dict[str, Any]

def attach_hash(segments: List[Segment], device_uuid: str) -> List[Segment]:
    """
    Thêm hash_key ổn định cho mỗi segment.
    """
    out: List[Segment] = []
    for s in segments:
        m = hashlib.sha256()
        # dùng các trụ cột đảm bảo idempotent
        m.update(str(device_uuid).encode())
        m.update(str(int(s["start_ts"].timestamp() * 1000)).encode())
        # m.update(str(int(s["end_ts"].timestamp() * 1000)).encode())
        m.update(str(s.get("reason_id")).encode())   # None -> "None"
        m.update(str(s.get("po")).encode())
        m.update(str(s.get("packaging_id")).encode())
        hash_key = m.hexdigest()[:32]  # gọn nhẹ 32 hexdigits
        s2 = dict(s)
        s2["hash_key"] = hash_key
        out.append(s2)
    return out

# def make_shift_lookup(shift_rows: List[Dict[str, Any]]) -> Callable[[datetime], Optional[int]]:
#     """
#     Tạo closure tra cứu shift_id theo mốc thời gian UTC.
#     shift_rows: mỗi dòng nên có 'start_utc', 'end_utc', 'shift_id' (datetime aware UTC).
#     """
#     # Đảm bảo đã sort và dùng UTC aware
#     spans = []
#     for r in shift_rows:
#         su = r["start_time"]
#         eu = r["end_time"]
#         if su.tzinfo is None: su = su.replace(tzinfo=timezone.utc)
#         if eu.tzinfo is None: eu = eu.replace(tzinfo=timezone.utc)
#         spans.append((su, eu, r["shift_no"]))
#     spans.sort(key=lambda x: x[0])

#     def _lookup(dt: datetime) -> Optional[int]:
#         if dt.tzinfo is None:
#             dt = dt.replace(tzinfo=timezone.utc)
#         # tuyến tính là đủ (n < vài trăm). Nếu nhiều thì dùng bisect.
#         for su, eu, sid in spans:
#             if su <= dt < eu:
#                 return sid
#         return None

#     return _lookup

def rows_for_upsert(
    segments: List[Segment],
    device_meta: Dict[str, Any],
    # ÁNH XẠ LÝ DO -> THÔNG TIN STATE (Option B)
    # Key là reason_id (int), value chứa ít nhất: {'state_id': int, 'state_code': str}
    states_lookup: Dict[int, Dict[str, Any]],
    shift_lookup_fn: Optional[Callable[[datetime], Optional[int]]] = None,
) -> List[Row]:
    """
    Biến các segment (đã có hash_key) thành các dòng để upsert vào fact_state_event.
    """
    
    out: List[Row] = []
    logger.debug("[DEBUG]rows_for_upsert: segments {} ",len(segments))
    for s in segments:
        start_ts: datetime = s["start_ts"]
        end_ts:   datetime = s["end_ts"]
        if start_ts.tzinfo is None: start_ts = start_ts.replace(tzinfo=timezone.utc)
        if end_ts.tzinfo is None:   end_ts   = end_ts.replace(tzinfo=timezone.utc)

        reason_id = s.get("reason_id")
        po = s.get("po")
        packaging_id = s.get("packaging_id")
        
        # map reason -> state theo Option B
        state_id = s.get("state_id")
        note = s.get("note")
        
        # shift
        shift_id = shift_lookup_fn(start_ts) if shift_lookup_fn else None

        out.append({
            "hash_key": s["hash_key"],
            "device_uuid": device_meta.get("device_id"),
            "line_id":  device_meta.get("line_id"),
            "machine_id": device_meta.get("machine_id"),
            "state_id": state_id,
            "reason_id": reason_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "shift_id": shift_id,
            "po": po,
            "packaging_id": packaging_id,
            "note": note,
        })
        # if device_meta.get("line_id") == 105:
            # logger.debug("[DEBUG]rows_for_upsert: line_id {} | start_ts {} | end_ts {}| reason_id {}| state_id {}| note {}  ",device_meta.get("line_id"),start_ts,end_ts,reason_id,state_id,note)

    return out

from zoneinfo import ZoneInfo
def make_shift_lookup(
    shift_rows: List[Dict[str, Any]],
    site_tz: ZoneInfo,                         # pytz / zoneinfo tz, ví dụ ZoneInfo("Asia/Ho_Chi_Minh")
) -> Callable[[datetime], Optional[int]]:
    """
    Tạo hàm tra cứu shift_id theo 1 mốc datetime (UTC-aware).
    - shift_rows: mỗi row có 'start_time' (datetime.time), 'end_time' (datetime.time), 'shift_no' (int)
    - site_tz: timezone tại site (để xác định ngày local, ca qua đêm, v.v.)
    Trả về: hàm lookup(dt_utc) -> shift_id (hoặc None)
    """

    # Cache spans theo ngày local để đỡ build nhiều lần
    # key = date (local), value = list[(start_utc, end_utc, shift_id)]
    spans_cache: Dict[date, List[Tuple[datetime, datetime, int]]] = {}
    # spans_cache: Dict[datetime.date, List[Tuple[datetime, datetime, int]]] = {}
    def _as_time(v) -> time:
        # Phòng khi start_time/end_time là string hoặc datetime
        if isinstance(v, time):
            return v
        if isinstance(v, datetime):
            return v.timetz() or v.time()
        if isinstance(v, str):
            # đổi định dạng nếu DB của bạn khác "HH:MM:SS"
            return datetime.strptime(v, "%H:%M:%S").time()
        raise TypeError(f"Unsupported time value: {type(v)}")

    def _build_spans_for_local_day(d: date):
        spans = []
        for r in shift_rows:
            st = _as_time(r["start_time"])
            en = _as_time(r["end_time"])

            st_local = datetime.combine(d, st, tzinfo=site_tz)
            # ca qua đêm nếu end <= start
            if en > st:
                en_local = datetime.combine(d, en, tzinfo=site_tz)
            else:
                en_local = datetime.combine(d + timedelta(days=1), en, tzinfo=site_tz)

            st_utc = st_local.astimezone(timezone.utc)
            en_utc = en_local.astimezone(timezone.utc)
            spans.append((st_utc, en_utc, int(r["shift_no"])))
        spans.sort(key=lambda x: x[0])
        return spans

    def _lookup(dt_utc: datetime) -> Optional[int]:
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)

        dt_local = dt_utc.astimezone(site_tz)   # <-- site_tz là tzinfo, không còn lỗi
        d = dt_local.date()

        if d not in spans_cache:
            spans_cache[d] = _build_spans_for_local_day(d)
        spans = spans_cache[d]

        # nếu dt nằm trước span đầu của ngày d -> xét ngày trước (ca qua đêm)
        if not spans or dt_utc < spans[0][0]:
            prev_d = d - timedelta(days=1)
            if prev_d not in spans_cache:
                spans_cache[prev_d] = _build_spans_for_local_day(prev_d)
            spans = spans_cache[prev_d]

        for su, eu, sid in spans:
            if su <= dt_utc < eu:
                return sid
        return None

    return _lookup

