from __future__ import annotations
from typing import Dict, List, Any, Sequence, Optional, Tuple, Callable
from datetime import datetime, timezone
import logging
import re
logger = logging.getLogger(__name__)

# Default cột đặc biệt luôn có mặt; đổi cho phù hợp dự án của bạn
DEFAULT_EVENT_COLS: Dict[str, Any] = {
    "machineState": None,
    "watchDog": None,
    "processOrderNr": None,
    "producedCounterPC": None,
    "note": None,
}

def _to_ms(ts: Any) -> Optional[int]:
    """Coerce timestamp (datetime|int|float|str epoch) → epoch ms (int)."""
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        # assume ms if looks big, else s
        return int(ts if ts > 10_000_000_000 else ts * 1000)
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)  # treat as UTC-naive
        return int(ts.timestamp() * 1000)
    # string: try parse epoch
    try:
        x = float(ts)
        return int(x if x > 10_000_000_000 else x * 1000)
    except Exception:
        return None

# -------- Value picking policy ----------------------------------------------

_BAD_STATUS_RX = re.compile(r"^\s*Bad status code:", re.IGNORECASE)

# def default_value_picker(key: str, p: Dict[str, Any]) -> Any:
#     """
#     Chọn giá trị từ 1 point thô của ThingsBoard/Cassandra:
#     - Với 'machineState' (kỳ vọng số): ưu tiên long_v → dbl_v → bool_v → str_v.
#       Nếu str_v trùng mẫu 'Bad status code: ...' thì coi là None và log WARNING.
#     - Với các key khác: ưu tiên str_v → long_v → dbl_v → bool_v (an toàn).
#     """
#     str_v = p.get("str_v")
#     long_v = p.get("long_v")
#     dbl_v = p.get("dbl_v")
#     bool_v = p.get("bool_v")

#     if key == "machineState":
#         if long_v is not None:
#             return long_v
#         if dbl_v is not None:
#             try:
#                 return int(dbl_v)
#             except Exception:
#                 return None
#         if bool_v is not None:
#             return int(bool_v)
#         if isinstance(str_v, str):
#             if _BAD_STATUS_RX.match(str_v):
#                 logger.warning("Drop error-text for %s at ts=%s: %s",
#                                key, p.get("ts"), str_v[:120])
#                 return None
#             # có nơi payload về state dạng chuỗi số
#             try:
#                 return int(str_v)
#             except Exception:
#                 logger.debug("Non-numeric str for %s at ts=%s: %s -> None",
#                              key, p.get("ts"), str_v[:120])
#                 return None
#         return None

#     # Keys khác: giữ nguyên chuỗi nếu có, else số/bool
#     if str_v is not None:
#         return str_v
#     if long_v is not None:
#         return long_v
#     if dbl_v is not None:
#         return dbl_v
#     if bool_v is not None:
#         return bool_v
#     return None

def default_value_picker(key: str, p: dict) -> object:
    """
    Chọn giá trị từ 1 point thô:
    - machineState: ưu tiên long_v; nếu không có thì GIỮ nguyên str_v (kể cả 'Bad status code:...').
      Nếu vẫn chưa có thì dùng dbl_v/bool_v (thử ép int).
    - keys khác: str_v -> long_v -> dbl_v -> bool_v.
    """
    str_v = p.get("str_v")
    long_v = p.get("long_v")
    dbl_v  = p.get("dbl_v")
    bool_v = p.get("bool_v")

    if key == "machineState":  
        # 2) Nếu có chuỗi (kể cả 'Bad status code: ...') -> GIỮ NGUYÊN
        if isinstance(str_v, str):
            # Log nhẹ để sau này trace được tại sao có chuỗi
            if str_v.lower().startswith("bad"):
                logger.warning("machineState string error at ts=%s: %s", p.get("ts"), str_v[:120])
            return 9000
        
        # 1) Ưu tiên số thực sự từ nguồn
        if long_v is not None:
            logger.warning("machineState string error at ts={}: {}", p.get("ts"), long_v)
            return long_v

        # 3) Thử lấy số từ dbl_v/bool_v
        if dbl_v is not None:
            try:
                return int(dbl_v)
            except Exception:
                return dbl_v
        if bool_v is not None:
            return int(bool_v)

        return None

    # --- keys khác ---
    if str_v is not None:
        return str_v
    if long_v is not None:
        return long_v
    if dbl_v is not None:
        return dbl_v
    if bool_v is not None:
        return bool_v
    return None

# -----------------------------------------------------------------------------

def normalize_timeseries(
    raw: Dict[str, List[Dict[str, Any]]],
    keys: Sequence[str],
    defaults: Optional[Dict[str, Any]] = None,
    picker: Optional[Callable[[str, Dict[str, Any]], Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Gộp nhiều series theo các `keys` về 1 trục thời gian hợp nhất (union timestamp),
    sort tăng dần, rồi forward-fill theo từng key. Các key không có dữ liệu được
    fill bằng `defaults` (nếu có). Không ghi đè giá trị khác None bằng default.

    Parameters
    ----------
    raw : dict[str, list[dict]]
        Dữ liệu thô: { key -> [ {ts, bool_v, long_v, dbl_v, str_v}, ... ] }.
    keys : Sequence[str]
        Danh sách key cần đưa vào timeline (thứ tự dùng cho init last_vals).
    defaults : dict[str, Any] | None
        Giá trị mặc định cho một số key (ví dụ machineState=None, note=None,...).
        Mặc định sẽ merge với DEFAULT_EVENT_COLS của module.
    picker : Callable[[key, point], Any] | None
        Hàm chọn giá trị từ point. Nếu None dùng `default_value_picker`.

    Returns
    -------
    list[dict]
        Timeline hợp nhất: [ {'ts': datetime(UTC), key1: v1, key2: v2, ...}, ... ].

    Notes
    -----
    - Khi gặp chuỗi lỗi kiểu 'Bad status code: ...' ở `machineState`, hàm sẽ
      coi là None, tránh đẩy 'rác' vào pipeline state; bạn vẫn có thể log/đếm.
    - Nếu muốn giữ nguyên chuỗi lỗi, truyền picker riêng và bỏ rule drop.
    """
    picker = picker or default_value_picker
    defaults = {**DEFAULT_EVENT_COLS, **(defaults or {})}

    # 1) Lấy (ts_ms, val) cho từng key với policy picker
    series: Dict[str, List[Tuple[int, Any]]] = {}
    for k in keys:
        pts = raw.get(k) or []
        kv: List[Tuple[int, Any]] = []
        for p in pts:
            ts = _to_ms(p.get("ts"))
            if ts is None:
                logger.debug("Skip point without ts for key=%s: %s", k, p)
                continue
            val = picker(k, p)
            # giữ cả None để forward-fill hợp lý? → KHÔNG: None ở đây nghĩa “không update last”
            if val is None:
                logger.debug("Drop point key=%s ts=%s val=None (after picker)", k, p.get("ts"))
                continue
            kv.append((ts, val))
        kv.sort(key=lambda x: x[0])
        series[k] = kv

    # 2) Hợp nhất toàn bộ timestamp xuất hiện ở bất kỳ key nào
    all_ts_set = set()
    for kv in series.values():
        for ts, _ in kv:
            all_ts_set.add(ts)

    if not all_ts_set:
        logger.info("normalize_timeseries: empty union ts (no points).")
        return []

    all_ts = sorted(all_ts_set)

    # 3) Forward-fill
    last_vals: Dict[str, Any] = {k: defaults.get(k, None) for k in keys}
    idx: Dict[str, int] = {k: 0 for k in keys}
    timeline: List[Dict[str, Any]] = []

    for ts in all_ts:
        row: Dict[str, Any] = {"ts": datetime.fromtimestamp(ts / 1000, tz=timezone.utc)}
        for k in keys:
            kv = series.get(k, [])
            i = idx[k]

            # đẩy con trỏ đến điểm mới nhất có ts <= hiện tại,
            # và chỉ update last_vals khi có value hợp lệ (picker đã loại None)
            while i < len(kv) and kv[i][0] <= ts:
                last_vals[k] = kv[i][1]
                i += 1
            idx[k] = i

            row[k] = last_vals[k]
        timeline.append(row)

    # 4) Đảm bảo các cột đặc biệt luôn có mặt (không overwrite non-None)
    for row in timeline:
        for c, dval in DEFAULT_EVENT_COLS.items():
            if c not in row or row[c] is None:
                row[c] = defaults.get(c, dval)

    # Log vài dòng mẫu để debug nhanh
    if timeline:
        logger.debug("normalize_timeseries: union points=%d, first=%s", len(timeline), timeline[0])
        if len(timeline) > 1:
            logger.debug("normalize_timeseries: second=%s", timeline[1])

    return timeline
