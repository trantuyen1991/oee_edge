# app.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
import os, logging, time
import httpx

app = FastAPI(title="OEE Ingest Bridge")

# --- Config (ENV) ---
TB_BASE_URL = os.getenv("TB_BASE_URL", "http://localhost:18080")
TB_DEVICE_TOKEN = os.getenv("TB_DEVICE_TOKEN", "OH1ip85DwBeDXGestuqk")  # per-device; or map by device name

# --- Logger ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("oee.ingest")

# --- Schemas ---
class TelemetryNow(BaseModel):
    """Telemetry without explicit timestamp (ThingsBoard will timestamp on arrival)."""
    values: Dict[str, Any]

class TelemetryTs(BaseModel):
    """Telemetry with explicit timestamp in milliseconds since epoch."""
    ts: int = Field(..., description="Epoch ms")
    values: Dict[str, Any]

class Attributes(BaseModel):
    """Shared attributes to update on device."""
    values: Dict[str, Any]

# --- Helper: simple retry POST ---
async def _post_with_retry(url: str, json_payload: Dict[str, Any], max_retries: int = 3, timeout_s: float = 5.0) -> httpx.Response:
    """
    POST JSON with small retry. Exponential backoff: 0.5s, 1s, 2s.
    Raises HTTPException if all attempts fail.
    """
    delay = 0.5
    for attempt in range(1, max_retries + 1):
        try:
            async with httpx.AsyncClient(timeout=timeout_s) as client:
                resp = await client.post(url, json=json_payload)
            if resp.status_code < 400:
                return resp
            logger.warning("TB POST failed (status=%s, body=%s) [attempt %d/%d]", resp.status_code, resp.text, attempt, max_retries)
        except Exception as exc:
            logger.exception("TB POST error [attempt %d/%d]: %s", attempt, max_retries, exc)
        if attempt < max_retries:
            time.sleep(delay)
            delay *= 2
    raise HTTPException(status_code=502, detail=f"Failed to POST to ThingsBoard after {max_retries} attempts")

# --- Endpoints ---
@app.post("/ingest/telemetry/now")
async def ingest_telemetry_now(body: TelemetryNow):
    """
    Receive telemetry (no timestamp) and forward to ThingsBoard device HTTP endpoint.
    """
    url = f"{TB_BASE_URL}/api/v1/{TB_DEVICE_TOKEN}/telemetry"
    payload = body.values  # TB expects flat JSON for no-ts
    resp = await _post_with_retry(url, payload)
    return {"ok": True, "status": resp.status_code}

@app.post("/ingest/telemetry/ts")
async def ingest_telemetry_ts(body: TelemetryTs):
    """
    Receive telemetry with explicit 'ts' (epoch ms) and forward to ThingsBoard.
    """
    url = f"{TB_BASE_URL}/api/v1/{TB_DEVICE_TOKEN}/telemetry"
    payload = {"ts": body.ts, "values": body.values}
    resp = await _post_with_retry(url, payload)
    return {"ok": True, "status": resp.status_code}

@app.post("/ingest/attributes")
async def ingest_attributes(body: Attributes):
    """
    Update shared attributes on the device via HTTP endpoint.
    """
    url = f"{TB_BASE_URL}/api/v1/{TB_DEVICE_TOKEN}/attributes"
    payload = body.values
    resp = await _post_with_retry(url, payload)
    return {"ok": True, "status": resp.status_code}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api.app:app", host="0.0.0.0", port=9000, reload=False, workers=1)