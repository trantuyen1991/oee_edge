# src/etl_state/config.py
from dataclasses import dataclass
from typing import List, Optional
import os, re, yaml
from dotenv import load_dotenv

@dataclass
class AppCfg:
    timezone: str
    lookback_h: int
    overlap_min: int
    max_backfill_h: int
    offline_grace_sec: int
    debounce_ms: int

@dataclass
class DomainTags:
    machine_state: str
    counter: str
    watchdog: Optional[str] = None
    status_str: Optional[str] = None
    po: Optional[str] = None
    reset: Optional[str] = None

@dataclass
class DomainCfg:
    tags: DomainTags
    devices: List[str]

@dataclass
class Config:
    # Global system config pulled from ENV only
    pg_dsn: str
    cas_contact_points: str
    cas_keyspace: str
    cas_port: int
    site_timezone: str

    # Job-specific from YAML (with ENV expansion)
    app: AppCfg
    domain: DomainCfg

def _expand_env(s: str) -> str:
    """
    Expand ${VAR:-default} patterns using environment variables.

    Args:
        s (str): Raw string possibly containing ${VAR} placeholders.

    Returns:
        str: Expanded string.

    Example:
        os.environ['FOO']='bar'; _expand_env('${FOO:-baz}') -> 'bar'
    """
    if not isinstance(s, str):
        return s
    # ${VAR:-default} support
    pattern = re.compile(r"\$\{([^}:]+)(?::-(.*?)|)\}")
    def repl(m):
        var, default = m.group(1), m.group(2)
        return os.getenv(var, default if default is not None else "")
    return pattern.sub(repl, s)

def load_config(path: str) -> Config:
    """
    Load configuration with ENV-first policy:
    - Global system config ONLY from .env / environment.
    - Job-specific overrides from YAML (with ${ENV} expansion).

    Args:
        path (str): Path to job YAML file.

    Returns:
        Config: Typed configuration for the etl_state job.

    Example:
        cfg = load_config("configs/etl_state.yaml")
    """
    # 1) Load .env early (global)
    load_dotenv()

    # 2) Read YAML (job-specific)
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    # ENV (global)
    pg_dsn = os.getenv("PG_DSN", "")
    cas_contact_points = os.getenv("CAS_CONTACT_POINTS", "")
    cas_keyspace = os.getenv("CAS_KEYSPACE", "")
    cas_port = int(os.getenv("CAS_PORT", "9042"))
    site_tz_env = os.getenv("SITE_TIMEZONE", "UTC")

    # YAML (job)
    app_raw = raw.get("app", {})
    dom_raw = raw.get("domain", {})
    # Allow ${ENV} in YAML values
    app_raw = {k: _expand_env(v) for k, v in app_raw.items()}
    dom_tags_raw = {k: _expand_env(v) for k, v in (dom_raw.get("tags") or {}).items()}
    dom_devices = dom_raw.get("devices", []) or []

    # Derive app cfg with fallback to site_tz_env for timezone
    app = AppCfg(
        timezone=app_raw.get("timezone") or site_tz_env,
        lookback_h=int(app_raw.get("lookback_h", 24)),
        overlap_min=int(app_raw.get("overlap_min", 3)),
        max_backfill_h=int(app_raw.get("max_backfill_h", 24)),
        offline_grace_sec=int(app_raw.get("offline_grace_sec", 30)),
        debounce_ms=int(app_raw.get("debounce_ms", 1000)),
    )

    tags = DomainTags(
        machine_state=dom_tags_raw.get("machine_state", "machineState"),
        counter=dom_tags_raw.get("counter", "producedCounterPC"),
        watchdog=dom_tags_raw.get("watchdog", "watchDog"),
        status_str=dom_tags_raw.get("status_str","status"),
        po=dom_tags_raw.get("po", "processOrderNr"),
        reset=dom_tags_raw.get("reset", "bCm_ResetCounterPC"),
    )
    domain = DomainCfg(tags=tags, devices=dom_devices)

    cfg = Config(
        pg_dsn=pg_dsn,
        cas_contact_points=cas_contact_points,
        cas_keyspace=cas_keyspace,
        cas_port=cas_port,
        site_timezone=site_tz_env,
        app=app,
        domain=domain,
    )
    validate_config(cfg)
    return cfg

def validate_config(cfg: Config) -> None:
    """
    Validate configuration semantics and raise ValueError on invalid fields.

    Args:
        cfg (Config): Configuration object.

    Returns:
        None

    Example:
        validate_config(cfg)
    """
    # ENV (global)
    if not cfg.pg_dsn:
        raise ValueError("PG_DSN is required in environment.")
    if not cfg.cas_contact_points:
        raise ValueError("CAS_CONTACT_POINTS is required in environment.")
    if not cfg.cas_keyspace:
        raise ValueError("CAS_KEYSPACE is required in environment.")
    if cfg.cas_port <= 0:
        raise ValueError("CAS_PORT must be positive.")

    # APP (job)
    if cfg.app.lookback_h <= 0:
        raise ValueError("lookback_h must be > 0")
    if cfg.app.overlap_min < 0:
        raise ValueError("overlap_min must be >= 0")
    if cfg.app.max_backfill_h <= 0:
        raise ValueError("max_backfill_h must be > 0")
    if cfg.app.offline_grace_sec < 0:
        raise ValueError("offline_grace_sec must be >= 0")

    # DOMAIN (job)
    if not cfg.domain.tags.machine_state or not cfg.domain.tags.counter:
        raise ValueError("domain.tags.machine_state and counter are required")
