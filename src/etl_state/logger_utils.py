# File: logger_utils.py
import os
import sys
from datetime import datetime, timezone
from loguru._logger import Logger
from loguru import logger as _logger

def init_logger(log_dir: str = "logs",
                log_name_prefix: str = "etl_state",
                retention_days: int = 7,
                rotation: str = "00:00",
                level: str = "INFO") -> Logger:
    """
    Initialize the Loguru logger with rotation, retention, and formatting.
    Args:
        log_dir (str): Folder to store log files.
        log_name_prefix (str): Prefix for log filenames.
        retention_days (int): How many days to keep logs.
        rotation (str): Time or size rule for log rotation (e.g., '00:00' or '10 MB').
        level (str): Default logging level ("DEBUG", "INFO", "WARNING", "ERROR").
    """
    # Create log folder if missing
    os.makedirs(log_dir, exist_ok=True)

    # Remove default handler to avoid duplicate console prints
    _logger.remove()

    # File path pattern (e.g., logs/etl_state_2025-10-11.log)
    log_file = os.path.join(
        log_dir,
        f"{log_name_prefix}_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.log"
    )

    # Add file handler with rotation and compression
    _logger.add(
        log_file,
        rotation=rotation,                # Rotate at local midnight or by size
        retention=f"{retention_days} days", # Keep recent N days
        compression="zip",                # Compress old logs to save space
        enqueue=True,                     # Safe for multi-thread/process
        encoding="utf-8",
        level=level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> "
            "| <level>{level: <8}</level> "
            "| <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
            "- <level>{message}</level>"
        ),
    )

    # Also log to console (stdout)
    _logger.add(sys.stdout, level=level, colorize=True)

    _logger.info("Logger initialized. File: {}", log_file)
    _logger.info("UTC time now: {}", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

    return _logger
