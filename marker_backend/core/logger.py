import logging
from logging.handlers import RotatingFileHandler
from .config import LOG_FILE, ensure_dirs

ensure_dirs()

def get_logger(name: str = "marker_backend") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    # Allow debug-level logging at the logger so handlers can filter separately
    logger.setLevel(logging.DEBUG)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch.setFormatter(ch_formatter)
    logger.addHandler(ch)

    # File handler with rotation
    # File handler should capture DEBUG-level details for troubleshooting Marker runs
    fh = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    fh.setFormatter(fh_formatter)
    logger.addHandler(fh)

    return logger
