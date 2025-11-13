import subprocess
from ..core.logger import get_logger

logger = get_logger(__name__)


def has_gpu():
    """Return True if nvidia-smi is available and GPU(s) detected."""
    try:
        res = subprocess.run(["nvidia-smi", "-L"], capture_output=True, text=True)
        if res.returncode == 0 and res.stdout.strip():
            logger.info("GPU detected via nvidia-smi")
            return True
    except FileNotFoundError:
        logger.info("nvidia-smi not found; assuming no GPU available")
    return False


def get_gpu_summary():
    try:
        res = subprocess.run(["nvidia-smi", "--query-gpu=name,memory.total,memory.free", "--format=csv,noheader,nounits"], capture_output=True, text=True)
        if res.returncode == 0:
            return res.stdout.strip()
    except Exception as e:
        logger.debug(f"Could not run nvidia-smi: {e}")
    return ""
