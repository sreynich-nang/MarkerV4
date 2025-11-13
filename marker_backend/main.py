from fastapi import FastAPI
import uvicorn
from .api.endpoints import router as api_router
from .core.config import ensure_dirs, HOST, PORT
from .core.logger import get_logger

ensure_dirs()
logger = get_logger(__name__)

app = FastAPI(title="Marker Backend")
app.include_router(api_router, prefix="/api")


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    logger.info(f"Starting Marker Backend on http://{HOST}:{PORT}")
    uvicorn.run("marker_backend.main:app", host=HOST, port=PORT, log_level="info")