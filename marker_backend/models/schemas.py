from pydantic import BaseModel
from typing import Optional


class UploadResponse(BaseModel):
    status: str
    filename: str
    merged_path: str
    processing_time_seconds: Optional[float]
