from pydantic import BaseModel
from typing import Optional, List


class UploadResponse(BaseModel):
    status: str
    filename: str
    merged_path: str
    processing_time_seconds: Optional[float]


class TableExtractionResponse(BaseModel):
    status: str
    document: str
    markdown_path: str
    tables_count: int
    excel_folder: str
    excel_files: List[str]
