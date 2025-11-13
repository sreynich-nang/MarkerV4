from fastapi import UploadFile
from pathlib import Path
import shutil
from ..core.config import UPLOADS_DIR, ALLOWED_EXTENSIONS
from ..core.exceptions import InvalidFileError


async def save_upload(upload_file: UploadFile) -> Path:
    """Validate uploaded file is PDF or an allowed image type and save to UPLOADS_DIR.
    Returns Path to saved file.
    """
    filename = upload_file.filename or "upload"
    suffix = Path(filename).suffix.lower()

    if suffix not in ALLOWED_EXTENSIONS:
        # Check content_type as a weaker fallback
        ct = (getattr(upload_file, "content_type", None) or "").lower()
        if not (ct.startswith("image/") or ct == "application/pdf"):
            raise InvalidFileError(f"Uploaded file type not supported: {suffix} / {ct}")

    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    target = UPLOADS_DIR / filename

    # Save streaming to disk
    with target.open("wb") as buffer:
        content = await upload_file.read()
        buffer.write(content)

    return target
