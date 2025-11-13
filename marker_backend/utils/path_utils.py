from pathlib import Path
import shutil

def clean_dir(path: Path, keep: int = 0):
    """Remove files in a directory. If keep > 0, keep newest `keep` files by mtime."""
    files = sorted(path.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
    for f in files[keep:]:
        if f.is_file():
            f.unlink()
        elif f.is_dir():
            shutil.rmtree(f)
