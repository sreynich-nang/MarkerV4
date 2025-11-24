import re
import math
from pathlib import Path
from io import StringIO
from typing import List
import pandas as pd

from ..core.logger import get_logger

logger = get_logger(__name__)

TABLE_REGEX = re.compile(r"(\|.*\|\s*\n)+", re.MULTILINE)
SEPARATOR_LINE_REGEX = re.compile(r'^\s*\|?\s*:?-{3,}', re.IGNORECASE)


def extract_tables_as_dataframes(md_file_path: Path) -> List[pd.DataFrame]:
    """Extract markdown tables from a file and convert them into DataFrames.

    Returns a list of DataFrames. Any table that fails to parse is skipped.
    """
    if not md_file_path.exists():
        raise FileNotFoundError(f"Markdown file not found: {md_file_path}")

    content = md_file_path.read_text(encoding="utf-8")
    tables_md = [m.group(0) for m in TABLE_REGEX.finditer(content)]

    dataframes: List[pd.DataFrame] = []
    for table_md in tables_md:
        cleaned_table = "\n".join(
            line for line in table_md.splitlines()
            if not SEPARATOR_LINE_REGEX.match(line)
        ).strip()
        if not cleaned_table:
            continue
        try:
            df = pd.read_csv(StringIO(cleaned_table), sep="|", engine="python")
            df = df.dropna(axis=1, how="all")
            df.columns = df.columns.str.strip()
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            dataframes.append(df)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to parse a table chunk: {e}")
            continue
    logger.info(f"Extracted {len(dataframes)} tables from {md_file_path.name}")
    return dataframes


def save_dfs_in_batches(
    dfs: List[pd.DataFrame],
    md_file_path: Path,
    output_dir: Path,
    sheets_per_file: int = 30,
) -> List[Path]:
    """Save DataFrames into Excel files.

    Files are written into `output_dir` which is created if absent.
    Returns list of created Excel file paths.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    created: List[Path] = []
    total = len(dfs)
    if total == 0:
        logger.info("No tables to save; skipping Excel generation.")
        return created

    batches = math.ceil(total / sheets_per_file)
    for batch_idx in range(batches):
        excel_path = output_dir / f"tables_{batch_idx + 1}.xlsx"
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:  # type: ignore[arg-type]
            for sheet_idx in range(sheets_per_file):
                df_idx = batch_idx * sheets_per_file + sheet_idx
                if df_idx >= total:
                    break
                dfs[df_idx].to_excel(writer, sheet_name=f"Sheet_{df_idx + 1}", index=False)
        created.append(excel_path)
        logger.info(f"Created Excel file: {excel_path}")
    return created


def extract_and_save_tables(
    document_name: str,
    outputs_dir: Path,
    sheets_per_file: int = 30,
    excel_base_dir: Path | None = None,
):
    """High-level helper to extract tables for a processed document and save them.

    Marker output markdown expected at: outputs_dir / document_name / document_name.md
    If excel_base_dir provided, Excel files stored under excel_base_dir / document_name.
    Otherwise defaults to outputs_dir / document_name / tables_xlsx_<document_name>.
    Returns tuple (markdown_path, tables_list, excel_files_list, excel_folder_path)
    """
    md_path = outputs_dir / document_name / f"{document_name}.md"
    if not md_path.exists():
        raise FileNotFoundError(f"Processed markdown not found for document '{document_name}': {md_path}")

    dfs = extract_tables_as_dataframes(md_path)
    if excel_base_dir:
        excel_folder = excel_base_dir / document_name
    else:
        excel_folder = outputs_dir / document_name / f"tables_xlsx_{document_name}"
    excel_files = save_dfs_in_batches(dfs, md_path, excel_folder, sheets_per_file)
    return md_path, dfs, excel_files, excel_folder
