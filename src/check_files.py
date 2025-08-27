import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def get_csv_files(folder: Path) -> list[Path]:
    csv_files = [f for f in folder.iterdir() if f.is_file() and f.suffix.lower() == '.csv']
    if not csv_files:
        logger.warning(f"No csv files found in {folder}")
    return csv_files