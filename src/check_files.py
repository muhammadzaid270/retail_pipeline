import logging
from config.config import FOLDERS
from pathlib import Path

logger = logging.getLogger(__name__)

#Check for CSV files
def get_csv_files(folder: Path) -> list[Path]:
    csv_files = [f for f in folder.iterdir() if f.is_file() and f.suffix.lower() == '.csv']
    if not csv_files:
        logger.warning(f"No csv files found in {folder.name}")
    return csv_files