import logging
from src.config import FOLDERS
from pathlib import Path

logger = logging.getLogger(__name__)

for folder in FOLDERS:
    Path(folder).mkdir(parents=True, exist_ok=True)

#Check for CSV files
def get_csv_files(folder):
    csv_files = [f for f in folder.iterdir() if f.is_file() and f.suffix.lower() == '.csv']
    if not csv_files:
        logger.warning(f"No csv files found in {folder.name}")
    return csv_files

#Check for excel files
def get_excel_files(folder):
    excel_files = [f for f in folder.iterdir() if f.is_file() and f.suffix.lower() == '.xlsx']
    if not excel_files:
        logger.warning(f"No excel files found in {folder.name}")
    return excel_files

#Check for json files
def get_json_files(folder):
    json_files = [f for f in folder.iterdir() if f.is_file() and f.suffix.lower() == '.json']
    if not json_files:
        logger.warning(f"No json files found in {folder.name}")
    return json_files
