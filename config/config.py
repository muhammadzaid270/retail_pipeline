import logging
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DATA_PATH = Path(BASE_DIR) / 'data' / 'raw'
ARCHIVE_DATA_PATH = Path(BASE_DIR) / 'data' / 'archive'
OUTPUT_DATA_PATH = Path(BASE_DIR) / 'data' / 'processed'
REPORTS_PATH = Path(BASE_DIR) / 'reports'
FOLDERS = [RAW_DATA_PATH, ARCHIVE_DATA_PATH, OUTPUT_DATA_PATH, REPORTS_PATH]

LOG_LEVEL = logging.INFO
LOG_FILE = Path(BASE_DIR) / 'logs' / 'app.log'

def setup_logging() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=LOG_LEVEL,
        format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(message)s',
        handlers= [
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(sys.stdout)
        ]
    )