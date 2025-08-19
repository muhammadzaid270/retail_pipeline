from src import config
from src.check_files import get_csv_files, get_excel_files, get_json_files
from src.load_files import load_csv_files
import logging

def main():
    config.setup_logging() 
    logger = logging.getLogger(__name__)
    logger.info("------Program Started-----")

    #Check files
    raw_csv_files = get_csv_files(config.RAW_DATA_PATH)
    if not raw_csv_files:
        logger.warning("No csv files found.")

    raw_excel_files = get_excel_files(config.RAW_DATA_PATH)
    if not raw_excel_files:
        logger.warning("No excel files found.")

    raw_json_files = get_json_files(config.RAW_DATA_PATH)
    if not raw_json_files:
        logger.warning("No JSON files found. Exiting")
        return
    
    #Load CSV Data
    df, failed_files = load_csv_files(raw_csv_files)
    if df.empty:
        logger.warning(f"No csv data loaded")
    if failed_files:
        for file in failed_files:
            logger.warning(f"The following file failed to load: {file}")
    # print(df.shape)
    # print(df.head())
    

if __name__ == '__main__':
    main()
