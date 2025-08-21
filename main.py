from src import config
from src.check_files import get_csv_files, get_excel_files, get_json_files
from src.load_files import DataLoader
from src.clean_data import DataCleaner
import logging
import time

def main():
    start_time = time.time()
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
    
    #Creating object to load data 
    load_data = DataLoader(raw_csv_files)
    dfs = []
    for df in load_data:
        dfs.append(df)

    if load_data.failed_files:
        for file in load_data.failed_files:
            logger.warning(f"The following file failed to load: {file}")
    
    df[1] = df
    clean = DataCleaner(df)
    clean.clean_columns(df)
    
    print(df)
       

    end_time = time.time()
    print(f"Elapsed: {end_time - start_time:.2f} seconds")


if __name__ == '__main__':
    main()