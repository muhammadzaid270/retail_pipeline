from config.config import setup_logging, RAW_DATA_PATH,OUTPUT_DATA_PATH, ARCHIVE_DATA_PATH, REPORTS_PATH
from src.check_files import get_csv_files, get_excel_files, get_json_files
from src.load_files import DataLoader
from src.clean_data import DataCleaner
from src.save_data import DataSaver
import logging
import time
import pandas as pd

def main():
    start_time = time.time()

def __init__(
    self,
    files: list[str],
    config: DataSaverConfig,
    dfs: DataFrames
) -> None:
    ...
-> seen this after function
    #Logging Setup
    setup_logging() 
    logger = logging.getLogger(__name__)

    logger.info("------Program Started-----")

    #Check files
    all_files = []
    raw_csv_files = get_csv_files(RAW_DATA_PATH)
    all_files.extend(raw_csv_files)
    if not raw_csv_files:
        logger.warning("No csv files found.")

    raw_excel_files = get_excel_files(RAW_DATA_PATH)
    all_files.extend(raw_excel_files)
    if not raw_excel_files:
        logger.warning("No excel files found.")

    raw_json_files = get_json_files(RAW_DATA_PATH)
    all_files.extend(raw_json_files)
    if not raw_json_files:
        logger.warning("No JSON files found. Exiting")
        return
    

    #Load Data
    load_data = DataLoader(raw_csv_files)
    raw_dfs = {}
    for file_name, df in load_data:
        raw_dfs[file_name] = df

    if load_data.failed_files:
        for file in load_data.failed_files:
            logger.warning(f"The following file failed to load: {file}")       


    #Clean Data
    cleaner = DataCleaner()
    for name, df in raw_dfs.items():
        cleaner.add_df(name, df)

    processed_dfs = {}
    for name in cleaner.pending_dfs.keys():
        print(f"\n=== Processing {name} ===")
        print(f"Original shape: {cleaner.pending_dfs[name].shape}")

        df, per_customer_rev, per_product_rev = cleaner.clean_data(name)
        processed_dfs[name] = df
        
        print(f"After cleaning shape: {df.shape}")
        
        with pd.option_context('display.max_columns' and 'display.max_rows', None):
            pass

    # Merge and Save Data
    merged_df = pd.concat([df for df in processed_dfs.values()], ignore_index=True)
    processed_dfs = [df for df in processed_dfs.values()]
    saver = DataSaver(raw_csv_files, RAW_DATA_PATH, OUTPUT_DATA_PATH, ARCHIVE_DATA_PATH, REPORTS_PATH, merged_df, processed_dfs, per_customer_rev, per_product_rev)
    saver.save_data()


    end_time = time.time()
    print(f"Elapsed: {end_time - start_time:.2f} seconds")


if __name__ == '__main__':
    main()