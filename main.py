from config.config import setup_logging, FOLDERS, RAW_DATA_PATH,OUTPUT_DATA_PATH, ARCHIVE_DATA_PATH, REPORTS_PATH
from src.check_files import get_csv_files
from src.load_files import DataLoader
from src.clean_data import DataCleaner
from src.save_data import DataSaver
import logging
import time
import pandas as pd

def main():
    #Setup
    for folder in FOLDERS:
        folder.mkdir(parents=True, exist_ok=True)
    
    setup_logging() 
    logger = logging.getLogger(__name__)

    #Check files
    raw_files = get_csv_files(RAW_DATA_PATH)
    if not raw_files:
        logger.warning("No files found to continue. Exiting...")
        return

    #Load Data
    load_data = DataLoader(raw_files)
    raw_dfs = {}
    for file_name, df in load_data:
        raw_dfs[file_name] = df

    if load_data.failed_files:
        for file in load_data.failed_files:
            logger.warning(f"The following file failed to load: {file}")       

    #Clean and Transform Data
    cleaner = DataCleaner()
    for name, df in raw_dfs.items():
        cleaner.add_df(name, df)

    processed_dfs = {}
    for name in cleaner.pending_dfs.keys():
        logger.debug("\n=== Processing %s ===", name)
        logger.debug("Original shape: %s",cleaner.pending_dfs[name].shape)

        df, customer_rev, product_rev, regional_rev, daily_rev, monthly_rev = cleaner.clean_data(name)
        processed_dfs[name] = df
        
        logger.debug("After cleaning shape: %s", df.shape)

    # Merge and Save Data
    if processed_dfs:
        merged_df = pd.concat([df for df in processed_dfs.values()], ignore_index=True)
        merged_df = merged_df.sort_values(by='Date', ascending=False).reset_index(drop=True)
        logger.debug("Sucessfully merged all the processed dataframes. Shape: %s", merged_df.shape)
    else:
        logger.warning("No dataframes available to merge.")
        return
    saver = DataSaver(
        raw_files,
        RAW_DATA_PATH,
        OUTPUT_DATA_PATH,
        ARCHIVE_DATA_PATH,
        REPORTS_PATH, 
        merged_df, 
        processed_dfs, 
        customer_rev,
        product_rev,
        regional_rev,
        daily_rev,
        monthly_rev
        )
    saver.save_data()
    logger.debug("Processing completed and files saved.")

if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    elapsed = start - end
    print(f"Time elapsed: {elapsed:.2f} seconds")