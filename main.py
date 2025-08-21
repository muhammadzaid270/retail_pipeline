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
    all_files = []
    raw_csv_files = get_csv_files(config.RAW_DATA_PATH)
    all_files.extend(raw_csv_files)
    if not raw_csv_files:
        logger.warning("No csv files found.")

    raw_excel_files = get_excel_files(config.RAW_DATA_PATH)
    all_files.extend(raw_excel_files)
    if not raw_excel_files:
        logger.warning("No excel files found.")

    raw_json_files = get_json_files(config.RAW_DATA_PATH)
    all_files.extend(raw_json_files)
    if not raw_json_files:
        logger.warning("No JSON files found. Exiting")
        return
    
    #Creating object to load data 
    load_data = DataLoader(raw_csv_files)
    dataFrames = {}
    for file_name, df in load_data:
        dataFrames[file_name] = df

    if load_data.failed_files:
        for file in load_data.failed_files:
            logger.warning(f"The following file failed to load: {file}")       

    #Creating object to clean data
    cleaner = DataCleaner()
    for name, df in dataFrames.items():
        cleaner.add_df(name, df)

    # for name in cleaner.pending_dfs.keys():
    #     choice = input(f"Do you want to clean the DataFrame '{name}'? (y/n): ").lower()
    #     if choice == 'y':
    #         cleaner.clean_pending_df(name)
    #         print(cleaner.get_cleaned_df(name).head(10))
    #     else:
    #         print(f"Skipping cleaning for DataFrame '{name}'.")
    
    target = list(cleaner.pending_dfs.keys())[0]
    for i, name in enumerate(cleaner.pending_dfs.keys()):
        cleaner.clean_pending_df(name)
        df = cleaner.get_cleaned_df(name)
        print(f"{i}) DataFrame '{name}':")
        # print(df.columns.tolist())
        print([(i+1, col) for i, col in enumerate(df.columns)])
        if name == target:
            pass


    end_time = time.time()
    print(f"Elapsed: {end_time - start_time:.2f} seconds")


if __name__ == '__main__':
    main()