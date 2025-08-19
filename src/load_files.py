import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

# def load_csv_files(files): 
#     # dfs = []
#     failed_files = []
    
#     for file in files:
#         try:
#             df = pd.read_csv(file)
#             yield df
#             logger.info(f"Loaded {file.name}")

#         except pd.errors.EmptyDataError:
#             logger.error(f"File is empty: {file.name}")
#             failed_files.append(file)

#         except pd.errors.ParserError:
#             logger.error(f"Parsing error in file: {file.name}")
#             try:
#                 df = pd.read_csv(file, sep=';', on_bad_lines='skip')
#                 yield df
#                 logger.info(f"Loaded {file.name} on retry")
#             except Exception as e:
#                 logger.error(f"Retry failed for {file.name}: {e}")
#                 failed_files.append(file)

#         except FileNotFoundError:
#             logger.error(f"File not found: {file.name}")
#             failed_files.append(file)

#         except Exception as e:
#             logger.error(f"Unexpected error reading {file.name}: {e}")
#             failed_files.append(file)

#     # combined_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
#     return failed_files 


class DataLoader():
    def __init__(self, files):
        self.files = files 
        self.failed_files = []

    def __iter__(self):
        for file in self.files:
            df = self.load_file(file)
            if df is not None:
                yield df
    
    def load_file(self, file):
        suffix = Path(file).suffix.lower()

        loaders = {
            '.csv': lambda f: pd.read_csv(f),
            'xlsx': lambda f: pd.read_excel(f),
            'xls': lambda f: pd.read_excel(f),
            'json': lambda f: pd.read_json(f)
        }
        try:
            if suffix not in loaders:
                raise ValueError("No supported file type")
            else:
                return loaders[suffix](file)
        
        except pd.errors.EmptyDataError:
            logger.error(f"File is empty: {file.name}")
            self.failed_files.append(file)
            return None

        except pd.errors.ParserError:
            logger.error(f"Parsing error in file: {file.name}")
            try:
                logger.info(f"Loaded {file.name} on retry")
                return pd.read_csv(file, on_bad_lines='skip')
            except Exception as e:
                logger.error(f"Retry failed for {file.name}: {e}")
                self.failed_files.append(file)
                return None

        except FileNotFoundError:
            logger.error(f"File not found: {file.name}")
            self.failed_files.append(file)
            return None
        except Exception as e:
            logger.error(f"Unexpected error reading {file.name}: {e}")
            self.failed_files.append(file)
            return None


