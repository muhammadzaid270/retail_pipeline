import logging
import pandas as pd
from pathlib import Path

#Set up logger object
logger = logging.getLogger(__name__)

class DataLoader():
    #Object Constructor for files
    def __init__(self, files):
        self.files = files 
        self.failed_files = []

    def __iter__(self):
        for file in self.files:
            result = self.load_file(file)
            if result is not None:
                file_name, df = result
                if not df.empty:
                   yield file_name, df
                else:
                   logger.warning(f"DataFrame is empty for file: {file_name}")
    
    def load_csv_with_delimiter_detection(self, file):
        """Try common delimiters to find the right one"""
        delimiters = [',', ';', '\t', '|']
        best_df = None
        best_delimiter = None
        max_columns = 0
        
        for delimiter in delimiters:
            try:
                # First try to read a few rows to test the delimiter
                test_df = pd.read_csv(file, sep=delimiter, nrows=5)
                if test_df.shape[1] > max_columns:
                    max_columns = test_df.shape[1]
                    best_delimiter = delimiter
                    # Now read the full file with the best delimiter
                    best_df = pd.read_csv(file, sep=delimiter)
            except Exception:
                continue
        
        if best_df is not None:
            logger.info(f"Successfully loaded CSV with delimiter '{best_delimiter}'")
            return best_df
        else:
            # Fallback to comma delimiter
            logger.warning(f"Could not detect delimiter, falling back to comma")
            return pd.read_csv(file, sep=',')

    def load_file(self, file):
        file = Path(file)
        suffix = file.suffix.lower()

        loaders = {
            '.csv': self.load_csv_with_delimiter_detection,
            '.xlsx': pd.read_excel,
            '.xls': pd.read_excel,
            '.json': pd.read_json
        }

        try:
            loader = loaders.get(suffix)
            if not loader:
                logger.error(f"Not supported file type for file: {file.name}")
                self.failed_files.append(file)
                return None
            else:
                df = loader(file)
                logger.info(f"Loaded file: {file.name} with {len(df)} rows")
                return file.name, df
        
        except pd.errors.EmptyDataError:
            logger.error(f"File is empty: {file.name}")
            self.failed_files.append(file)
            return None

        except pd.errors.ParserError:
            logger.error(f"Parsing error in file: {file.name}")
            if suffix == '.csv':
                try:
                    # Try with different approach - read with error handling
                    logger.info(f"Attempting to load {file.name} with error handling")
                    df = pd.read_csv(file, sep=';', error_bad_lines=False, warn_bad_lines=True)
                    logger.info(f"Loaded {file.name} with error handling, {len(df)} rows")
                    return file.name, df
                except Exception as e:
                    logger.error(f"Error handling approach failed for {file.name}: {e}")
                    self.failed_files.append(file)
                    return None
            else:
                logger.error(f"Cannot retry parsing for non-CSV file: {file.name}")
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