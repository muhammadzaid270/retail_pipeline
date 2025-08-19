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
            df = self.load_file(file)
            if df is not None and not df.empty:
                yield df
    
    def load_file(self, file):
        file = Path(file)
        suffix = file.suffix.lower()

        loaders = {
            '.csv': pd.read_csv,
            'xlsx': pd.read_excel,
            'xls': pd.read_excel,
            'json': pd.read_json
        }

        try:
            loader = loaders.get(suffix)
            if not loader:
                logger.error(f"Not supported file type for file: {file.name}")
                self.failed_files.append(file)
                return None
            else:
                df = loader(file)
                logger.info(f"Loaded file: {file.name}")
                return df
        
        except pd.errors.EmptyDataError:
            logger.error(f"File is empty: {file.name}")
            self.failed_files.append(file)
            return None

        except pd.errors.ParserError:
            logger.error(f"Parsing error in file, attempting retry with `on_bad_lines='skip'` for CSV: {file.name}")
            if suffix == '.csv':
                try:
                    df =  pd.read_csv(file, on_bad_lines='skip') 
                    logger.info(f"Loaded {file.name} on retry")
                    return df
                except Exception as e:
                    logger.error(f"Retry failed for {file.name}: {e}")
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