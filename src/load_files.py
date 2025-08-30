import logging
import pandas as pd
from pathlib import Path
from collections.abc import Iterator

logger = logging.getLogger(__name__)

class DataLoader():
    def __init__(self, files: list[str | Path]) -> None:
        self.files = files
        self.failed_files: list[Path] = []
    
    def __iter__(self) -> Iterator[tuple[str, pd.DataFrame]]:
        for file in self.files:
            result = self.load_file(file)
            if result is not None:
                file_name, df = result
                if not df.empty:
                   yield file_name, df
                else:
                   logger.warning(f"DataFrame is empty for file: {file_name}")
   
    def load_file(self, file: str | Path) -> tuple[str, pd.DataFrame] |  None:
        file = Path(file)
        suffix = file.suffix.lower()

        try:
            if suffix == '.csv':
                delimiters = [',', ';', '\t', '|']
                best_df = None
                best_delimiter = None
                max_columns = 0
                
                for delimiter in delimiters:
                    try:
                        test_df = pd.read_csv(file, sep=delimiter, nrows=5)
                        if test_df.shape[1] > max_columns:
                            max_columns = test_df.shape[1]
                            best_delimiter = delimiter
                            best_df = pd.read_csv(file, sep=delimiter)
                    except:
                        continue
                
                if best_df is not None:
                    df = best_df
                    logger.debug(f"Loaded CSV {file.name} using delimiter '{best_delimiter}' with {len(best_df)} rows")
                else:
                    logger.debug("Cannot find best delimiter; falling back to comma")
                    df = pd.read_csv(file, sep=',')
            else:
                logger.error(f"Not supported file type for file: {file.name}")
                self.failed_files.append(file)
                return None
            
            logger.info(f"Loaded file: {file.name} with {len(df)} rows")
            return file.name, df
        
        except Exception as e:
            logger.error(f"Error loading {file.name}: {e}")
            self.failed_files.append(file)
            return None