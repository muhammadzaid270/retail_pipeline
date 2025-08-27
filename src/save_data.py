import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
import shutil

logger = logging.getLogger(__name__)

class DataSaver:
    def __init__(
        self, 
        raw_files: list[Path], 
        raw_data: Path,
        output: Path, 
        archive: Path, 
        reports: Path,
        merged_df: pd.DataFrame,
        processed_dfs: dict[str, pd.DataFrame],
        customer_rev: pd.DataFrame,
        product_rev: pd.DataFrame,
        regional_rev: pd.DataFrame
    ):
        self.raw_files = raw_files
        self.raw_data = raw_data
        self.output = output
        self.archive = archive
        self.reports = reports
        self.merged_df = merged_df
        self.processed_dfs = processed_dfs
        self.customer_rev = customer_rev
        self.product_rev = product_rev
        self.regional_rev = regional_rev

    def save_data(self) -> None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self._save_processed_output(timestamp)
        self._save_report(timestamp)
        self._move_to_archive(timestamp)

    def _save_processed_output(self, timestamp: str) -> None:
        for name, df in self.processed_dfs.items():
            output_file = Path(self.output) / f'Processed_{Path(name).stem}_{timestamp}.csv'
            if not output_file.exists():
                df.to_csv(output_file, index=False)

    def _save_report(self, timestamp: str) -> None:
        report_file = Path(self.reports) / f'sales_report_{timestamp}.xlsx'
        try:
            with pd.ExcelWriter(report_file, engine='xlsxwriter') as writer:
                self.merged_df.to_excel(writer, sheet_name="Sales Report", index=False)
                self.customer_rev.to_excel(writer, sheet_name="Customer Revenue", index=False)
                self.product_rev.to_excel(writer, sheet_name="Product Revenue", index=False)
                self.regional_rev.to_excel(writer, sheet_name="Regional Revenue", index=False)

                for sheet_name in writer.sheets:
                    worksheet = writer.sheets[sheet_name]
                    worksheet.set_column('A:Z', 20)
            logger.info(f"Report saved to {report_file}")
        except Exception as e:
            logger.critical(f"Fatal error occurred while saving reports: {e}")      
            raise RuntimeError("Error occurred saving report") from e

    def _move_to_archive(self, timestamp: str) -> None:
        for file in self.raw_files:
            file_path = Path(self.raw_data) / file
            try:
                if file_path.is_file() and file_path.name in self.processed_dfs:
                    shutil.move(file_path, Path(self.archive) / f"{file}_{timestamp}")
                    logger.info(f"Moved {file} to archive")
                else:
                    logger.warning(f"File not found or not processed: {file}")
            except Exception as e:
                logger.error(f"Error moving {file} to archive: {e}")