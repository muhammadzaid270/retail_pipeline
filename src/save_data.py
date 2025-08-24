import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
import shutil

logger = logging.getLogger(__name__)

class DataSaver:
    def __init__(
        self, 
        raw_files, 
        raw_data,
        output, 
        archive, 
        reports,
        merged_df,
        processed_dfs,
        per_customer_rev,
        per_product_rev
    ):
        self.raw_files = raw_files  #list
        self.raw_data = raw_data  #dir
        self.output = output  #dir
        self.archive = archive  #dir
        self.reports = reports  #dir
        self.merged_df = merged_df  #df
        self.processed_dfs = processed_dfs  #dict
        self.per_customer_rev = per_customer_rev  #df
        self.per_product_rev = per_product_rev  #df

    def save_data(self):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self._save_processed_output(timestamp)
        self._save_report(timestamp)
        self._move_to_archive(timestamp)

    def _save_processed_output(self, timestamp):
        for name, df in self.processed_dfs.items():
            output_file = Path(self.output) / f'Processed_{name}_{timestamp}.csv'
            if not output_file.exists():
                df.to_csv(output_file, index=False)

    def _save_report(self, timestamp):
        report_file = Path(self.reports) / f'sales_report_{timestamp}.xlsx'
        try:
            with pd.ExcelWriter(report_file, engine='xlsxwriter') as writer:
                self.merged_df.to_excel(writer, sheet_name="Sales Report", index=False)
                self.per_customer_rev.to_excel(writer, sheet_name="Customer Revenue", index=False)
                self.per_product_rev.to_excel(writer, sheet_name="Product Revenue", index=False)

                for sheet_name in writer.sheets:
                    worksheet = writer.sheets[sheet_name]
                    worksheet.set_column('A:Z', 20)
            logger.info(f"Report saved to {report_file}")
        except Exception as e:
            logger.critical(f"Fatal error occurred while saving reports: {e}")      
            raise      

    def _move_to_archive(self, timestamp):
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