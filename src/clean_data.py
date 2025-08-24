import pandas as pd
import numpy as np
from config.schema_mapping import mapping
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DataCleaner:
    def __init__(self):
        self.pending_dfs = {}
        self.processed_dfs = {}

    def add_df(self, name, df):
        self.pending_dfs[name] = df
    
    def clean_data(self, name):
        if name not in self.pending_dfs:
            logger.warning(f"No pending dataframe found with name: {name}")
            return
        else:
            df = self.pending_dfs[name]
            df = (
                df
                .pipe(self._clean_headers)
                .pipe(self._clean_ids)
                .pipe(self._clean_string_columns)
                .pipe(self._clean_date)
                .pipe(self._aggregate_data)
                .pipe(self._drop_columns)
            )
            per_customer_rev, per_product_rev = self._group_by(df)
            self.processed_dfs[name] = df
            return df, per_customer_rev, per_product_rev

           
    # Clean column names
    def _clean_headers(self, df):
        try:
            if df.shape[1] == 0:
                logger.warning("DataFrame has no columns.")
                return df
            df.columns = df.columns.str.strip().str.title().str.replace(' ', '_')
            df.rename(columns=mapping, inplace=True)
            df.dropna(how='all', inplace=True)
            return df
        except Exception as e:
            logger.error(f"Error cleaning headers: {e}")
            raise

    # Clean customer ID and product ID
    def _clean_ids(self, df):
        try:  
            if 'Customer_ID' in df.columns:
                df['Customer_ID'] = pd.to_numeric(df['Customer_ID'], errors='coerce').astype('Int64')

            if 'Product_ID' in df.columns:
                df['Product_ID'] = df['Product_ID'].astype('string').str.strip().str.replace(' ', '_')
            return df
        except Exception as e:
            logger.error(f"Error cleaning ids: {e}")

    # Clean all the string data
    def _clean_string_columns(self, df, cols = None):
        try:
            if cols is None:
                cols = ["Customer_Name", 'Sales_Rep', 'Region', 'Payment_Method', "Product_Description"]

            # Join columns and fill missing values
            df = df.reindex(columns=df.columns.union(cols))
            for col in cols:
                df[col] = (df[col].astype('string')
                           .str.strip()
                           .replace(['', ' '], np.nan)
                           .fillna("No Description Available" if col == "Product_Description" else "Unknown"))
            return df
        except Exception as e:
            logger.error(f"Fatal error occurred while cleaning string columns: {e}")
            raise

    # Clean Date
    def _clean_date(self, df, col = "Date"):
        try:
            if col not in df.columns:
                df[col] = pd.NaT
                return df
            
            # First, clean up the date strings
            df[col] = df[col].astype('string').str.strip()
            
            # Handle common date format issues
            mask_invalid_format = df[col].str.match(r'^\d{2}-\d{2}-\d{4}$', na=False)
            df.loc[mask_invalid_format, col] = df.loc[mask_invalid_format, col].str.replace(
                r'^(\d{2})-(\d{2})-(\d{4})$', r'\3-\2-\1', regex=True
            )
            
            # Convert to datetime with coerce to handle invalid dates
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # df.dropna(subset=[col], inplace= True)
            
            # Log how many dates were successfully parsed
            valid_dates = df[col].notna().sum()
            total_rows = len(df)
            # logger.info(f"Date cleaning: {valid_dates}/{total_rows} dates successfully parsed")

            
            return df
        except Exception as e:
            logger.error(f"Error Cleaning Date: {e}")
            return df

    # Aggregate data and calculate missing values
    def _aggregate_data(self, df, cols = None):
        try:   
            if cols is None:
                cols = ['Total', 'Price', 'Quantity', 'Commission', 'Tax_Amount','Net_Total']
            for col in cols:
                if col not in df.columns:
                    df[col] = np.nan
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')

            rows_before = len(df)
            # Calculate Total
            mask_total = df['Total'].isna() & df['Price'].notna() & df['Quantity'].notna()
            df.loc[mask_total, 'Total'] = df.loc[mask_total,'Price'] * df.loc[mask_total,'Quantity']

            # Calculate Price
            mask_price = df['Price'].isna() & df['Total'].notna() & df['Quantity'].notna()
            df.loc[mask_price & (df['Quantity'] != 0), 'Price'] = df.loc[mask_price, 'Total'] / df.loc[mask_price, 'Quantity']

            # Calculate Quantity
            mask_quantity = df['Quantity'].isna() & df['Total'].notna() & df['Price'].notna()
            df.loc[mask_quantity & (df['Price'] != 0), 'Quantity'] = df.loc[mask_quantity, 'Total'] / df.loc[mask_quantity, 'Price']

            # Calculate Net Total
            df['Net_Total'] = df['Total'] - (df['Commission'].fillna(0) + df['Tax_Amount'].fillna(0))

            df = (df
                .dropna(how='all')
                .dropna(subset=['Total'])
                .dropna(subset=['Price', 'Quantity'], how='all').reset_index(drop=True)
                )

            # Drop if any two rows are zeros
            drop_zeros = (df[['Total', 'Price', 'Quantity']] == 0).sum(axis=1) >= 2
            df = df[~drop_zeros].reset_index(drop=True)

            rows_after = len(df)
            dropped_rows = rows_before - rows_after
            logger.info(f"Rows before cleaning: {rows_before}")
            logger.info(f"Rows after cleaning: {rows_after}")
            logger.info(f"Rows actually dropped: {dropped_rows}")

            return df
        except Exception as e:
            logger.error(f"Error aggregating data: {e}")
            raise     

    def _group_by(self, df):
        per_customer_rev = df.groupby('Customer_Name')[['Quantity', 'Total', 'Net_Total']].sum().reset_index()
        per_product_rev  = df.groupby('Product_ID')[['Quantity', 'Total', 'Net_Total']].sum().reset_index()
        return per_customer_rev, per_product_rev


    # Drop unnecessary columns
    def _drop_columns(self, df, columns = None):
        try:
            if columns is None:
                columns = ['Email', 'Phone', 'Shipping_Address', 'Order_Priority', 'Notes']
            existing_cols = [col for col in columns if col in df.columns]
            df = (df.drop(columns=existing_cols, axis=1) if existing_cols else df)
            return df
        except Exception as e:
            logger.error(f"Error dropping columns: {e}")