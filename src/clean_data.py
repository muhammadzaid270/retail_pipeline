import pandas as pd
import numpy as np
from config.schema_mapping import mapping
import logging

logger = logging.getLogger(__name__)


class DataCleaner():
    def __init__(self):
        self.pending_dfs = {}
        self.processed_dfs = {}

    def add_df(self, name, df):
        self.pending_dfs[name] = df
    
    def clean_data(self, name):
        # Retrieve pending dataframes
        if name in self.pending_dfs:
            df = self.pending_dfs[name]

            # Clean column names
            def clean_columns(df):
                df.columns = df.columns.str.strip().str.title().str.replace(' ', '_')
                df.rename(columns=mapping, inplace=True)
                df.dropna(how='all', inplace=True)
                return df
            self.processed_dfs[name] = clean_columns(df)

            # Clean customer ID and product ID
            def clean_ids(df):
                try:  
                    if 'Customer_ID' in df.columns:
                        df['Customer_ID'] = df['Customer_ID'].astype('Int64').fillna(np.nan)

                    if 'Product_ID' in df.columns:
                        df['Product_ID'] = df['Product_ID'].astype('string').str.strip().str.replace(' ', '_')
                    return df
                except Exception as e:
                    logger.error(f"Error cleaning ids: {e}")
            self.processed_dfs[name] = clean_ids(df)

            # Clean all the string data
            def clean_strings(df, cols = None):
                try:
                    if cols is None:
                        cols = ["Customer_Name", 'Sales_Rep', 'Region', 'Payment_Method', "Product_Description"]
                    for col in cols:
                        if col not in df.columns:
                            df[col] = np.nan
                        df[col] = df[col].str.strip().replace(['', ' '], np.nan).astype('string')

                        if df[col] is df["Product_Description"]:
                            df[col] = df[col].fillna('No Description Available.')
                        else:
                            df[col] =  df[col].fillna('Unknown')
                    return df
                except Exception as e:
                    logger.error(f"Fatal error occurred while cleaning string columns: {e}")
                    raise
            self.processed_dfs[name] = clean_strings(df)

            # Clean Date
            def clean_date(df, col = "Date"):
                try:
                    if col not in df.columns:
                        df[col] = pd.NaT
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    df.dropna(subset="Date", inplace=True)
                    return df
                except Exception as e:
                    logger.error(f"Error Cleaning Date: {e}")
            self.processed_dfs[name] = clean_date(df)

            # Aggregate data and calculate missing values
            def aggregate_data(df, cols = None):
                try:   
                    if cols is None:
                        cols = ['Total', 'Price', 'Quantity', 'Commission', 'Tax_Amount','Net_Total']
                    for col in cols:
                        if col not in df.columns:
                            df[col] = np.nan
                    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')

                    mask_total = df['Total'].isna() & df['Price'].notna() & df['Quantity'].notna()
                    df.loc[mask_total, 'Total'] = df['Price'] * df['Quantity']

                    mask_price = df['Price'].isna() & df['Total'].notna() & df['Quantity'].notna()
                    df.loc[mask_price, 'Price'] = df['Total'] / df['Quantity']

                    mask_quantity = df['Quantity'].isna() & df['Total'].notna() & df['Price'].notna()
                    df.loc[mask_quantity, 'Quantity'] = df['Total'] / df['Price']

                    df.dropna(subset=['Total'], inplace=True)
                    df.dropna(subset=['Price', 'Quantity'], how='all', inplace=True) 

                    df['Net_Total'] = df['Total'] - (df['Commission'].fillna(0) - df['Tax_Amount'].fillna(0))
                    return df

                except Exception as e:
                    logger.error(f"Error processing numeric columns: {e}")
                    raise
            
            self.processed_dfs[name] = aggregate_data(df) 


    def get_cleaned_df(self, name):
        return self.processed_dfs.get(name)        