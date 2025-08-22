import pandas as pd
import numpy as np
from pathlib import Path
from src.schema_mapping import mapping


class DataCleaner():
    def __init__(self):
        self.pending_dfs = {}
        self.processed_dfs = {}

    def add_df(self, name, df):
        self.pending_dfs[name] = df
    
    def clean_data(self, name):
        if name in self.pending_dfs:
            df = self.pending_dfs[name]


            def clean_columns(df):
                df.columns = df.columns.str.strip().str.title().str.replace(' ', '_')
                df.rename(columns=mapping, inplace=True)
                df.dropna(how='all', inplace=True)
                return df
            self.processed_dfs[name] = clean_columns(df)


            def clean_ids(df):
                if 'Customer_ID' in df.columns:
                    df['Customer_ID'] = df['Customer_ID'].astype('Int64')
                    df['Customer_ID'] = df['Customer_ID'].fillna(np.nan)

                if 'Product_ID' in df.columns:
                    df['Product_ID'] = df['Product_ID'].astype('string')
                    df['Product_ID'] = df['Product_ID'].str.strip().str.replace(' ', '_')
                return df
            self.processed_dfs[name] = clean_ids(df)


            def clean_names(df, col = "Customer_Name"):
                if col in df.columns:
                    df[col] = df[col].str.strip().replace(['', ' '], np.nan).fillna('Unknown').astype('string')
                return df
            self.processed_dfs[name] = clean_names(df)


            def clean_description(df, col = "Product_Description"):
                if col in df.columns:
                    df[col] = df[col].str.strip().replace(['', ' '], np.nan).fillna('No Description Available').astype('string')
                return df
            self.processed_dfs[name] = clean_description(df)

            def clean_date(df, col = "Date"):
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                return df
            self.processed_dfs[name] = clean_date(df)

    def get_cleaned_df(self, name):
        return self.processed_dfs.get(name)
            