import pandas as pd
import numpy as np
from pathlib import Path


class DataCleaner():
    def __init__(self):
        self.pending_dfs = {}
        self.processed_dfs = {}

    def add_df(self, name, df):
        self.pending_dfs[name] = df
    
    def clean_pending_df(self, name):
        if name in self.pending_dfs:
            df = self.pending_dfs[name]

            def clean_columns(df):
                df.columns = df.columns.str.strip().str.title().str.replace(' ', '_')
                df.dropna(how='all', inplace=True)
                return df

            self.processed_dfs[name] = clean_columns(df)

    def get_cleaned_df(self, name):
        return self.processed_dfs.get(name)
            