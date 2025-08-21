import pandas as pd
import numpy as np
from pathlib import Path


class DataCleaner():
    def __init__(self, df):
        self.df = df

    def clean_columns(self, df):
        df.columns = df.columns.str.strip().str.title().str.replace(' ', '_')
        df.dropna(how='all', inplace=True)
        return df