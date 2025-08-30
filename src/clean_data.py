from config.schema_mapping import mapping
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class DataCleaner:
    def __init__(self) -> None:
        self.pending_dfs: dict[str, pd.DataFrame] = {}
        self.processed_dfs: dict[str, pd.DataFrame] = {}

    def add_df(self, name: str, df: pd.DataFrame) -> None:
        self.pending_dfs[name] = df
    
    def clean_data(self, name: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame] | None:
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
            customer_rev, product_rev, regional_rev, daily_rev, monthly_rev = self._group_by(df)
            self.processed_dfs[name] = df
            return df, customer_rev, product_rev, regional_rev, daily_rev, monthly_rev
           
    def _clean_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.debug("Cleaning Headers: ")
        if df.shape[1] == 0:
            logger.warning("DataFrame has no columns.")
            return df
        try:
            df.columns = (
                df.columns
                .str.strip()
                .str.title()
                .str.replace(' ', '_', regex=False)
                )
            df.rename(columns=mapping, inplace=True)
            df.dropna(how='all', inplace=True)

            logger.debug("Cleaned column headers by removing empty spaces and used column mapping to standardize column names. Standard column names: %s",print(f for f in df.colums))

            return df
        except Exception as e:
            logger.error(f"Error cleaning headers: {e}")

    def _clean_ids(self, df: pd.DataFrame) -> pd.DataFrame: 
            logger.debug("Converting 'Customer_ID' to numeric, coercing errors.")
            if 'Customer_ID' in df.columns:
                df['Customer_ID'] = pd.to_numeric(df['Customer_ID'], errors='coerce').astype('Int64')
                        
            if 'Product_ID' in df.columns:
                df['Product_ID'] = (
                    df['Product_ID']
                    .astype('string')
                    .str.strip()
                    .str.replace(' ', '_', regex=False)
                    )
                
            return df

    def _clean_string_columns(self, df: pd.DataFrame, cols: list[str] = None) -> pd.DataFrame:
        try:
            if cols is None:
                cols = ["Customer_Name", 'Sales_Rep', 'Region', 'Payment_Method', "Product_Description"]

            df = df.reindex(columns=df.columns.union(cols))
            for col in cols:
                df[col] = (
                    df[col]
                    .astype('string')
                    .str.strip()
                    .replace(['', ' '], pd.NA)
                    .fillna("No Description Available" if col == "Product_Description" else "Unknown")
                    )
            return df
        except Exception as e:
            logger.error("Error occurred while cleaning string columns: %s", e)
            raise RuntimeError("String column cleaning failed") from e

    def _clean_date(self, df: pd.DataFrame, col: str = "Date") -> pd.DataFrame:
        if col not in df.columns:
            df[col] = pd.NaT
            return df
        try:
            df[col] = df[col].astype('string').str.strip()
            
            mask_invalid_format = df[col].str.match(r'^\d{2}-\d{2}-\d{4}$', na=False)
            df.loc[mask_invalid_format, col] = df.loc[mask_invalid_format, col].str.replace(
                r'^(\d{2})-(\d{2})-(\d{4})$', r'\3-\2-\1', regex=True
                )            
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')
            df['Month'] = df[col].dt.strftime('%B %Y')
            df.dropna(subset= [col], inplace=True)
            df = df.sort_values(by=col, ascending=False).reset_index(drop=True)
            df[col] = df[col].dt.date
            return df
        except Exception as e:
            logger.error(f"Error Cleaning Date: {e}")
            
    def _aggregate_data(self, df: pd.DataFrame, cols: list[str] = None) -> pd.DataFrame:
        try:   
            if cols is None:
                cols = ['Total', 'Price', 'Quantity', 'Commission', 'Tax_Amount','Net_Total']
            for col in cols:
                if col not in df.columns:
                    df[col] = pd.NA
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')

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
                .dropna(subset=['Price', 'Quantity'], how='all')
                .reset_index(drop=True)
                )

            # Drop if any two rows are zeros
            drop_zeros = (df[['Total', 'Price', 'Quantity']] == 0).sum(axis=1) >= 2
            df = df[~drop_zeros].reset_index(drop=True)

            return df
        except Exception as e:
            logger.error(f"Error aggregating data: {e}")
            raise RuntimeError("Error aggregating") from e

    def _group_by(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        try:
            customer_rev = df.groupby('Customer_Name')[['Quantity', 'Total', 'Net_Total']].sum().reset_index()
            product_rev  = df.groupby('Product_ID')[['Quantity', 'Total', 'Net_Total']].sum().reset_index()
            regional_rev  = df.groupby('Region')[['Quantity', 'Total', 'Net_Total']].sum().reset_index()
            daily_rev  = df.groupby('Date')[['Quantity', 'Total', 'Net_Total']].sum().reset_index()
            monthly_rev  = df.groupby('Month')[['Quantity', 'Total', 'Net_Total']].sum().reset_index()
            return customer_rev, product_rev, regional_rev, daily_rev, monthly_rev
        except Exception as e:
            logging.error(f"Error occurred while grouping: {e}")

    def _drop_columns(self, df: pd.DataFrame, columns: list[str] = None) -> pd.DataFrame:
        try:
            if columns is None:
                columns = ['Email', 'Phone', 'Shipping_Address', 'Order_Priority', 'Notes'] 
            existing_cols = [col for col in columns if col in df.columns]
            df = (df.drop(columns=existing_cols, axis=1) if existing_cols else df)
            order = [
                'Date',
                'Month',
                'Customer_ID',
                'Customer_Name', 
                'Product_ID', 
                'Product_Description', 
                'Region',
                'Sales_Rep', 
                'Payment_Method', 
                'Quantity',
                'Price', 
                'Total',
                'Commission',
                'Tax_Amount',
                'Net_Total'
                ]
            df = df[order]
            return df
        except Exception as e:
            logger.error(f"Error dropping columns: {e}")