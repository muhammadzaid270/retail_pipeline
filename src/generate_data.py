import pandas as pd
import numpy as np
from faker import Faker
import random
import json
from datetime import datetime, timedelta
import uuid
import os

def create_messy_sales_data():
    """
    Creates messy, realistic sales data across multiple file formats
    - 6 files total (mix of CSV, Excel, JSON)
    - 70-80 rows per file
    - 10-12 columns with inconsistent naming/formatting
    - Real-world messiness: duplicates, nulls, formatting issues, etc.
    """
    
    fake = Faker()
    
    # Base data for consistency across some files
    customers = [fake.company() for _ in range(50)]
    products = [fake.catch_phrase() for _ in range(30)]
    salespeople = [fake.name() for _ in range(15)]
    regions = ['North', 'South', 'East', 'West', 'Central', 'Northeast', 'Southwest']
    
    def generate_messy_date():
        """Generate dates in different formats"""
        date = fake.date_between(start_date='-2y', end_date='today')
        formats = [
            date.strftime('%Y-%m-%d'),
            date.strftime('%m/%d/%Y'),
            date.strftime('%d-%m-%Y'),
            date.strftime('%m-%d-%y'),
            str(date)
        ]
        return random.choice(formats)
    
    def add_typos(text, probability=0.1):
        """Add random typos to text"""
        if random.random() < probability and text:
            text = str(text)
            if len(text) > 3:
                pos = random.randint(1, len(text)-2)
                chars = list(text)
                chars[pos] = random.choice('abcdefghijklmnopqrstuvwxyz')
                return ''.join(chars)
        return text
    
    def generate_base_data(num_rows, file_type="csv"):
        """Generate base sales data with intentional messiness"""
        data = []
        
        for i in range(num_rows):
            # 5% chance of duplicate record
            if random.random() < 0.05 and len(data) > 0:
                duplicate_row = data[-1].copy()
                # Modify slightly
                if 'quantity' in duplicate_row:
                    duplicate_row['quantity'] = random.randint(1, 5)
                data.append(duplicate_row)
                continue
            
            # Base row generation
            customer_name = random.choice(customers)
            product_name = random.choice(products)
            salesperson = random.choice(salespeople)
            
            quantity = fake.random_int(1, 100) if random.random() > 0.08 else random.choice([0, None, ''])
            unit_price = round(fake.pyfloat(left_digits=3, right_digits=2, positive=True), 2)
            
            # Calculate total with potential errors
            if quantity and unit_price and pd.notna(quantity):
                if random.random() < 0.12:  # 12% calculation errors
                    total_amount = round(float(quantity) * unit_price * random.uniform(0.7, 1.4), 2)
                else:
                    total_amount = round(float(quantity) * unit_price, 2)
            else:
                total_amount = random.choice([None, 0, ''])
            
            row = {
                'customer_id': fake.random_int(1000, 9999) if random.random() > 0.05 else random.choice([None, '', 'NULL']),
                'customer_name': add_typos(customer_name) if random.random() > 0.03 else random.choice(['', None, 'N/A']),
                'product_id': f"PROD_{fake.random_int(100, 999)}" if random.random() > 0.02 else '',
                'product_name': add_typos(product_name),
                'quantity': quantity,
                'unit_price': unit_price if random.random() > 0.04 else random.choice([None, 0]),
                'total_amount': total_amount,
                'sale_date': generate_messy_date() if random.random() > 0.03 else random.choice(['', None, 'Invalid Date']),
                'salesperson': add_typos(salesperson) if random.random() > 0.02 else '',
                'region': random.choice(regions),
                'payment_method': random.choice(['Credit Card', 'Cash', 'Check', 'Bank Transfer', 'PayPal', 'Wire Transfer']),
                'discount': round(random.uniform(0, 0.25), 2) if random.random() > 0.7 else 0
            }
            
            # Add extra whitespace randomly
            for key, value in row.items():
                if isinstance(value, str) and value and random.random() < 0.15:
                    row[key] = f"  {value}  "
            
            data.append(row)
        
        return data
    
    # File 1: CSV with inconsistent headers and mixed case
    print("Creating sales_data_Q1.csv...")
    data1 = generate_base_data(random.randint(72, 78))
    df1 = pd.DataFrame(data1)
    
    # Rename columns inconsistently
    column_mapping1 = {
        'customer_id': 'cust_id',
        'customer_name': 'Customer Name',
        'product_id': 'ProductID', 
        'product_name': 'Product',
        'quantity': 'qty',
        'unit_price': 'price',
        'total_amount': 'Total',
        'sale_date': 'Date',
        'salesperson': 'Sales Rep',
        'region': 'Territory',
        'payment_method': 'Payment Method',
        'discount': 'Discount %'
    }
    df1 = df1.rename(columns=column_mapping1)
    
    # Add some completely missing columns
    df1 = df1.drop(['Discount %'], axis=1)
    
    df1.to_csv('sales_data_Q1.csv', index=False)
    
    # File 2: Excel with multiple sheets and formatting chaos
    print("Creating sales_report_2024.xlsx...")
    main_data = generate_base_data(random.randint(74, 82))
    df_main = pd.DataFrame(main_data)
    
    # Different column names for Excel
    column_mapping2 = {
        'customer_id': 'CustomerID',
        'customer_name': 'client_name',
        'product_id': 'SKU',
        'product_name': 'item_name', 
        'quantity': 'Quantity',
        'unit_price': 'cost_per_unit',
        'total_amount': 'amount',
        'sale_date': 'transaction_date',
        'salesperson': 'agent',
        'region': 'area',
        'payment_method': 'pay_type',
        'discount': 'discount_applied'
    }
    df_main = df_main.rename(columns=column_mapping2)
    
    # Create returns data (negative quantities)
    returns_data = []
    for _ in range(15):
        base_row = random.choice(main_data)
        return_row = base_row.copy()
        return_row['quantity'] = -abs(random.randint(1, int(base_row['quantity']) if base_row['quantity'] else 5))
        return_row['total_amount'] = return_row['quantity'] * base_row['unit_price'] if base_row['unit_price'] else 0
        return_row['sale_date'] = generate_messy_date()
        returns_data.append(return_row)
    
    df_returns = pd.DataFrame(returns_data)
    df_returns = df_returns.rename(columns=column_mapping2)
    
    # Clean numeric data for summary calculations
    df_main_clean = df_main.copy()
    df_returns_clean = df_returns.copy()
    
    # Convert amount columns to numeric, replacing non-numeric with 0
    df_main_clean['amount'] = pd.to_numeric(df_main_clean['amount'], errors='coerce').fillna(0)
    df_returns_clean['amount'] = pd.to_numeric(df_returns_clean['amount'], errors='coerce').fillna(0)
    
    # Summary data with merged info
    summary_data = {
        'Metric': ['Total Sales', 'Total Returns', 'Net Sales', '', 'Top Region', 'Top Salesperson'],
        'Value': [df_main_clean['amount'].sum(), df_returns_clean['amount'].sum(), 
                 df_main_clean['amount'].sum() + df_returns_clean['amount'].sum(), '',
                 df_main['area'].mode().iloc[0] if not df_main['area'].mode().empty else 'Unknown',
                 df_main['agent'].mode().iloc[0] if not df_main['agent'].mode().empty else 'Unknown']
    }
    df_summary = pd.DataFrame(summary_data)
    
    with pd.ExcelWriter('sales_report_2024.xlsx', engine='openpyxl') as writer:
        df_main.to_excel(writer, sheet_name='Sales Data', index=False)
        df_returns.to_excel(writer, sheet_name='Returns', index=False) 
        df_summary.to_excel(writer, sheet_name='Summary', index=False)
    
    # File 3: JSON with nested structure and inconsistencies
    print("Creating sales_export.json...")
    json_data = []
    base_data = generate_base_data(random.randint(71, 77))
    
    for i, row in enumerate(base_data):
        # Create inconsistent JSON structure
        if random.random() < 0.3:  # 30% nested customer info
            json_row = {
                'id': i + 1,
                'customer': {
                    'id': row['customer_id'],
                    'name': row['customer_name'],
                    'region': row['region']
                },
                'product_info': {
                    'sku': row['product_id'],
                    'description': row['product_name'],
                    'unit_cost': row['unit_price']
                },
                'transaction': {
                    'qty': row['quantity'],
                    'total': row['total_amount'],
                    'date': row['sale_date'],
                    'payment': row['payment_method']
                },
                'sales_rep': row['salesperson']
            }
        elif random.random() < 0.2:  # 20% array format for multiple items
            json_row = {
                'transaction_id': f"TXN_{i+1}",
                'customer_id': row['customer_id'],
                'customer_name': row['customer_name'],
                'items': [
                    {
                        'product_id': row['product_id'],
                        'product_name': row['product_name'],
                        'quantity': row['quantity'],
                        'price': row['unit_price'],
                        'subtotal': row['total_amount']
                    }
                ],
                'sale_date': row['sale_date'],
                'region': row['region'],
                'payment_method': row['payment_method'],
                'handled_by': row['salesperson']
            }
        else:  # Flat structure with different field names
            json_row = {
                'sale_id': f"SALE_{i+1:04d}",
                'cust_id': row['customer_id'],
                'cust_name': row['customer_name'],
                'prod_id': row['product_id'], 
                'prod_desc': row['product_name'],
                'units': row['quantity'],
                'unit_rate': row['unit_price'],
                'gross_amount': row['total_amount'],
                'trans_date': row['sale_date'],
                'territory': row['region'],
                'payment_type': row['payment_method'],
                'rep': row['salesperson'],
                'discount_pct': row.get('discount', 0)
            }
        
        json_data.append(json_row)
    
    with open('sales_export.json', 'w') as f:
        json.dump(json_data, f, indent=2, default=str)
    
    # File 4: CSV with semicolon delimiter and encoding issues
    print("Creating regional_sales.csv...")
    data4 = generate_base_data(random.randint(70, 75))
    df4 = pd.DataFrame(data4)
    
    # Different column structure
    column_mapping4 = {
        'customer_id': 'ID',
        'customer_name': 'Company',
        'product_id': 'Code',
        'product_name': 'Description',
        'quantity': 'Units',
        'unit_price': 'Rate',
        'total_amount': 'Value',
        'sale_date': 'Date',
        'salesperson': 'Rep',
        'region': 'Region',
        'payment_method': 'PayMethod'
    }
    df4 = df4.rename(columns=column_mapping4)
    df4 = df4.drop(['discount'], axis=1)
    
    # Add extra columns
    df4_clean = df4.copy()
    df4_clean['Value'] = pd.to_numeric(df4_clean['Value'], errors='coerce').fillna(0)
    df4['Commission'] = df4_clean['Value'] * 0.05
    df4['Notes'] = [fake.sentence() if random.random() > 0.7 else '' for _ in range(len(df4))]
    
    # Use semicolon separator
    df4.to_csv('regional_sales.csv', index=False, sep=';')
    
    # File 5: Excel with merged cells and weird formatting
    print("Creating monthly_summary.xlsx...")
    data5 = generate_base_data(random.randint(76, 83))
    df5 = pd.DataFrame(data5)
    
    # Add summary rows mixed in
    summary_rows = []
    for i in range(0, len(df5), 20):
        summary_row = {col: '' for col in df5.columns}
        summary_row['customer_name'] = f'=== SUBTOTAL FOR ROWS {i+1}-{min(i+20, len(df5))} ==='
        # Calculate subtotal safely
        subset_total = pd.to_numeric(df5.iloc[i:i+20]['total_amount'], errors='coerce').fillna(0).sum()
        summary_row['total_amount'] = subset_total
        summary_rows.append((i+len(summary_rows), summary_row))
    
    # Insert summary rows
    for idx, summary_row in reversed(summary_rows):
        df5 = pd.concat([df5.iloc[:idx], pd.DataFrame([summary_row]), df5.iloc[idx:]], ignore_index=True)
    
    # Different column names again
    column_mapping5 = {
        'customer_id': 'Cust#',
        'customer_name': 'Customer',
        'product_id': 'Item#',
        'product_name': 'ItemName',
        'quantity': 'Qty',
        'unit_price': 'UnitPrice',
        'total_amount': 'ExtPrice',
        'sale_date': 'SaleDate',
        'salesperson': 'SalesRep',
        'region': 'SalesRegion',
        'payment_method': 'PayType',
        'discount': 'DiscPct'
    }
    df5 = df5.rename(columns=column_mapping5)
    
    df5.to_excel('monthly_summary.xlsx', index=False)
    
    # File 6: CSV with tons of missing data and extra columns
    print("Creating sales_backup.csv...")
    data6 = generate_base_data(random.randint(78, 85))
    df6 = pd.DataFrame(data6)
    
    # Introduce way more missing data
    for col in df6.columns:
        if col not in ['customer_id', 'product_id']:  # Keep some key fields
            mask = np.random.random(len(df6)) < 0.25  # 25% missing
            df6.loc[mask, col] = random.choice([None, '', 'N/A', 'NULL'])
    
    # Add extra columns with sparse data
    df6['email'] = [fake.email() if random.random() > 0.6 else '' for _ in range(len(df6))]
    df6['phone'] = [fake.phone_number() if random.random() > 0.7 else '' for _ in range(len(df6))]
    df6['shipping_address'] = [fake.address() if random.random() > 0.8 else '' for _ in range(len(df6))]
    df6['order_priority'] = [random.choice(['High', 'Medium', 'Low', '']) for _ in range(len(df6))]
    df6['tax_amount'] = [round(float(row) * 0.08, 2) if pd.notna(row) and str(row) != '' and str(row).replace('.','').replace('-','').isdigit() else '' 
                        for row in df6['total_amount']]
    
    # Yet another naming convention
    column_mapping6 = {
        'customer_id': 'customer_id',
        'customer_name': 'cust_company',
        'product_id': 'sku_code',
        'product_name': 'product_desc',
        'quantity': 'order_qty',
        'unit_price': 'list_price',
        'total_amount': 'line_total',
        'sale_date': 'order_date',
        'salesperson': 'account_mgr',
        'region': 'sales_territory',
        'payment_method': 'payment_terms'
    }
    df6 = df6.rename(columns=column_mapping6)
    df6 = df6.drop(['discount'], axis=1)
    
    df6.to_csv('sales_backup.csv', index=False)
    
    print("\n" + "="*50)
    print("Generated messy sales data files successfully!")
    print("="*50)
    print("Files created:")
    print("1. sales_data_Q1.csv - Mixed case headers, missing discount column")
    print("2. sales_report_2024.xlsx - Multi-sheet with returns and summary")
    print("3. sales_export.json - Nested/inconsistent JSON structure")
    print("4. regional_sales.csv - Semicolon delimited with extra columns")
    print("5. monthly_summary.xlsx - Has summary rows mixed in data")
    print("6. sales_backup.csv - Lots of missing data and extra columns")
    print("\nData quality issues included:")
    print("• Inconsistent column naming across all files")
    print("• Mixed date formats and null representations")
    print("• ~5% duplicate records with variations")
    print("• ~12% calculation errors in totals")
    print("• Random typos in names and text fields")
    print("• Missing values in various formats")
    print("• Extra whitespace in string fields")
    print("• Different file delimiters and structures")
    print("• Summary rows mixed with data")
    print("• Negative quantities (returns) in some files")

if __name__ == "__main__":
    create_messy_sales_data()