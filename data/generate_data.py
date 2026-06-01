"""
generate_data.py
Sample Data Generator for Testing
Z5008 Big Data Lab - IIT Madras Zanzibar
Author: Vineet Joshi | ZDA25M007

Generates synthetic retail transaction data for testing
when the real dataset is not available.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# ============================================================
# CONFIG
# ============================================================
N_RECORDS   = 10000  # Number of records to generate
OUTPUT_PATH = 'data/sample_retail.csv'
SEED        = 42
np.random.seed(SEED)
random.seed(SEED)

# Sample data
COUNTRIES = [
    'United Kingdom', 'France', 'Germany', 'Netherlands',
    'EIRE', 'Spain', 'Belgium', 'Sweden', 'Australia',
    'Norway', 'Switzerland', 'Portugal', 'Denmark', 'Japan'
]

PRODUCTS = [
    ('85123A', 'WHITE HANGING HEART T-LIGHT HOLDER', 2.55),
    ('71053',  'WHITE METAL LANTERN', 3.39),
    ('84406B', 'CREAM CUPID HEARTS COAT HANGER', 2.75),
    ('84029G', 'KNITTED UNION FLAG HOT WATER BOTTLE', 3.39),
    ('84029E', 'RED WOOLLY HOTTIE WHITE HEART', 3.39),
    ('22752',  'SET 7 BABUSHKA NESTING BOXES', 7.65),
    ('21730',  'GLASS STAR FROSTED T-LIGHT HOLDER', 4.25),
    ('22633',  'HAND WARMER UNION JACK', 1.85),
    ('22632',  'HAND WARMER RED POLKA DOT', 1.85),
    ('84879',  'ASSORTED COLOUR BIRD ORNAMENT', 1.69),
]


def generate_invoice_date(start='2009-12-01', end='2011-12-09'):
    """Generate a random datetime between start and end."""
    start_dt = datetime.strptime(start, '%Y-%m-%d')
    end_dt   = datetime.strptime(end,   '%Y-%m-%d')
    delta    = end_dt - start_dt
    random_days  = random.randint(0, delta.days)
    random_hours = random.randint(7, 20)
    random_mins  = random.randint(0, 59)
    return start_dt + timedelta(
        days=random_days, hours=random_hours, minutes=random_mins
    )


def generate_records(n=N_RECORDS):
    """Generate n synthetic retail transaction records."""
    records = []
    invoice_num = 536365

    for i in range(n):
        # New invoice every ~5 records
        if i % 5 == 0:
            invoice_num += 1
            invoice_date = generate_invoice_date()
            customer_id  = str(random.randint(12000, 18500))
            country      = random.choices(
                COUNTRIES,
                weights=[70, 5, 4, 3, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1]
            )[0]

        stock_code, description, base_price = random.choice(PRODUCTS)
        quantity   = random.randint(1, 12)
        unit_price = round(base_price * random.uniform(0.9, 1.1), 2)
        revenue    = round(quantity * unit_price, 2)

        records.append({
            'InvoiceNo':   str(invoice_num),
            'StockCode':   stock_code,
            'Description': description,
            'Quantity':    quantity,
            'InvoiceDate': invoice_date,
            'UnitPrice':   unit_price,
            'CustomerID':  customer_id,
            'Country':     country,
            'Revenue':     revenue,
        })

    return pd.DataFrame(records)


def add_derived_columns(df):
    """Add derived time-based columns."""
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['Hour']       = df['InvoiceDate'].dt.hour
    df['DayOfWeek']  = df['InvoiceDate'].dt.dayofweek
    df['DayName']    = df['InvoiceDate'].dt.day_name()
    df['Month']      = df['InvoiceDate'].dt.month
    df['Year']       = df['InvoiceDate'].dt.year
    df['WeekOfYear'] = df['InvoiceDate'].dt.isocalendar().week.astype(int)
    df['Date']       = df['InvoiceDate'].dt.date.astype(str)
    df['MonthStr']   = df['InvoiceDate'].dt.strftime('%Y-%m')
    return df


def print_summary(df):
    """Print dataset summary."""
    print('='*50)
    print('GENERATED DATASET SUMMARY')
    print('='*50)
    print(f'Records      : {len(df):,}')
    print(f'Invoices     : {df["InvoiceNo"].nunique():,}')
    print(f'Customers    : {df["CustomerID"].nunique():,}')
    print(f'Countries    : {df["Country"].nunique()}')
    print(f'Products     : {df["StockCode"].nunique()}')
    print(f'Total Revenue: GBP {df["Revenue"].sum():,.2f}')
    print(f'Date Range   : {df["InvoiceDate"].min()} to {df["InvoiceDate"].max()}')
    print(f'Output       : {OUTPUT_PATH}')
    print('='*50)


def main():
    """Generate and save sample data."""
    print(f'Generating {N_RECORDS:,} synthetic retail records...')

    # Generate records
    df = generate_records(N_RECORDS)

    # Add derived columns
    df = add_derived_columns(df)

    # Save to CSV
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    # Print summary
    print_summary(df)
    print(f'\nDone! Use this file for testing when real dataset is unavailable.')


if __name__ == '__main__':
    main()
