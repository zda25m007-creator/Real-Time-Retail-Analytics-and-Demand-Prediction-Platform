"""
retail_batch_job.py
Production Spark Batch Processing Job
Z5008 Big Data Lab - IIT Madras Zanzibar
Author: Vineet Joshi | ZDA25M007
"""

import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import pyarrow as pa

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# STORAGE CONFIG
# ============================================================
STORAGE = {
    'AWS_ENDPOINT_URL':           'http://minio:9000',
    'AWS_ACCESS_KEY_ID':          'admin',
    'AWS_SECRET_ACCESS_KEY':      'bigdata123',
    'AWS_ALLOW_HTTP':             'true',
    'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
    'AWS_REGION':                 'us-east-1'
}

DELTA_TRANSACTIONS = 's3://retail-v2/delta/transactions'
DELTA_COUNTRY_AGG  = 's3://retail-v2/delta/spark_country_agg'
DELTA_DAILY_MA     = 's3://retail-v2/delta/spark_daily_ma'
DELTA_MONTHLY      = 's3://retail-v2/delta/spark_monthly'


def create_spark_session():
    """Create and return a Spark session."""
    spark = SparkSession.builder \
        .appName('RetailAnalytics-BatchJob') \
        .master('local[*]') \
        .config('spark.eventLog.enabled', 'false') \
        .config('spark.sql.shuffle.partitions', '4') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    logger.info(f'Spark session created: version {spark.version}')
    return spark


def load_data(spark):
    """Load transaction data from Delta Lake."""
    logger.info('Loading data from Delta Lake...')
    df_pd = DeltaTable(DELTA_TRANSACTIONS, storage_options=STORAGE).to_pandas()
    df_pd['InvoiceDate'] = pd.to_datetime(df_pd['InvoiceDate'])
    df_pd['Revenue'] = pd.to_numeric(df_pd['Revenue'], errors='coerce')
    df_pd['Quantity'] = pd.to_numeric(df_pd['Quantity'], errors='coerce')
    df_spark = spark.createDataFrame(df_pd)
    df_spark.createOrReplaceTempView('transactions')
    count = df_spark.count()
    logger.info(f'Loaded {count:,} records into Spark')
    return df_spark


def compute_country_aggregations(df_spark):
    """Compute country-level revenue aggregations with RANK."""
    logger.info('Computing country aggregations...')

    country_window = Window.orderBy(F.desc('TotalRevenue'))

    df_country = df_spark.groupBy('Country').agg(
        F.round(F.sum('Revenue'), 2).alias('TotalRevenue'),
        F.countDistinct('InvoiceNo').alias('TotalInvoices'),
        F.countDistinct('CustomerID').alias('UniqueCustomers'),
        F.round(F.avg('Revenue'), 2).alias('AvgOrderValue'),
        F.sum('Quantity').alias('TotalQuantity')
    ).withColumn(
        'RevenueRank', F.rank().over(country_window)
    ).orderBy('RevenueRank')

    logger.info(f'Country aggregations: {df_country.count()} countries')
    return df_country


def compute_daily_moving_average(df_spark):
    """Compute daily revenue with 7-day moving average."""
    logger.info('Computing daily moving averages...')

    df_daily = df_spark.groupBy('Date').agg(
        F.round(F.sum('Revenue'), 2).alias('DailyRevenue'),
        F.countDistinct('InvoiceNo').alias('DailyInvoices')
    ).orderBy('Date')

    date_window = Window.orderBy('Date').rowsBetween(-6, 0)

    df_daily_ma = df_daily.withColumn(
        'MA7_Revenue',
        F.round(F.avg('DailyRevenue').over(date_window), 2)
    ).withColumn(
        'MA7_Invoices',
        F.round(F.avg('DailyInvoices').over(date_window), 2)
    )

    logger.info(f'Daily MA computed: {df_daily_ma.count()} days')
    return df_daily_ma


def compute_monthly_growth(spark):
    """Compute monthly revenue with growth rate using LAG."""
    logger.info('Computing monthly growth rates...')

    df_monthly = spark.sql("""
        SELECT
            MonthStr,
            ROUND(SUM(Revenue), 2) as TotalRevenue,
            COUNT(DISTINCT InvoiceNo) as TotalInvoices,
            COUNT(DISTINCT CustomerID) as UniqueCustomers,
            LAG(ROUND(SUM(Revenue), 2))
                OVER (ORDER BY MonthStr) as PrevMonthRevenue,
            ROUND(
                (SUM(Revenue) - LAG(SUM(Revenue))
                    OVER (ORDER BY MonthStr)) /
                LAG(SUM(Revenue)) OVER (ORDER BY MonthStr) * 100,
            2) as GrowthPct
        FROM transactions
        GROUP BY MonthStr
        ORDER BY MonthStr
    """)

    logger.info(f'Monthly growth: {df_monthly.count()} months')
    return df_monthly


def write_to_delta(df_spark, path, name):
    """Write Spark DataFrame to Delta Lake."""
    logger.info(f'Writing {name} to {path}...')
    df_pd = df_spark.toPandas()
    table = pa.Table.from_pandas(df_pd)
    write_deltalake(
        path,
        table,
        storage_options=STORAGE,
        mode='overwrite'
    )
    logger.info(f'{name} written: {len(df_pd):,} rows')


def main():
    """Main entry point for the batch job."""
    logger.info('='*60)
    logger.info('RETAIL ANALYTICS BATCH JOB STARTING')
    logger.info(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    logger.info('='*60)

    start_time = datetime.now()

    # Step 1: Create Spark session
    spark = create_spark_session()

    # Step 2: Load data
    df_spark = load_data(spark)

    # Step 3: Country aggregations
    df_country = compute_country_aggregations(df_spark)
    write_to_delta(df_country, DELTA_COUNTRY_AGG, 'country_agg')

    # Step 4: Daily moving average
    df_daily_ma = compute_daily_moving_average(df_spark)
    write_to_delta(df_daily_ma, DELTA_DAILY_MA, 'daily_ma')

    # Step 5: Monthly growth
    df_monthly = compute_monthly_growth(spark)
    write_to_delta(df_monthly, DELTA_MONTHLY, 'monthly_growth')

    # Done
    duration = (datetime.now() - start_time).seconds
    logger.info('='*60)
    logger.info('BATCH JOB COMPLETE')
    logger.info(f'Duration: {duration} seconds')
    logger.info('='*60)

    spark.stop()


if __name__ == '__main__':
    main()
