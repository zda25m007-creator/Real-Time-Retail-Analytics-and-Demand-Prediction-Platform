"""
retail_streaming_job.py
Production Spark Structured Streaming Job
Z5008 Big Data Lab - IIT Madras Zanzibar
Author: Vineet Joshi | ZDA25M007
"""

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKERS = 'kafka:9092'
TOPIC         = 'retail-txn-v2'
CHECKPOINT    = '/tmp/retail_streaming_checkpoint'
OUTPUT_PATH   = 's3://retail-v2/streaming/output'

STORAGE = {
    'AWS_ENDPOINT_URL':           'http://minio:9000',
    'AWS_ACCESS_KEY_ID':          'admin',
    'AWS_SECRET_ACCESS_KEY':      'bigdata123',
    'AWS_ALLOW_HTTP':             'true',
    'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
    'AWS_REGION':                 'us-east-1'
}

# Schema for incoming Kafka messages
TRANSACTION_SCHEMA = StructType([
    StructField('InvoiceNo',   StringType(),    True),
    StructField('StockCode',   StringType(),    True),
    StructField('Description', StringType(),    True),
    StructField('Quantity',    IntegerType(),   True),
    StructField('InvoiceDate', TimestampType(), True),
    StructField('UnitPrice',   DoubleType(),    True),
    StructField('CustomerID',  StringType(),    True),
    StructField('Country',     StringType(),    True),
    StructField('Revenue',     DoubleType(),    True),
])


def create_spark_session():
    """Create Spark session with Kafka support."""
    spark = SparkSession.builder \
        .appName('RetailAnalytics-StreamingJob') \
        .master('local[*]') \
        .config('spark.eventLog.enabled', 'false') \
        .config('spark.sql.shuffle.partitions', '4') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    logger.info(f'Spark Streaming session ready: {spark.version}')
    return spark


def read_kafka_stream(spark):
    """Read from Kafka topic as a streaming DataFrame."""
    logger.info(f'Connecting to Kafka: {KAFKA_BROKERS}, topic: {TOPIC}')
    df_raw = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_BROKERS) \
        .option('subscribe', TOPIC) \
        .option('startingOffsets', 'latest') \
        .option('failOnDataLoss', 'false') \
        .load()
    return df_raw


def parse_messages(df_raw):
    """Parse JSON messages from Kafka."""
    df_parsed = df_raw.select(
        F.from_json(
            F.col('value').cast('string'),
            TRANSACTION_SCHEMA
        ).alias('data')
    ).select('data.*')

    df_parsed = df_parsed.withColumn(
        'Revenue',
        F.col('Quantity') * F.col('UnitPrice')
    ).withColumn(
        'ProcessedAt',
        F.current_timestamp()
    )

    return df_parsed


def compute_streaming_aggregations(df_parsed):
    """Compute windowed aggregations on the stream."""
    df_agg = df_parsed \
        .withWatermark('InvoiceDate', '10 minutes') \
        .groupBy(
            F.window('InvoiceDate', '1 hour', '30 minutes'),
            'Country'
        ).agg(
            F.round(F.sum('Revenue'), 2).alias('HourlyRevenue'),
            F.count('InvoiceNo').alias('TransactionCount'),
            F.countDistinct('CustomerID').alias('UniqueCustomers')
        )

    return df_agg


def write_stream_to_console(df_agg):
    """Write streaming output to console (for testing)."""
    query = df_agg.writeStream \
        .outputMode('update') \
        .format('console') \
        .option('truncate', 'false') \
        .trigger(processingTime='30 seconds') \
        .start()
    return query


def write_stream_to_memory(df_parsed):
    """Write raw stream to in-memory table (for testing)."""
    query = df_parsed.writeStream \
        .outputMode('append') \
        .format('memory') \
        .queryName('retail_stream') \
        .trigger(processingTime='10 seconds') \
        .start()
    return query


def main():
    """Main entry point for streaming job."""
    logger.info('='*60)
    logger.info('RETAIL ANALYTICS STREAMING JOB STARTING')
    logger.info(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    logger.info('='*60)

    spark = create_spark_session()

    try:
        # Read from Kafka
        df_raw = read_kafka_stream(spark)

        # Parse messages
        df_parsed = parse_messages(df_raw)

        # Compute aggregations
        df_agg = compute_streaming_aggregations(df_parsed)

        # Write to console
        query = write_stream_to_console(df_agg)

        logger.info('Streaming job running. Press Ctrl+C to stop.')
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info('Streaming job stopped by user.')
    except Exception as e:
        logger.error(f'Streaming job error: {e}')
        raise
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
