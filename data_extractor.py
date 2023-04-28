from __future__ import annotations

import logging
import os
from datetime import datetime

import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import mean, stddev, max, udf
from pyspark.sql.types import ArrayType, DoubleType

# Configure logging
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

# Load configuration from env variables
project_id = os.getenv('PROJECT_ID')
topic = os.getenv('TOPIC')
subscription = os.getenv('SUBSCRIPTION')
bucket_name = os.getenv('BUCKET_NAME')
folder_path = 'summarized'

# Set up binning options
batch_size = int(os.environ.get('BATCH_SIZE', '100'))
num_bins = int(os.environ.get('NUM_BINS', '10'))
lower_bound = float(os.environ.get('LOWER_BOUND', '0'))
upper_bound = float(os.environ.get('UPPER_BOUND', '50'))


def get_session() -> SparkSession:
    """Create a spark session for the Spark application.

    Returns:
        SparkSession.
    """
    # Configure Spark
    spark = SparkSession.builder.appName("Read from BigQuery Stream").getOrCreate()

    # Configure GCS credentials
    spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    spark.conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/path/to/keyfile.json")

    # Set GCS as the default file system
    spark.conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.conf.set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    # Get app id
    app_id = spark.conf.get("spark.app.id")

    logging.info(f"SparkSession started successfully for app: {app_id}")

    return spark


def histogram(column, num_bins, lower_bound, upper_bound):
    """
    Calculate histogram of a column using numpy library.
    
    Parameters:
        column (str): column to calculate histogram on
        num_bins (int): number of bins to divide the range into
        lower_bound (float): minimum value of the range
        upper_bound (float): maximum value of the range

    Returns:
        pyspark.sql.Column: Column containing the histogram values
    """

    def histogram_func(values):
        hist, edges = np.histogram(values, bins=num_bins, range=(lower_bound, upper_bound))
        return list(hist.astype(np.double))

    return udf(histogram_func, ArrayType(DoubleType()))(column)


def process(streaming_data: DataFrame) -> DataFrame:
    """
    Process a streaming DataFrame by calculating statistical summaries and uploading to MinIO
    
    Parameters:
        streaming_data (pyspark.sql.DataFrame): streaming DataFrame to process

    Returns:
        stats (pyspark.sql.DataFrame)
    """
    for column in streaming_data.columns:
        for i, row in enumerate(streaming_data, start=1):
            if i % batch_size == 0:
                stats: DataFrame = streaming_data.select(
                    mean(column),
                    stddev(column),
                    max(column),
                    histogram(column, num_bins, lower_bound, upper_bound)
                )
                return stats


def write_df_to_gcs(df: DataFrame):
    file_name = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    gcs_path = "gs://{bucket_name}/{folder_path}/{file_name}.json".format(
        bucket_name=bucket_name, folder_path=folder_path, file_name=file_name
    )
    df.write.mode("overwrite").format("json").save(gcs_path)


if __name__ == '__main__':
    spark = get_session()

    # Read data stream
    streaming_df: DataFrame = spark \
        .readStream \
        .format("pubsub") \
        .option("projectId", project_id) \
        .option("subscription", subscription) \
        .load()

    # Process data
    stats_df = process(streaming_df)

    # Write processed data to GCS
    stats_df.writeStream \
        .foreachBatch(lambda df: write_df_to_gcs(df)) \
        .start() \
        .awaitTermination()
