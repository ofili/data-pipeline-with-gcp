from __future__ import annotations
from csv import DictReader
import csv
import datetime

import logging
import os
import re
import tempfile
from typing import Any

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import Client, SchemaField, LoadJobConfig, DatasetReference, Dataset
from pyspark import StorageLevel

# Configure logging
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

# Load configuration from env variables
PROJECT_ID = os.getenv('PROJECT_ID')
DATASET_NAME = os.getenv('DATASET_NAME')
TABLE_NAME = os.getenv('TABLE_NAME')


# Define the table schema
SCHEMA: list[SchemaField] = [
    bigquery.SchemaField('timestamp', 'TIMESTAMP', mode='REQUIRED'),
    bigquery.SchemaField('year', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('month', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('generator___mechanical___speed', 'NUMERIC', mode='NULLABLE'),
    bigquery.SchemaField('generator___temperature', 'NUMERIC', mode='NULLABLE'),
    bigquery.SchemaField('turbine_data___electrical___power_production', 'NUMERIC', mode='NULLABLE'),
    bigquery.SchemaField('met_sensors___mechanical___wind_speed', 'NUMERIC', mode='NULLABLE'),
    bigquery.SchemaField('met_sensors___temperature___ambient', 'NUMERIC', mode='NULLABLE'),
    bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH,
        field="timestamp"
    ),
]

bq_client = bigquery.Client()


def parse_csv(csv_file):
    # Parse CSV file and add year and month columns
    data = []
    reader = csv.DictReader(csv_file)
    for row in reader:
        # Extract year and month from timestamp
        timestamp = row['timestamp']
        match = re.match(r'(\d{4})-(\d{2})-\d{2}T\d{2}:\d{2}\.\d+Z', timestamp)
        if not match:
            raise ValueError(f"Invalid timestamp format: {timestamp}")
        year = int(match.group(1))
        month = int(match.group(2))

        # Add year and month columns to row
        row['year'] = year
        row['month'] = month
        data.append(row)
    
    return data


def write_to_bigquery(table_id, rows):
    # Write processed data to BigQuery table
    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        schema=SCHEMA,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="timestamp",
        ),
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = bq_client.load_table_from_json(
        rows,
        table_id,
        job_config=job_config
    )
    job.result()


def process_file(event, context):
    if event is None:
        raise ValueError("Event is required")
    if context is None:
        raise ValueError("Context is required")
    
    # extract bucket and file name from event
    bucket_name = event['bucket']
    file_name = event['name']

    # Check if file is a CSV file
    if not file_name.endswith('.csv'):
        logging.info(f"Ignoring non-CSV file: {file_name}")
        return
    
    # Download file from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    _, temp_local_filename = tempfile.mkstemp()
    blob.download_to_filename(temp_local_filename)

    # Parse CSV file and add year and month columns
    with open(temp_local_filename, mode='r') as csv_file:
        data = parse_csv(csv_file)

    # Write processed data to BigQuery
    table_id = f"{bq_client.project}.{DATASET_NAME}.{TABLE_NAME}"
    write_to_bigquery(table_id, data)

    # Delete temporary file
    os.remove(temp_local_filename)

    logging.info(f"Processed file {file_name} uploaded to BigQuery")

    # Save processed file to new GCS bucket
    new_bucket_name = 'new-bucket-name'
    new_blob_name = f"{os.path.splitext(file_name)[0]}-processed.csv"
    new_blob = bucket.bucket(new_bucket_name).blob(new_blob_name)
    new_blob.upload_from_string('\n'.join([','.join(map(str, row.values())) for row in data]))
    logging.info(f"Processed file {file_name} uploaded to {new_bucket_name}/{new_blob_name}")


def gcs_csv_trigger(event, context):
    for file in event:
        process_file(file, context)
