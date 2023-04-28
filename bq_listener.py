from __future__ import annotations

import logging
import os

from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.cloud.bigquery import Client, Table
from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator
from google.cloud.pubsub_v1 import PublisherClient
from google.pubsub_v1.types import pubsub

# Configure logging
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)


# Load configuration from env variables
PROJECT_ID = os.getenv('PROJECT_ID')
TOPIC = os.getenv('TOPIC')
TABLE = os.getenv('TABLE')
BUFFER_SIZE = 1000


def on_bigquery_update(event=None, context=None):
    """
    Cloud Function that listens for changes to a BigQuery table and publishes messages to the Pub/Sub topic whenever .
    It uses retries to handle failures and batch sizes to optimize performance.
    """
    if event is None:
        raise ValueError("Event is required")
    if context is None:
        raise ValueError("Context is required")
    
    client: Client = bigquery.Client()
    pubsub_client: PublisherClient = pubsub_v1.PublisherClient()

    # Get the BigQuery table that was updated
    table_id = event['resource']['name']
    table: Table = client.get_table(table_id)

    # Read the new rows from the table
    query = f"SELECT * FROM {table} WHERE _PARTITIONTIME >= TIMESTAMP('{event['eventTime']})"
    rows: RowIterator | _EmptyRowIterator = client.query(query).result()

    # Publish the rows in batches
    topic_path = pubsub_client.topic_path(PROJECT_ID, TOPIC)
    batch_size = BUFFER_SIZE
    buffer = []
    for row in rows:
        message_data: str = ','.join(str(field.value) for field in row)
        message_bytes: bytes = message_data.encode('utf-8')
        message = pubsub.PubsubMessage(message_bytes)

        buffer.append(message)
        if len(buffer) == batch_size:
            publish_batch(pubsub_client, topic_path, buffer)
            buffer.clear()

    # Publish any remaining rows
    if buffer:
        publish_batch(pubsub_client, topic_path, buffer)


def publish_batch(pubsub_client, topic_path, messages):
    # Publish a batch of messages with retries in case of failure
    retries = 3
    while retries > 0:
        try:
            response = pubsub_client.publish(topic_path, messages=messages)
            message_ids = [msg.message_id for msg in response.message_ids]
            logging.info(f"Published {len(message_ids)} messages")
            break
        except Exception as e:
            retries -= 1
            if retries == 0:
                raise e
