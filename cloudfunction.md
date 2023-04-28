# BigQuery Update to Pub/Sub Cloud Function

This is a Cloud Function that listens for changes to a BigQuery table and publishes messages to a Pub/Sub topic for each updated row.

## Prerequisites
Before deploying the Cloud Function, you need to have the following:
- A Google Cloud Platform (GCP) project with the required APIs enabled.
- The required permission to access the BigQuery table and Pub/Sub topic.
- The gcloud CLI tool installed and configured to use your GCP project.
- The google-cloud-pubsub Python packages installed in your environment.

## Deployment
To deploy the Cloud Function, follow these steps:
1. Create a new Cloud Function in your GCP project.
2. Choose a name, region, and trigger type for the Cloud Function. For example, you can trigger the Cloud Function whenever a new row is inserted or updated in a BigQuery table. In this case, when a new row is inserted.
3. Specify the Cloud Function's entry point as on_bigquery_update in the cloud_function.py.
4. Add any required environment variables, such as the Pub/Sub topic name and BigQuery table name.
5. Deploy the Cloud Function.

After the Cloud Function is deployed, it will listen for changes to the specified BigQuery table and publish messages to the specified Pub/Sub topic for each inserted row.

## Configuration
The following environment variables are required to configure the Cloud Function:
- PROJECT_ID: The ID of your GCP project.
- TOPIC: The name of the Pub/Sub topic to publish messages to.
- TABLE: The name of the BigQuery table to listen for changes to.

## Testing
To test the Cloud Function, you can insert a row in the specified table. The Cloud Function should publish a message to the specified Pub/Sub topic for each row.
