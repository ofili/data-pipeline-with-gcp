# Data Ingestion and Processing Pipeline

## Introduction
This project implements a data ingestion and processing pipeline to collect, store and process time-series data. The pipeline consists of a publisher, a message queue (Pub/Sub), a consumer, a data warehouse (BigQuery) and a data extractor. The pipeline is designed to be scalable, efficient and easy to maintain.

### Deployment
The solution can be deployed using Docker and Docker-compose, Ansible, Puppet, or any other tool. The pipeline components can be deployed as individual containers for easy scaling and maintenance.

## Prerequisites
Before you start using the pipeline, make sure you have the following software installed:
- Python 3.x
- The google-cloud-pubsub Python packages installed in your environment
- Docker (optional)

## Installation
1. Clone the repository
    ```bash
    git clone https://github.com/ofili/wind_turbine_data_pipeline.git
    ```
2. Navigate to the project directory
    ```bash
    cd wind_turbine_data_pipeline
    ```
3. Install the required packages
    ```bash
    pip3 install -r requirements.txt
    ```
