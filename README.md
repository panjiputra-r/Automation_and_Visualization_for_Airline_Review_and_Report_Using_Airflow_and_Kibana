# Airline Reviews Data Pipeline

## Overview

This project automates the workflow for loading, cleaning, and uploading airline reviews data for visualization purposes. It leverages Apache Airflow for orchestration, PostgreSQL for data storage, and Elasticsearch for indexing and search functionalities.

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Apache Airflow
- PostgreSQL
- Elasticsearch

### Installation

1. **Airflow Setup:** Ensure Airflow is installed and configured as per the provided `airflow.yaml` configuration.
2. **Database Setup:** Use PostgreSQL for data storage. Credentials and configurations can be adjusted in the Airflow DAG file and `airflow.yaml`.
3. **Elasticsearch Setup:** Ensure Elasticsearch is running for indexing cleaned data. Adjust the connection details in the Airflow DAG script if necessary.

### Dataset

The dataset utilized in this project is available at [Kaggle - Airline Reviews Dataset](https://www.kaggle.com/datasets/juhibhojani/airline-reviews).

## Workflow

The project's workflow consists of the following steps:

1. **Load CSV to PostgreSQL:** A CSV file containing raw airline reviews data is loaded into a PostgreSQL database.
2. **Fetch Data:** Data is fetched from PostgreSQL for processing.
3. **Data Preprocessing:** The data undergoes cleaning and preprocessing to make it suitable for analysis and visualization.
4. **Upload to Elasticsearch:** The cleaned data is uploaded to Elasticsearch for indexing, also visualize the data.

This workflow is automated using Apache Airflow, with each step represented as a task in the DAG named `P2M3_Panji_DAG_hck`.

## Usage

To run the workflow:

1. Start your Airflow environment.
2. Navigate to the Airflow web interface.
3. Trigger the `P2M3_Panji_DAG_hck` DAG.

## Contributing

Contributions to this project are welcome. Please fork the repository and submit a pull request with your changes.
