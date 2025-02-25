# banking-data-etl-pipeline
# Banking Data ETL Pipeline

## Overview

This repository contains an Apache Beam-based ETL pipeline for processing banking customer data. The pipeline reads data from a CSV file, performs data validation, enrichment, and customer segmentation, and writes the processed data to local JSON files.

## Features

- **Data Validation**: Ensures data quality by validating input records.
- **Data Enrichment**: Adds derived features such as age group, wealth segment, and engagement score.
- **Customer Segmentation**: Applies RFM analysis to segment customers into different value tiers.
- **Local Output**: Writes processed data and error records to local JSON files.
- **Apache Beam**: Uses Apache Beam for scalable and portable data processing.

## Prerequisites

- Python 3.6+
- Apache Beam SDK
- Google Cloud SDK (for BigQuery integration, if needed)

## Setup

1.  Clone the repository:

    ```bash
    git clone <repository_url>
    cd banking_data_pipeline
    ```

2.  Create a virtual environment:

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

1.  Run the pipeline:

    ```bash
    python batch_processing/batch_pipeline1.py \
      --project=<your_gcp_project_id> \
      --input_path=data/train.csv \
      --output_table=<your_gcp_project_id>.banking_data_etl.processed_data \
      --error_table=<your_gcp_project_id>.banking_data_etl.error_records \
      --temp_location=gs://<your_gcp_bucket>/temp
    ```

    **Note**: Replace `<your_gcp_project_id>` and `<your_gcp_bucket>` with your Google Cloud project ID and bucket name.

2.  View the output:

    Processed data will be written to `/output/processed_data*.json` and error records to `/output/error_records*.json`.

## Configuration

-   **Input Data**: Modify the `input_path` parameter to point to your CSV data file.
-   **BigQuery Tables**: Update the `output_table` and `error_table` parameters with your BigQuery table names.
-   **Data Validation**: Adjust the `min_age` and `max_age` parameters in [batch_pipeline1.py](cci:7://file:///Users/krishnavardhan/banking_data_pipeline_gcp/batch_processing/batch_pipeline1.py:0:0-0:0) to configure age validation.

## Code Structure

-   [batch_processing/batch_pipeline1.py](cci:7://file:///Users/krishnavardhan/banking_data_pipeline_gcp/batch_processing/batch_pipeline1.py:0:0-0:0): Main script for defining and running the Apache Beam pipeline.
-   `data/train.csv`: Sample CSV data file.
-   `output/`: Directory for storing local output files.
-   `requirements.txt`: List of Python dependencies.

## Error Handling

The pipeline includes robust error handling to capture and report data quality issues. Error records are written to a separate output file for analysis.

## License

[Specify the license type, e.g., MIT License]