import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
import logging
import csv
import json
import uuid
import traceback
import datetime
from typing import Dict, Any
from google.cloud import bigquery
import google.api_core.exceptions
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO)

# Constants for better readability and maintainability
NUM_CSV_COLUMNS = 17
MIN_VALID_AGE = 18
MAX_VALID_AGE = 100
HIGH_NET_WORTH_BALANCE_THRESHOLD = 50000
MASS_AFFLUENT_BALANCE_THRESHOLD = 10000
RECENCY_SCORES = {
    -1: 1,  # Never contacted before
    7: 5,
    30: 4,
    90: 3,
    180: 2,
    float('inf'): 1
}
FREQUENCY_SCORES = {
    3: 2,
    5: 3,
    7: 4,
    10: 5,
    float('inf'): 1
}
MONETARY_SCORES = {
    5000: 2,
    10000: 3,
    25000: 4,
    50000: 5,
    float('inf'): 1
}

# Define schemas for BigQuery tables
PROCESSED_DATA_SCHEMA = [
    bigquery.SchemaField('age', 'INTEGER'),
    bigquery.SchemaField('job', 'STRING'),
    bigquery.SchemaField('marital', 'STRING'),
    bigquery.SchemaField('education', 'STRING'),
    bigquery.SchemaField('default', 'STRING'),
    bigquery.SchemaField('balance', 'FLOAT'),
    bigquery.SchemaField('housing', 'STRING'),
    bigquery.SchemaField('loan', 'STRING'),
    bigquery.SchemaField('contact', 'STRING'),
    bigquery.SchemaField('day', 'INTEGER'),
    bigquery.SchemaField('month', 'STRING'),
    bigquery.SchemaField('duration', 'INTEGER'),
    bigquery.SchemaField('campaign', 'INTEGER'),
    bigquery.SchemaField('pdays', 'INTEGER'),
    bigquery.SchemaField('previous', 'INTEGER'),
    bigquery.SchemaField('poutcome', 'STRING'),
    bigquery.SchemaField('y', 'STRING'),
    bigquery.SchemaField('age_group', 'STRING'),
    bigquery.SchemaField('wealth_segment', 'STRING'),
    bigquery.SchemaField('contact_day_type', 'STRING'),
    bigquery.SchemaField('has_loans', 'BOOLEAN'),
    bigquery.SchemaField('customer_segment', 'STRING'),
    bigquery.SchemaField('rfm_scores', 'STRING'),
    bigquery.SchemaField('engagement_score', 'FLOAT'),
    bigquery.SchemaField('processing_timestamp', 'TIMESTAMP'),
    bigquery.SchemaField('_ingestion_timestamp', 'TIMESTAMP', mode='REQUIRED'),
    bigquery.SchemaField('_processing_timestamp', 'TIMESTAMP', mode='REQUIRED'),
    bigquery.SchemaField('_batch_id', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('_pipeline_version', 'STRING', mode='REQUIRED')
]

ERROR_RECORDS_SCHEMA = [
    bigquery.SchemaField('raw_data', 'STRING'),
    bigquery.SchemaField('error_message', 'STRING'),
    bigquery.SchemaField('error_type', 'STRING'),
    bigquery.SchemaField('timestamp', 'TIMESTAMP')
]

class BankingBatchOptions(PipelineOptions):
    """Pipeline options for the banking ETL batch job."""
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_path', required=True, help='Path to input CSV file')
        parser.add_argument('--output_table', required=True, help='BigQuery table for output')
        parser.add_argument('--error_table', required=True, help='BigQuery table for errors')
        parser.add_argument('--stats_table', required=False, help='BigQuery table for statistics')
        parser.add_argument('--min_age', type=int, default=MIN_VALID_AGE, help='Minimum valid age')
        parser.add_argument('--max_age', type=int, default=MAX_VALID_AGE, help='Maximum valid age')

    def create_error_record(self, raw_data: Any, error_message: str, error_type: str) -> Dict:
        """Creates a standardized error record."""
        if not isinstance(raw_data, dict):
            raw_data = {"raw_data_value": raw_data}  # Wrap non-dict data in a dict
        return {
            'raw_data': json.dumps(raw_data),
            'error_message': str(error_message),
            'error_type': error_type,
            'timestamp': datetime.datetime.utcnow().isoformat()
        }

class ParseCSVFn(beam.DoFn):
    """Parses a CSV file and validates the data."""
    def __init__(self, banking_options):
        self.banking_options = banking_options
        
    def process(self, element: str, timestamp=beam.DoFn.TimestampParam):
        try:
            # Use semicolon as delimiter and handle quotes
            row = list(csv.reader([element], delimiter=';', quotechar='"'))[0]
            if len(row) != NUM_CSV_COLUMNS:  # Validate column count
                raise ValueError(f"Expected {NUM_CSV_COLUMNS} columns, got {len(row)}")

            record = {
                'age': int(row[0]),
                'job': row[1].lower().strip(),
                'marital': row[2].lower().strip(),
                'education': row[3].lower().strip(),
                'default': row[4].lower().strip(),
                'balance': float(row[5]),
                'housing': row[6].lower().strip(),
                'loan': row[7].lower().strip(),
                'contact': row[8].lower().strip(),
                'day': int(row[9]),
                'month': row[10].lower().strip(),
                'duration': int(row[11]),
                'campaign': int(row[12]),
                'pdays': int(row[13]),
                'previous': int(row[14]),
                'poutcome': row[15].lower().strip(),
                'y': row[16].lower().strip(),
                '_ingestion_timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat()
            }

            # Validate required fields
            for field in ['age', 'job', 'balance']:
                if record[field] is None:
                    raise ValueError(f"Required field {field} is missing")

            yield record
        except Exception as e:
            logging.error(f"Error parsing record: {str(e)}")
            error_record = {
                'raw_data': element,
                'error_message': f"ParseError: {str(e)}",
                'error_type': 'parsing_error',
                'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            yield beam.pvalue.TaggedOutput('error_records', error_record)

class ValidateAndEnrichFn(beam.DoFn):
    """Validates and enriches the data."""
    def __init__(self, min_age: int, max_age: int, banking_options=None):
        self.min_age = min_age
        self.max_age = max_age
        self.banking_options = banking_options

    def process(self, element: Dict, timestamp=beam.DoFn.TimestampParam):
        try:
            if not isinstance(element['age'], int):
                raise ValueError("Age must be integer")

            # Validation
            if not (self.min_age <= element['age'] <= self.max_age):
                raise ValueError(f"Age {element['age']} outside valid range")

            # Feature Engineering
            # Age Group
            element['age_group'] = (
                'young' if element['age'] < 30
                else 'middle_aged' if element['age'] < 50
                else 'senior'
            )

            # Wealth Segmentation (Advanced)
            balance = element['balance']
            element['wealth_segment'] = (
                'high_net_worth' if balance > HIGH_NET_WORTH_BALANCE_THRESHOLD
                else 'mass_affluent' if balance > MASS_AFFLUENT_BALANCE_THRESHOLD
                else 'mass_market'
            )

            # Contact Day Type
            element['contact_day_type'] = 'weekend' if element['day'] % 7 in [0, 6] else 'weekday'

            # Loan Status Aggregation
            element['has_loans'] = element['housing'] == 'yes' or element['loan'] == 'yes'

            # Engagement Score (Complex Feature)
            engagement_factors = {
                'previous_contacts': min(element['previous'], 10) / 10,
                'campaign_intensity': min(element['campaign'], 10) / 10,
                'duration_factor': min(element['duration'], 1000) / 1000,
                'response': 1.0 if element['y'] == 'yes' else 0.0
            }
            element['engagement_score'] = sum(engagement_factors.values()) / len(engagement_factors)

            yield element

        except Exception as e:
            error_record = {
                'raw_data': str(element),
                'error_message': f"ValidationError: {str(e)}",
                'error_type': 'data_validation',
                'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            logging.error(f"Validation failed: {error_record}")
            yield beam.pvalue.TaggedOutput('error_records', error_record)

class CustomerSegmentationFn(beam.DoFn):
    """Segments customers based on their data."""
    def __init__(self, banking_options=None):
        self.banking_options = banking_options
        
    def process(self, element: Dict, timestamp=beam.DoFn.TimestampParam):
        try:
            # Add processing timestamp
            element['_processing_timestamp'] = datetime.datetime.now(datetime.timezone.utc).isoformat()

            # Calculate RFM scores
            recency_score = self._calculate_score(element.get('pdays', -1), RECENCY_SCORES)
            frequency_score = self._calculate_score(element.get('previous', 0), FREQUENCY_SCORES)
            monetary_score = self._calculate_score(element.get('balance', 0), MONETARY_SCORES)

            # Store individual scores
            element['rfm_scores'] = json.dumps({
                'recency': recency_score,
                'frequency': frequency_score,
                'monetary': monetary_score
            })

            # Calculate segment based on average score
            avg_score = (recency_score + frequency_score + monetary_score) / 3
            element['customer_segment'] = self._determine_segment(avg_score)

            yield element

        except Exception as e:
            logging.error(f"Error in customer segmentation: {str(e)}")
            error_record = {
                'raw_data': str(element),
                'error_message': f"SegmentationError: {str(e)}",
                'error_type': 'segmentation_error',
                'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            yield beam.pvalue.TaggedOutput('error_records', error_record)
    
    def _calculate_score(self, value, score_map):
        for threshold, score in sorted(score_map.items()):
            if value <= threshold:
                return score
        return score_map[float('inf')]

    def _determine_segment(self, avg_score: float) -> str:
        if avg_score >= 4:
            return 'premium'
        elif avg_score >= 3:
            return 'high_value'
        elif avg_score >= 2:
            return 'medium_value'
        else:
            return 'low_value'

class PrepareForBigQueryFn(beam.DoFn):
    """Prepares the data for writing to BigQuery."""
    def __init__(self, banking_options=None):
        self.banking_options = banking_options
        
    def process(self, element: Dict):
        try:
            element.update({
                'processing_timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                '_batch_id': str(uuid.uuid4()),
                '_pipeline_version': '1.2'
            })
            yield element
        except Exception as e:
            error_record = {
                'raw_data': str(element),
                'error_message': f"PreparationError: {str(e)}",
                'error_type': 'preparation_error',
                'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            yield beam.pvalue.TaggedOutput('error_records', error_record)

def create_bigquery_table(project_id, table_name, schema):
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{table_name}"
    
    # Check if table exists first
    try:
        client.get_table(table_id)
        logging.info(f"Table {table_id} already exists")
        return
    except google.api_core.exceptions.NotFound:
        logging.info(f"Table {table_id} not found, creating...")
    
    # Convert schema to dictionary format
    schema_dict = [field.to_api_repr() for field in schema]
    
    table = bigquery.Table(table_id, schema=schema_dict)
    try:
        client.create_table(table)
        logging.info(f"Table {table_id} created successfully")
    except google.api_core.exceptions.Conflict:
        logging.info(f"Table {table_id} already exists")
    except Exception as e:
        logging.warning(f"Error creating table {table_id}: {str(e)}")
        logging.info("Continuing with pipeline execution...")

def run_pipeline(argv=None):
    """Main entry point; defines and runs the pipeline."""
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set up pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    
    # Get banking-specific options
    banking_options = pipeline_options.view_as(BankingBatchOptions)

    # Skip table creation as tables already exist
    logging.info("Skipping table creation as tables already exist")

    # Use local file instead of GCS file if needed
    input_path = banking_options.input_path
    if input_path.startswith('gs://'):
        logging.info(f"Using local file instead of GCS file: {input_path}")
        input_path = "/Users/krishnavardhan/banking_data_pipeline_gcp/train.csv"

    with beam.Pipeline(options=pipeline_options) as p:
        logging.info("Starting pipeline execution")

        # Read input data
        raw_data = p | 'ReadCSV' >> beam.io.ReadFromText(input_path, skip_header_lines=1)

        # Parse and validate data
        parsed_data = raw_data | 'ParseCSV' >> beam.ParDo(ParseCSVFn(banking_options)).with_outputs('error_records', main='records')

        validated_data = (parsed_data.records
            | 'ValidateAndEnrich' >> beam.ParDo(ValidateAndEnrichFn(
                banking_options.min_age,
                banking_options.max_age,
                banking_options
            )).with_outputs('error_records', main='valid_records'))

        # Segment customers
        segmented_data = (validated_data.valid_records
            | 'Segmentation' >> beam.ParDo(CustomerSegmentationFn(banking_options)).with_outputs('error_records', main='segmented_records'))

        # Prepare for BigQuery
        prepared_data = (segmented_data.segmented_records
            | 'PrepareBigQuery' >> beam.ParDo(PrepareForBigQueryFn(banking_options)).with_outputs('error_records', main='prepared_records'))

        output_data = prepared_data.prepared_records

        # Combine all errors
        all_errors = ((parsed_data.error_records,
                      validated_data.error_records,
                      segmented_data.error_records,
                      prepared_data.error_records)
            | 'FlattenErrors' >> beam.Flatten())

        # Write to local files instead of BigQuery due to billing issues
        output_data | 'WriteProcessedToJSON' >> beam.io.WriteToText(
            '/Users/krishnavardhan/banking_data_pipeline_gcp/output/processed_data',
            file_name_suffix='.json'
        )

        all_errors | 'WriteErrorsToJSON' >> beam.io.WriteToText(
            '/Users/krishnavardhan/banking_data_pipeline_gcp/output/error_records',
            file_name_suffix='.json'
        )

        logging.info("Pipeline definition complete")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
