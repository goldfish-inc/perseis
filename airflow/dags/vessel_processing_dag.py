#!/usr/bin/env python3
"""
Perseis ML Pipeline - Vessel Data Processing DAG
Maritime Intelligence Platform with Docling-Granite
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import os
import boto3
from pathlib import Path

# Default DAG arguments
default_args = {
    'owner': 'perseis-platform',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize MinIO client for data storage
def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
        region_name='us-east-1'
    )

def ingest_vessel_data(**context):
    """Ingest vessel data from watched folders"""
    import pandas as pd
    from scripts.processing.process_vessel_pdfs import main as process_pdfs
    from scripts.validation.validate_vessel_import import VesselImportValidator

    minio = get_minio_client()

    # Check for new files in raw-trade-data bucket
    response = minio.list_objects_v2(Bucket='raw-trade-data', Prefix='ingest/')

    processed_files = []

    for obj in response.get('Contents', []):
        file_key = obj['Key']
        local_path = f"/tmp/{Path(file_key).name}"

        # Download file
        minio.download_file('raw-trade-data', file_key, local_path)

        # Process based on file type
        if file_key.endswith('.pdf'):
            # Use Docling-Granite for PDF processing
            print(f"Processing PDF with Docling-Granite: {file_key}")
            # This calls the existing ebisu script
            process_pdfs()

        elif file_key.endswith('.csv'):
            # Validate CSV vessel data
            print(f"Validating CSV vessel data: {file_key}")

            # Set up database config from environment
            db_config = {
                'host': os.getenv('POSTGRES_HOST', 'postgres-postgresql'),
                'port': os.getenv('POSTGRES_PORT', '5432'),
                'database': os.getenv('POSTGRES_DB', 'tradedb'),
                'user': os.getenv('POSTGRES_USER', 'postgres'),
                'password': os.getenv('POSTGRES_PASSWORD')
            }

            # Run validation
            validator = VesselImportValidator(db_config)
            validator.connect()

            df = pd.read_csv(local_path, dtype=str)
            validated_df = validator.validate_dataframe(df, file_key)

            # Save validation report
            report_file = f"/tmp/{Path(file_key).stem}_validation_report.json"
            validator.generate_report(validated_df, report_file)

            # Upload validation results to processed bucket
            processed_key = f"processed/{Path(file_key).stem}_validated.csv"
            validated_df.to_csv(f"/tmp/{Path(file_key).stem}_validated.csv", index=False)
            minio.upload_file(f"/tmp/{Path(file_key).stem}_validated.csv", 'processed-data', processed_key)

            # Upload validation report
            report_key = f"reports/{Path(file_key).stem}_validation.json"
            minio.upload_file(report_file, 'processed-data', report_key)

        processed_files.append(file_key)

    return processed_files

def extract_trade_entities(**context):
    """Extract trade entities using Granite models"""
    import json
    from transformers import AutoTokenizer, AutoModelForCausalLM

    # Load Granite models for entity extraction
    tokenizer = AutoTokenizer.from_pretrained("ibm-granite/granite-3.1-2b-instruct")
    model = AutoModelForCausalLM.from_pretrained("ibm-granite/granite-3.1-2b-instruct")

    minio = get_minio_client()
    processed_files = context['task_instance'].xcom_pull(task_ids='ingest_vessel_data')

    entity_results = []

    for file_key in processed_files:
        if 'processed/' in file_key:
            # Download processed file
            local_path = f"/tmp/{Path(file_key).name}"
            minio.download_file('processed-data', file_key, local_path)

            # Extract trade entities (vessel_name, hs_code, flag, etc.)
            with open(local_path, 'r') as f:
                content = f.read()

            # Use Granite for entity extraction
            prompt = f"""
            Extract the following trade entities from this vessel data:
            - vessel_name
            - hs_code
            - flag_country
            - registration_number
            - risk_indicators

            Data: {content[:2000]}  # Limit context

            Return as JSON format.
            """

            inputs = tokenizer(prompt, return_tensors="pt")
            outputs = model.generate(**inputs, max_new_tokens=512)
            extracted_entities = tokenizer.decode(outputs[0], skip_special_tokens=True)

            # Save extracted entities
            entities_key = f"entities/{Path(file_key).stem}_entities.json"
            entity_file = f"/tmp/{Path(file_key).stem}_entities.json"

            with open(entity_file, 'w') as f:
                json.dump({'file': file_key, 'entities': extracted_entities}, f)

            minio.upload_file(entity_file, 'processed-data', entities_key)
            entity_results.append(entities_key)

    return entity_results

def validate_with_great_expectations(**context):
    """Run Great Expectations validation on processed data"""
    import great_expectations as ge

    processed_files = context['task_instance'].xcom_pull(task_ids='ingest_vessel_data')
    minio = get_minio_client()

    # Set up Great Expectations context
    context_ge = ge.DataContext()

    validation_results = []

    for file_key in processed_files:
        if file_key.endswith('_validated.csv'):
            local_path = f"/tmp/{Path(file_key).name}"
            minio.download_file('processed-data', file_key, local_path)

            # Load as Great Expectations dataset
            df = ge.from_pandas(ge.read_csv(local_path))

            # Define vessel data expectations
            df.expect_column_to_exist('vessel_name')
            df.expect_column_values_to_not_be_null('vessel_name')
            df.expect_column_values_to_match_regex('imo', r'^\d{7}$', mostly=0.8)
            df.expect_column_values_to_be_in_set('flag_code', ['USA', 'GBR', 'NOR', 'ESP', 'PRT'], mostly=0.9)

            # Run validation
            results = df.validate()

            # Save validation results
            validation_key = f"validation/{Path(file_key).stem}_ge_validation.json"
            validation_file = f"/tmp/{Path(file_key).stem}_ge_validation.json"

            with open(validation_file, 'w') as f:
                json.dump(results.to_json_dict(), f)

            minio.upload_file(validation_file, 'processed-data', validation_key)
            validation_results.append(validation_key)

    return validation_results

# Create the DAG
dag = DAG(
    'perseis_vessel_processing',
    default_args=default_args,
    description='Maritime Intelligence Platform - Vessel Data Processing',
    schedule_interval='@hourly',  # Run every hour to check for new data
    catchup=False,
    tags=['perseis', 'maritime', 'intelligence', 'docling-granite']
)

# Task 1: Ingest vessel data (PDFs, CSVs) with Docling-Granite
ingest_task = PythonOperator(
    task_id='ingest_vessel_data',
    python_callable=ingest_vessel_data,
    dag=dag
)

# Task 2: Extract trade entities using Granite models
extract_entities_task = PythonOperator(
    task_id='extract_trade_entities',
    python_callable=extract_trade_entities,
    dag=dag
)

# Task 3: Validate data quality with Great Expectations
validate_task = PythonOperator(
    task_id='validate_with_great_expectations',
    python_callable=validate_with_great_expectations,
    dag=dag
)

# Task 4: Store processed data in PostgreSQL with pgvector
store_task = PostgresOperator(
    task_id='store_in_postgres',
    postgres_conn_id='postgres_default',
    sql="""
    -- Insert validated vessel data
    INSERT INTO trade_transactions (
        trade_id,
        vessel_name,
        hs_code,
        commodity,
        risk_score,
        embedding
    )
    SELECT
        gen_random_uuid()::text,
        vessel_name,
        hs_code,
        COALESCE(commodity_json, '{}'::jsonb),
        COALESCE(calculated_risk_score, 0.0),
        -- Generate embeddings using PostgresML
        pgml.embed('sentence-transformers/all-MiniLM-L6-v2', vessel_name || ' ' || COALESCE(hs_code, ''))
    FROM vessel_staging_validated
    WHERE validation_status IN ('VALID', 'WARNING')
    AND processed_at IS NULL;

    -- Mark as processed
    UPDATE vessel_staging_validated
    SET processed_at = NOW()
    WHERE validation_status IN ('VALID', 'WARNING')
    AND processed_at IS NULL;
    """,
    dag=dag
)

# Task 5: Generate intelligence reports
generate_reports_task = BashOperator(
    task_id='generate_intelligence_reports',
    bash_command="""
    # Generate vessel intelligence dashboard
    cd /app && python scripts/reporting/generate_vessel_intelligence.py

    # Update vessel trust scores
    cd /app && python scripts/processing/update_trust_scores.py
    """,
    dag=dag
)

# Set task dependencies
ingest_task >> extract_entities_task >> validate_task >> store_task >> generate_reports_task