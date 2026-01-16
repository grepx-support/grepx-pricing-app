"""
Apache Airflow DAG for Ticker Ingestion
Replicates the same functionality as the Dagster ticker_data asset
Integrates with Celery for actual data processing
"""
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add the project directory to the Python path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from airflow import DAG
from airflow.operators.python import PythonOperator
from celery import Celery

# Configure Celery to match your existing setup
celery_app = Celery(
    'ticker_tasks',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0',
    include=['business_tasks.tasks.data.tickers']
)

def load_tickers_task(csv_file_path, date):
    """Wrapper function to call the actual business task"""
    from business_tasks.tasks.data.tickers import load_tickers
    return load_tickers(csv_file_path, date)

# Define default arguments
default_args = {
    'owner': 'grepx',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'ticker_ingestion_pipeline',
    default_args=default_args,
    description='Load tickers from CSV file and store in database via Celery',
    schedule=timedelta(days=1),  # Daily execution
    catchup=False,
    tags=['ticker', 'ingestion', 'data', 'celery']
) as dag:

    def run_celery_ticker_task():
        """Callable that executes the Celery task"""
        result = celery_app.send_task(
            'business-tasks.data.load_tickers',
            kwargs={
                'csv_file_path': '/home/atchu/Work/grepx-pricing-app/data/tickers.csv',
                'date': datetime.now().strftime('%Y-%m-%d'),
                'collection_name': 'tickers'
            }
        )
        # Wait for the task to complete and get the result
        final_result = result.get()
        print(f"Celery task result: {final_result}")
        return final_result

    # Define the task that calls Celery
    ingest_tickers_task = PythonOperator(
        task_id='ingest_tickers_from_csv_via_celery',
        python_callable=run_celery_ticker_task,
    )

    ingest_tickers_task