"""
Apache Airflow DAG for Financial Data Processing
Replicates the Dagster financial_data asset workflow
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
    'financial_tasks',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0',
    include=['business_tasks.tasks.fundamentals.download']
)

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
    'financial_data_pipeline',
    default_args=default_args,
    description='Download raw financial statements for all tickers via Celery',
    schedule=timedelta(days=1),  # Daily execution
    catchup=False,
    tags=['financial', 'data', 'download', 'celery']
) as dag:

    def run_celery_financial_task():
        """Callable that executes the Celery financial download task"""
        result = celery_app.send_task(
            'business-tasks.fundamentals.download_financial_data',
            kwargs={
                'year': 2023,
                'quarter': 4,
                'data_types': ["balance_sheet", "income_statement", "cash_flow"]
            }
        )
        # Wait for the task to complete and get the result
        final_result = result.get()
        print(f"Celery financial task result: {final_result}")
        return final_result

    # Define the task that calls Celery
    download_task = PythonOperator(
        task_id='download_financial_statements_via_celery',
        python_callable=run_celery_financial_task,
    )

    download_task