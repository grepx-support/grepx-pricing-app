"""
Comprehensive Apache Airflow DAG for Ticker Processing Pipeline
Combines ticker ingestion, financial data processing, and metrics calculation
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
    'ticker_processing_pipeline',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0',
    include=[
        'business_tasks.tasks.data.tickers',
        'business_tasks.tasks.fundamentals.download',
        'business_tasks.tasks.fundamentals.metrics'
    ]
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
    'ticker_processing_pipeline',
    default_args=default_args,
    description='Complete pipeline: ticker ingestion -> financial data -> metrics calculation',
    schedule=timedelta(days=1),  # Daily execution
    catchup=False,
    tags=['ticker', 'financial', 'metrics', 'pipeline', 'celery']
) as dag:

    def run_celery_ticker_task():
        """Callable that executes the Celery ticker ingestion task"""
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
        print(f"Ticker ingestion Celery task result: {final_result}")
        return final_result

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
        print(f"Financial data Celery task result: {final_result}")
        return final_result

    def run_celery_extract_task():
        """Callable that executes the Celery data extraction task"""
        result = celery_app.send_task(
            'business-tasks.fundamentals.extract_financial_fields',
            kwargs={
                'year': 2023,
                'quarter': 4
            }
        )
        # Wait for the task to complete and get the result
        final_result = result.get()
        print(f"Data extraction Celery task result: {final_result}")
        return final_result

    def run_celery_metrics_task():
        """Callable that executes the Celery metrics calculation tasks"""
        # First extract the financial fields
        extracted_data = run_celery_extract_task()
        
        # Execute multiple metrics calculations with the extracted data
        tasks = [
            ('business-tasks.fundamentals.calculate_free_cash_flow', {'year': 2023, 'quarter': 4, 'extracted_fields': extracted_data}),
            ('business-tasks.fundamentals.calculate_debt_to_equity', {'year': 2023, 'quarter': 4, 'extracted_fields': extracted_data}),
            ('business-tasks.fundamentals.calculate_current_ratio', {'year': 2023, 'quarter': 4, 'extracted_fields': extracted_data}),
            ('business-tasks.fundamentals.calculate_net_profit_margin', {'year': 2023, 'quarter': 4, 'extracted_fields': extracted_data}),
            ('business-tasks.fundamentals.calculate_return_on_equity', {'year': 2023, 'quarter': 4, 'extracted_fields': extracted_data})
        ]
        
        results = {}
        for task_name, kwargs in tasks:
            result = celery_app.send_task(task_name, kwargs=kwargs)
            task_result = result.get()
            results[task_name] = task_result
            print(f"{task_name} Celery task result: {task_result}")
        
        return results

    # Define the tasks that call Celery
    ticker_ingestion_task = PythonOperator(
        task_id='ticker_ingestion_via_celery',
        python_callable=run_celery_ticker_task,
    )

    financial_data_task = PythonOperator(
        task_id='financial_data_processing_via_celery',
        python_callable=run_celery_financial_task,
    )

    metrics_calculation_task = PythonOperator(
        task_id='metrics_calculation_via_celery',
        python_callable=run_celery_metrics_task,
    )

    # Set dependencies: ticker -> financial data -> metrics
    ticker_ingestion_task >> financial_data_task >> metrics_calculation_task