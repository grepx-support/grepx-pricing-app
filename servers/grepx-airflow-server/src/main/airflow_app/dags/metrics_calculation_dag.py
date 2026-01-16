"""
Apache Airflow DAG for Metrics Calculation
Replicates the Dagster metrics calculation workflow
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
    'metrics_tasks',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0',
    include=[
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
    'metrics_calculation_pipeline',
    default_args=default_args,
    description='Calculate fundamental financial metrics via Celery',
    schedule=timedelta(days=1),  # Daily execution
    catchup=False,
    tags=['metrics', 'calculation', 'fundamentals', 'celery']
) as dag:

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

    def run_celery_fcf_task():
        """Callable that executes the Celery Free Cash Flow calculation task"""
        extracted_data = run_celery_extract_task()
        result = celery_app.send_task(
            'business-tasks.fundamentals.calculate_free_cash_flow',
            kwargs={
                'year': 2023,
                'quarter': 4,
                'extracted_fields': extracted_data
            }
        )
        # Wait for the task to complete and get the result
        final_result = result.get()
        print(f"Free Cash Flow Celery task result: {final_result}")
        return final_result

    def run_celery_dte_task():
        """Callable that executes the Celery Debt-to-Equity calculation task"""
        extracted_data = run_celery_extract_task()
        result = celery_app.send_task(
            'business-tasks.fundamentals.calculate_debt_to_equity',
            kwargs={
                'year': 2023,
                'quarter': 4,
                'extracted_fields': extracted_data
            }
        )
        # Wait for the task to complete and get the result
        final_result = result.get()
        print(f"Debt-to-Equity Celery task result: {final_result}")
        return final_result

    def run_celery_cr_task():
        """Callable that executes the Celery Current Ratio calculation task"""
        extracted_data = run_celery_extract_task()
        result = celery_app.send_task(
            'business-tasks.fundamentals.calculate_current_ratio',
            kwargs={
                'year': 2023,
                'quarter': 4,
                'extracted_fields': extracted_data
            }
        )
        # Wait for the task to complete and get the result
        final_result = result.get()
        print(f"Current Ratio Celery task result: {final_result}")
        return final_result

    def run_celery_npm_task():
        """Callable that executes the Celery Net Profit Margin calculation task"""
        extracted_data = run_celery_extract_task()
        result = celery_app.send_task(
            'business-tasks.fundamentals.calculate_net_profit_margin',
            kwargs={
                'year': 2023,
                'quarter': 4,
                'extracted_fields': extracted_data
            }
        )
        # Wait for the task to complete and get the result
        final_result = result.get()
        print(f"Net Profit Margin Celery task result: {final_result}")
        return final_result

    def run_celery_roe_task():
        """Callable that executes the Celery Return on Equity calculation task"""
        extracted_data = run_celery_extract_task()
        result = celery_app.send_task(
            'business-tasks.fundamentals.calculate_return_on_equity',
            kwargs={
                'year': 2023,
                'quarter': 4,
                'extracted_fields': extracted_data
            }
        )
        # Wait for the task to complete and get the result
        final_result = result.get()
        print(f"Return on Equity Celery task result: {final_result}")
        return final_result

    # Define the tasks that call Celery
    fcf_task = PythonOperator(
        task_id='calculate_free_cash_flow_via_celery',
        python_callable=run_celery_fcf_task,
    )

    dte_task = PythonOperator(
        task_id='calculate_debt_to_equity_via_celery',
        python_callable=run_celery_dte_task,
    )

    cr_task = PythonOperator(
        task_id='calculate_current_ratio_via_celery',
        python_callable=run_celery_cr_task,
    )

    npm_task = PythonOperator(
        task_id='calculate_net_profit_margin_via_celery',
        python_callable=run_celery_npm_task,
    )

    roe_task = PythonOperator(
        task_id='calculate_return_on_equity_via_celery',
        python_callable=run_celery_roe_task,
    )

    # Set dependencies - all metrics depend on having the financial data ready
    fcf_task
    dte_task
    cr_task
    npm_task
    roe_task