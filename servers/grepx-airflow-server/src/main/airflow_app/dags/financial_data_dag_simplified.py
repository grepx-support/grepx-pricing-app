"""
Apache Airflow DAG for Financial Data Processing - Simplified Version
This version runs business tasks directly instead of trying to bridge with Celery
"""

from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add the project directory to the Python path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "business-tasks"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

def check_database_connection():
    """Check if database services are available"""
    import requests
    
    # Check database server
    try:
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code == 200:
            print("✓ Database server is running")
            return True
        else:
            print("✗ Database server health check failed")
            return False
    except Exception as e:
        print(f"✗ Cannot connect to database server: {e}")
        return False

def load_tickers_if_needed():
    """Load tickers from CSV to database if not already present"""
    import asyncio
    import aiohttp
    from business_tasks.tasks.fundamentals.helpers import fetch_tickers_from_db
    
    # Check if tickers exist
    try:
        tickers = asyncio.run(fetch_tickers_from_db())
        if tickers:
            print(f"✓ Found {len(tickers)} tickers in database")
            return True
    except Exception as e:
        print(f"✗ Error checking tickers: {e}")
    
    print("No tickers found, attempting to load from CSV...")
    
    # Try to load tickers from CSV
    try:
        csv_path = project_root / "data" / "tickers.csv"
        if not csv_path.exists():
            print(f"✗ CSV file not found: {csv_path}")
            return False
            
        # Read and load tickers
        import csv
        tickers_to_load = []
        with open(csv_path, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header
            for row in reader:
                if row and row[0].strip():
                    tickers_to_load.append(row[0].strip())
        
        if not tickers_to_load:
            print("✗ No tickers found in CSV")
            return False
            
        # Load to database via API
        api_url = "http://localhost:8000"
        storage_name = "tickers"
        
        async def load_tickers():
            async with aiohttp.ClientSession() as session:
                for ticker in tickers_to_load[:5]:  # Load first 5 for testing
                    try:
                        payload = {
                            "storage_name": storage_name,
                            "model_class_name": "tickers",
                            "data": {"tickers": ticker}
                        }
                        async with session.post(f"{api_url}/write", json=payload) as response:
                            if response.status == 200:
                                print(f"✓ Loaded ticker: {ticker}")
                    except Exception as e:
                        print(f"✗ Failed to load {ticker}: {e}")
        
        asyncio.run(load_tickers())
        print(f"✓ Attempted to load {min(5, len(tickers_to_load))} tickers")
        return True
        
    except Exception as e:
        print(f"✗ Error loading tickers: {e}")
        return False

def run_download_task():
    """Run the financial data download task directly"""
    try:
        from business_tasks.tasks.fundamentals.download import download_financial_data
        
        print("Starting financial data download...")
        result = download_financial_data(
            year=2023,
            quarter=4,
            data_types=["balance_sheet", "income_statement", "cash_flow"]
        )
        print(f"Download result: {result}")
        return result
    except Exception as e:
        print(f"✗ Download task failed: {e}")
        raise

def run_extract_task():
    """Run the financial data extraction task directly"""
    try:
        from business_tasks.tasks.fundamentals.extract import extract_financial_fields
        
        print("Starting financial data extraction...")
        result = extract_financial_fields(year=2023, quarter=4)
        print(f"Extraction result: {result}")
        return result
    except Exception as e:
        print(f"✗ Extraction task failed: {e}")
        raise

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
    'financial_data_pipeline_simplified',
    default_args=default_args,
    description='Download and process financial data - Simplified version',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['financial', 'data', 'simplified']
) as dag:

    # Start task
    start = DummyOperator(task_id='start')
    
    # Check database connectivity
    check_db = PythonOperator(
        task_id='check_database_connectivity',
        python_callable=check_database_connection
    )
    
    # Load tickers if needed
    load_tickers = PythonOperator(
        task_id='load_tickers_if_needed',
        python_callable=load_tickers_if_needed
    )
    
    # Download financial data
    download_data = PythonOperator(
        task_id='download_financial_data',
        python_callable=run_download_task
    )
    
    # Extract financial fields
    extract_data = PythonOperator(
        task_id='extract_financial_fields',
        python_callable=run_extract_task
    )
    
    # End task
    end = DummyOperator(task_id='end')
    
    # Set dependencies
    start >> check_db >> load_tickers >> download_data >> extract_data >> end