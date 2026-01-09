"""
EXAMPLE 8: Complete Project Structure
======================================
This example shows how to organize a real Dagster project with:
- Multiple modules
- Configuration management
- Testing setup
- Production-ready patterns
"""

from dagster import (
    asset,
    Definitions,
    FilesystemIOManager,
    ConfigurableResource,
    AssetExecutionContext,
    define_asset_job,
    ScheduleDefinition,
    AssetSelection,
)
import pandas as pd
from pathlib import Path
from typing import Dict, Any
import yaml


# ============================================================================
# CONFIGURATION MANAGEMENT
# ============================================================================

class AppConfig:
    """
    Load configuration from YAML file
    Integrates with your provided dagster_config.yaml
    """
    def __init__(self, config_path: str = None):
        if config_path and Path(config_path).exists():
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            # Default configuration
            self.config = {
                'app': {
                    'name': 'dagster_framework',
                    'version': '1.0.0',
                    'environment': 'local'
                },
                'storage': {
                    'backend': 'sqlite',
                    'base_dir': './storage'
                }
            }
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get config value with dot notation"""
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
        return value if value is not None else default


# Initialize config
config = AppConfig()


# ============================================================================
# RESOURCES
# ============================================================================

class DataWarehouse(ConfigurableResource):
    """Simulated data warehouse connection"""
    connection_string: str = "sqlite:///data.db"
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query"""
        # In production: return pd.read_sql(query, self.connection)
        print(f"Executing query: {query[:50]}...")
        return pd.DataFrame({'result': ['Mock data']})
    
    def write_table(self, df: pd.DataFrame, table_name: str):
        """Write dataframe to table"""
        # In production: df.to_sql(table_name, self.connection)
        print(f"Writing {len(df)} rows to table '{table_name}'")


class APIService(ConfigurableResource):
    """External API service"""
    api_key: str = "dev_key"
    base_url: str = "https://api.example.com"
    
    def fetch_data(self, endpoint: str) -> Dict:
        """Fetch data from API"""
        print(f"Fetching from {self.base_url}/{endpoint}")
        return {'status': 'success', 'data': []}


# ============================================================================
# ASSETS - DATA INGESTION LAYER
# ============================================================================

@asset(
    group_name="ingestion",
    description="Extract raw customer data from API"
)
def raw_customers(api_service: APIService) -> pd.DataFrame:
    """Extract customer data from external API"""
    data = api_service.fetch_data('customers')
    
    # Simulate customer data
    customers = pd.DataFrame({
        'customer_id': range(1, 101),
        'name': [f'Customer {i}' for i in range(1, 101)],
        'email': [f'customer{i}@example.com' for i in range(1, 101)],
        'signup_date': pd.date_range('2023-01-01', periods=100, freq='D')
    })
    
    return customers


@asset(
    group_name="ingestion",
    description="Extract raw transaction data"
)
def raw_transactions() -> pd.DataFrame:
    """Extract transaction data"""
    import random
    
    transactions = pd.DataFrame({
        'transaction_id': range(1, 501),
        'customer_id': [random.randint(1, 100) for _ in range(500)],
        'amount': [random.uniform(10, 500) for _ in range(500)],
        'transaction_date': pd.date_range('2023-01-01', periods=500, freq='H')
    })
    
    return transactions


# ============================================================================
# ASSETS - TRANSFORMATION LAYER
# ============================================================================

@asset(
    group_name="transformation",
    description="Clean and transform customer data"
)
def clean_customers(
    context: AssetExecutionContext,
    raw_customers: pd.DataFrame
) -> pd.DataFrame:
    """Clean customer data"""
    df = raw_customers.copy()
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['email'])
    
    # Add derived fields
    df['days_since_signup'] = (
        pd.Timestamp.now() - df['signup_date']
    ).dt.days
    
    context.add_output_metadata({
        'num_customers': len(df),
        'duplicate_removed': len(raw_customers) - len(df)
    })
    
    return df


@asset(
    group_name="transformation",
    description="Aggregate transaction metrics per customer"
)
def customer_metrics(
    context: AssetExecutionContext,
    raw_transactions: pd.DataFrame,
    clean_customers: pd.DataFrame
) -> pd.DataFrame:
    """Calculate customer-level metrics"""
    
    # Aggregate transactions by customer
    metrics = raw_transactions.groupby('customer_id').agg({
        'transaction_id': 'count',
        'amount': ['sum', 'mean', 'max']
    }).reset_index()
    
    metrics.columns = [
        'customer_id', 
        'transaction_count',
        'total_spent',
        'avg_transaction',
        'max_transaction'
    ]
    
    # Join with customer data
    result = clean_customers.merge(metrics, on='customer_id', how='left')
    result = result.fillna(0)
    
    context.add_output_metadata({
        'num_customers': len(result),
        'total_revenue': result['total_spent'].sum(),
        'avg_customer_value': result['total_spent'].mean()
    })
    
    return result


# ============================================================================
# ASSETS - ANALYTICS LAYER
# ============================================================================

@asset(
    group_name="analytics",
    description="Customer segmentation based on behavior"
)
def customer_segments(
    context: AssetExecutionContext,
    customer_metrics: pd.DataFrame
) -> pd.DataFrame:
    """Segment customers into groups"""
    df = customer_metrics.copy()
    
    # Simple segmentation
    def get_segment(row):
        if row['total_spent'] > 5000:
            return 'VIP'
        elif row['total_spent'] > 2000:
            return 'Regular'
        elif row['total_spent'] > 0:
            return 'Occasional'
        else:
            return 'Inactive'
    
    df['segment'] = df.apply(get_segment, axis=1)
    
    # Segment summary
    segment_counts = df['segment'].value_counts().to_dict()
    
    context.add_output_metadata({
        'segment_distribution': segment_counts
    })
    
    return df


@asset(
    group_name="analytics",
    description="Daily business KPIs"
)
def business_kpis(
    context: AssetExecutionContext,
    customer_segments: pd.DataFrame,
    raw_transactions: pd.DataFrame
) -> Dict:
    """Calculate key business metrics"""
    
    kpis = {
        'total_customers': len(customer_segments),
        'active_customers': len(customer_segments[customer_segments['segment'] != 'Inactive']),
        'vip_customers': len(customer_segments[customer_segments['segment'] == 'VIP']),
        'total_revenue': customer_segments['total_spent'].sum(),
        'avg_customer_value': customer_segments['total_spent'].mean(),
        'total_transactions': len(raw_transactions),
    }
    
    context.add_output_metadata(kpis)
    
    print("=" * 60)
    print("BUSINESS KPIs")
    print("=" * 60)
    for key, value in kpis.items():
        print(f"{key:.<40} {value:>15.2f}" if isinstance(value, float) else f"{key:.<40} {value:>15}")
    print("=" * 60)
    
    return kpis


# ============================================================================
# JOBS - Define specific asset groups to run together
# ============================================================================

# Daily ETL job
daily_etl_job = define_asset_job(
    name="daily_etl",
    selection=AssetSelection.groups("ingestion", "transformation", "analytics"),
    description="Daily ETL pipeline - ingestion → transformation → analytics"
)

# Quick refresh job (analytics only)
analytics_refresh_job = define_asset_job(
    name="analytics_refresh",
    selection=AssetSelection.groups("analytics"),
    description="Quick refresh of analytics assets"
)


# ============================================================================
# SCHEDULES
# ============================================================================

daily_schedule = ScheduleDefinition(
    name="daily_pipeline",
    target=daily_etl_job,
    cron_schedule="0 2 * * *",  # 2 AM daily
    description="Run full pipeline daily"
)


# ============================================================================
# DEFINITIONS - Bring it all together
# ============================================================================

defs = Definitions(
    assets=[
        # Ingestion
        raw_customers,
        raw_transactions,
        # Transformation
        clean_customers,
        customer_metrics,
        # Analytics
        customer_segments,
        business_kpis,
    ],
    jobs=[
        daily_etl_job,
        analytics_refresh_job,
    ],
    schedules=[
        daily_schedule,
    ],
    resources={
        "io_manager": FilesystemIOManager(
            base_dir=config.get('storage.base_dir', './storage')
        ),
        "data_warehouse": DataWarehouse(
            connection_string=config.get('storage.postgres_url', 'sqlite:///data.db')
        ),
        "api_service": APIService(
            api_key="your_api_key_here",
            base_url="https://api.example.com"
        ),
    }
)


"""
HOW TO RUN:
-----------
1. Run: dagster dev -f example_08_complete_project.py
2. Explore the UI - notice the organization!

WHAT YOU'LL SEE:
----------------
- Assets organized into groups (ingestion, transformation, analytics)
- Lineage graph showing data flow
- Two jobs you can run
- Schedule configured
- Resources configured

PROJECT STRUCTURE:
------------------
In a real project, you'd split this into:

dagster_project/
├── dagster_project/
│   ├── __init__.py
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── ingestion.py      (raw_customers, raw_transactions)
│   │   ├── transformation.py (clean_customers, customer_metrics)
│   │   └── analytics.py      (customer_segments, business_kpis)
│   ├── resources/
│   │   ├── __init__.py
│   │   ├── data_warehouse.py
│   │   └── api_service.py
│   ├── jobs/
│   │   └── __init__.py
│   ├── schedules/
│   │   └── __init__.py
│   └── sensors/
│       └── __init__.py
├── config/
│   ├── local.yaml
│   └── prod.yaml
├── tests/
│   └── test_assets.py
├── setup.py
└── README.md

KEY CONCEPTS:
-------------
1. Asset groups for organization
2. Jobs for running specific asset combinations
3. Resources for external connections
4. Configuration management
5. Metadata for observability

PRODUCTION TIPS:
----------------
1. Use different config files per environment
2. Add data quality checks
3. Implement proper error handling
4. Use partitions for large datasets
5. Set up alerting (Slack, email)
6. Monitor with Dagster+ or self-hosted UI
7. Write tests for your assets
"""
