"""
EXAMPLE 7: Partitioned Assets
==============================
Partitions let you process data in chunks (by date, region, etc.)
Perfect for: daily reports, backfills, incremental processing

Instead of processing ALL data, process it partition by partition.
"""

from dagster import (
    asset,
    Definitions,
    DailyPartitionsDefinition,
    StaticPartitionsDefinition,
    AssetExecutionContext,
)
import pandas as pd
from datetime import datetime
import random


# Define partitions - one partition per day
daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")

# Static partitions - for regions
region_partitions = StaticPartitionsDefinition(
    ["North", "South", "East", "West"]
)


@asset(
    partitions_def=daily_partitions,
)
def daily_orders(context: AssetExecutionContext):
    """
    Generate orders for a specific day
    The partition key tells us which day to process
    """
    partition_date = context.partition_key  # e.g., "2024-01-15"
    
    # Generate random orders for this day
    num_orders = random.randint(50, 150)
    
    orders = {
        'order_id': [f'ORD_{partition_date}_{i}' for i in range(num_orders)],
        'date': [partition_date] * num_orders,
        'amount': [random.uniform(10, 500) for _ in range(num_orders)],
        'customer_id': [f'CUST_{random.randint(1, 100)}' for _ in range(num_orders)]
    }
    
    df = pd.DataFrame(orders)
    
    context.log.info(f"Generated {num_orders} orders for {partition_date}")
    context.add_output_metadata({
        'num_orders': num_orders,
        'total_revenue': df['amount'].sum(),
        'partition_date': partition_date
    })
    
    return df


@asset(
    partitions_def=daily_partitions,
)
def daily_revenue(context: AssetExecutionContext, daily_orders: pd.DataFrame):
    """
    Calculate revenue for a specific day
    Depends on the same day's orders
    """
    partition_date = context.partition_key
    
    revenue_data = {
        'date': partition_date,
        'total_revenue': daily_orders['amount'].sum(),
        'num_orders': len(daily_orders),
        'avg_order_value': daily_orders['amount'].mean(),
        'max_order': daily_orders['amount'].max(),
        'min_order': daily_orders['amount'].min()
    }
    
    context.log.info(f"Revenue for {partition_date}: ${revenue_data['total_revenue']:.2f}")
    context.add_output_metadata(revenue_data)
    
    return revenue_data


@asset(
    partitions_def=region_partitions,
)
def regional_sales(context: AssetExecutionContext):
    """
    Generate sales data for a specific region
    One partition per region
    """
    region = context.partition_key  # "North", "South", etc.
    
    # Generate random sales for this region
    num_sales = random.randint(20, 80)
    
    sales = {
        'sale_id': [f'{region}_{i}' for i in range(num_sales)],
        'region': [region] * num_sales,
        'amount': [random.uniform(100, 1000) for _ in range(num_sales)],
        'product': [f'Product_{random.randint(1, 10)}' for _ in range(num_sales)]
    }
    
    df = pd.DataFrame(sales)
    
    context.log.info(f"Generated {num_sales} sales for region: {region}")
    context.add_output_metadata({
        'region': region,
        'num_sales': num_sales,
        'total_sales': df['amount'].sum()
    })
    
    return df


# Non-partitioned asset that depends on ALL partitions
@asset
def overall_summary(daily_revenue):
    """
    This asset waits for ALL daily_revenue partitions to complete
    Then creates an overall summary
    
    Note: In practice, you'd use a database to aggregate across partitions
    """
    summary = {
        'message': 'This would aggregate all partitions',
        'note': 'In production, query your database or data lake'
    }
    
    return summary


defs = Definitions(
    assets=[daily_orders, daily_revenue, regional_sales, overall_summary],
)


"""
HOW TO RUN:
-----------
1. Run: dagster dev -f example_07_partitions.py
2. Go to "Assets" tab
3. Click on "daily_orders"
4. You'll see a partition selector!
5. Choose a date (e.g., 2024-01-15)
6. Click "Materialize"

WHAT YOU'LL SEE:
----------------
- Calendar view for date partitions
- List view for static partitions
- Each partition can be materialized independently
- Backfill option to process many partitions at once
- Partition status (success/failed/missing)

BACKFILLING:
------------
To process multiple dates at once:
1. Select "daily_orders" asset
2. Click "Materialize"
3. Choose "Backfill" option
4. Select date range (e.g., 2024-01-01 to 2024-01-31)
5. Click "Launch backfill"

This will process all 31 days efficiently!

WHY USE PARTITIONS?
-------------------
1. Process historical data incrementally
2. Reprocess specific time periods
3. Parallel processing (multiple workers)
4. Clear data lineage per partition
5. Easy to identify failures by partition

COMMON PARTITION TYPES:
-----------------------
- DailyPartitionsDefinition: One partition per day
- WeeklyPartitionsDefinition: One partition per week
- MonthlyPartitionsDefinition: One partition per month
- HourlyPartitionsDefinition: One partition per hour
- StaticPartitionsDefinition: Fixed list (regions, categories, etc.)
- DynamicPartitionsDefinition: Can add partitions dynamically

PRODUCTION EXAMPLE:
-------------------
# Process last 7 days of data
dagster asset materialize -a daily_orders \\
  --partition-range "2024-01-10 to 2024-01-16"

# Process specific partition
dagster asset materialize -a daily_orders \\
  --partition "2024-01-15"
"""
