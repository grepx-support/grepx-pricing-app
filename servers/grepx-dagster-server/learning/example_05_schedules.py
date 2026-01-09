"""
EXAMPLE 5: Schedules
====================
Schedules let you run your pipelines automatically at specific times.
Think: daily reports, hourly data syncs, weekly summaries.
"""

from dagster import (
    asset,
    Definitions,
    ScheduleDefinition,
    AssetSelection,
    AssetExecutionContext
)
import pandas as pd
from datetime import datetime


@asset
def daily_sales_data(context: AssetExecutionContext):
    """
    Generate daily sales data
    In production, this would fetch from your sales database
    """
    import random
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    data = {
        'date': [today] * 10,
        'product': [f'Product_{i}' for i in range(10)],
        'sales': [random.randint(100, 1000) for _ in range(10)],
        'revenue': [random.randint(1000, 10000) for _ in range(10)]
    }
    
    df = pd.DataFrame(data)
    
    context.add_output_metadata({
        'total_sales': df['sales'].sum(),
        'total_revenue': df['revenue'].sum(),
        'date': today
    })
    
    print(f"Generated sales data for {today}")
    return df


@asset
def sales_summary(context: AssetExecutionContext, daily_sales_data: pd.DataFrame):
    """
    Summarize daily sales
    """
    summary = {
        'date': daily_sales_data['date'].iloc[0],
        'total_sales': int(daily_sales_data['sales'].sum()),
        'total_revenue': int(daily_sales_data['revenue'].sum()),
        'avg_revenue_per_sale': float(daily_sales_data['revenue'].mean()),
        'top_product': daily_sales_data.loc[
            daily_sales_data['revenue'].idxmax(), 'product'
        ]
    }
    
    context.add_output_metadata(summary)
    
    print("="*50)
    print(f"DAILY SALES SUMMARY - {summary['date']}")
    print("="*50)
    for key, value in summary.items():
        print(f"{key}: {value}")
    print("="*50)
    
    return summary


# Define schedules
daily_schedule = ScheduleDefinition(
    name="daily_sales_schedule",
    target=AssetSelection.all(),  # Run all assets
    cron_schedule="0 9 * * *",  # Every day at 9 AM
    # cron_schedule="* * * * *",  # Uncomment for every minute (testing)
)

hourly_schedule = ScheduleDefinition(
    name="hourly_refresh",
    target=AssetSelection.assets(daily_sales_data),  # Only refresh sales data
    cron_schedule="0 * * * *",  # Every hour
)

# Weekend report schedule
weekend_schedule = ScheduleDefinition(
    name="weekend_report",
    target=AssetSelection.all(),
    cron_schedule="0 10 * * 6",  # Saturday at 10 AM
)


defs = Definitions(
    assets=[daily_sales_data, sales_summary],
    schedules=[daily_schedule, hourly_schedule, weekend_schedule]
)


"""
HOW TO RUN:
-----------
1. Run: dagster dev -f example_05_schedules.py
2. Go to "Automation" tab in the UI
3. Click on "daily_sales_schedule"
4. Toggle "Running" to ON
5. For testing, change cron to "* * * * *" for every minute!

WHAT YOU'LL SEE:
----------------
- Multiple schedules in the Automation tab
- You can turn schedules on/off individually
- View schedule history and upcoming runs
- Manual trigger option for testing

CRON SCHEDULE FORMAT:
---------------------
"* * * * *"
 │ │ │ │ │
 │ │ │ │ └─── Day of week (0-6, Sunday = 0)
 │ │ │ └───── Month (1-12)
 │ │ └─────── Day of month (1-31)
 │ └───────── Hour (0-23)
 └─────────── Minute (0-59)

COMMON SCHEDULES:
-----------------
"0 0 * * *"     - Daily at midnight
"0 9 * * *"     - Daily at 9 AM
"0 */6 * * *"   - Every 6 hours
"0 9 * * 1"     - Every Monday at 9 AM
"0 0 1 * *"     - First day of every month
"*/15 * * * *"  - Every 15 minutes

PRO TIP:
--------
Use https://crontab.guru/ to build and test cron schedules!
"""
