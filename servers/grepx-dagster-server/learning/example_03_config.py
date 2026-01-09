"""
EXAMPLE 3: Configuration and I/O Managers
==========================================
Learn how to:
1. Configure assets with parameters
2. Use I/O managers to persist data to disk
3. Add metadata to assets
"""

from dagster import (
    asset, 
    Definitions, 
    Config,
    AssetExecutionContext,
    FilesystemIOManager
)
import pandas as pd
import json
from pathlib import Path


# Configuration class for parameterizing assets
class DataConfig(Config):
    """Configuration for data generation"""
    num_rows: int = 10
    min_age: int = 20
    max_age: int = 60


@asset
def employee_data(config: DataConfig):
    """
    Generate employee data with configurable size
    """
    import random
    
    names = ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Henry']
    
    data = {
        'name': [random.choice(names) for _ in range(config.num_rows)],
        'age': [random.randint(config.min_age, config.max_age) 
                for _ in range(config.num_rows)],
        'salary': [random.randint(40000, 100000) 
                   for _ in range(config.num_rows)]
    }
    
    df = pd.DataFrame(data)
    print(f"Generated {len(df)} employee records")
    return df


@asset
def salary_stats(context: AssetExecutionContext, employee_data: pd.DataFrame):
    """
    Calculate salary statistics with metadata
    """
    stats = {
        'mean': float(employee_data['salary'].mean()),
        'median': float(employee_data['salary'].median()),
        'min': float(employee_data['salary'].min()),
        'max': float(employee_data['salary'].max()),
        'std': float(employee_data['salary'].std())
    }
    
    # Add metadata that will show in the UI
    context.add_output_metadata({
        'num_employees': len(employee_data),
        'mean_salary': stats['mean'],
        'median_salary': stats['median'],
    })
    
    print(f"Salary statistics: {json.dumps(stats, indent=2)}")
    return stats


@asset
def age_distribution(context: AssetExecutionContext, employee_data: pd.DataFrame):
    """
    Analyze age distribution
    """
    age_groups = {
        '20-30': len(employee_data[employee_data['age'].between(20, 30)]),
        '31-40': len(employee_data[employee_data['age'].between(31, 40)]),
        '41-50': len(employee_data[employee_data['age'].between(41, 50)]),
        '51-60': len(employee_data[employee_data['age'].between(51, 60)]),
    }
    
    # Metadata for visualization
    context.add_output_metadata({
        'age_distribution': age_groups,
        'average_age': float(employee_data['age'].mean()),
    })
    
    return age_groups


# Set up I/O manager to save data to disk
storage_dir = Path(__file__).parent / "storage"
storage_dir.mkdir(exist_ok=True)

defs = Definitions(
    assets=[employee_data, salary_stats, age_distribution],
    resources={
        # This I/O manager will save all asset outputs to disk
        "io_manager": FilesystemIOManager(
            base_dir=str(storage_dir)
        )
    }
)


"""
HOW TO RUN:
-----------
1. Run: dagster dev -f example_03_config.py
2. In the UI, click on "employee_data"
3. Click "Materialize" 
4. You'll see a config editor - try changing num_rows to 20!
5. Check the "storage" folder - your data is saved there!

WHAT YOU'LL SEE:
----------------
- Assets with configurable parameters
- Metadata displayed in the UI (hover over assets)
- Data persisted to disk in the storage/ folder
- You can rematerialize with different config values

KEY CONCEPTS:
-------------
1. Config classes let you parameterize assets
2. AssetExecutionContext gives you access to metadata and logging
3. I/O managers handle data persistence automatically
4. Metadata makes your pipeline observable
"""
