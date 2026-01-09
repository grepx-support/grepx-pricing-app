"""
EXAMPLE 2: Assets with Dependencies
====================================
Now we'll create assets that depend on each other.
This creates a data pipeline: raw_data → cleaned_data → analysis
"""

from dagster import asset, Definitions
import pandas as pd


@asset
def raw_data():
    """
    Step 1: Generate raw data
    This simulates fetching data from a source
    """
    data = {
        'name': ['Alice', 'Bob', 'Charlie', 'David'],
        'age': [25, 30, 35, 28],
        'salary': [50000, 60000, 75000, 55000]
    }
    df = pd.DataFrame(data)
    print(f"Generated raw data with {len(df)} rows")
    return df


@asset
def cleaned_data(raw_data: pd.DataFrame):
    """
    Step 2: Clean the data
    This asset depends on raw_data (notice the parameter!)
    Dagster will automatically pass the output of raw_data here
    """
    # Add a new column
    df = raw_data.copy()
    df['age_group'] = df['age'].apply(
        lambda x: 'young' if x < 30 else 'experienced'
    )
    print(f"Cleaned data, added age_group column")
    return df


@asset
def analysis(cleaned_data: pd.DataFrame):
    """
    Step 3: Analyze the data
    This depends on cleaned_data
    """
    avg_salary = cleaned_data['salary'].mean()
    avg_age = cleaned_data['age'].mean()
    
    result = {
        'average_salary': avg_salary,
        'average_age': avg_age,
        'total_employees': len(cleaned_data),
        'young_employees': len(cleaned_data[cleaned_data['age_group'] == 'young'])
    }
    
    print("Analysis Results:")
    for key, value in result.items():
        print(f"  {key}: {value}")
    
    return result


# Define the pipeline
defs = Definitions(
    assets=[raw_data, cleaned_data, analysis],
)


"""
HOW TO RUN:
-----------
1. Install pandas: pip install pandas
2. Run: dagster dev -f example_02_dependencies.py
3. Open http://localhost:3000
4. Go to "Assets" tab
5. Click "Materialize all"

WHAT YOU'LL SEE:
----------------
- Three assets in a pipeline: raw_data → cleaned_data → analysis
- Dagster shows the dependency graph visually
- Assets execute in order (can't run analysis before raw_data)
- If raw_data changes, downstream assets become "stale"

KEY CONCEPT:
------------
When you add a parameter to an asset function that matches another asset's name,
Dagster automatically creates a dependency and passes the data!
"""
