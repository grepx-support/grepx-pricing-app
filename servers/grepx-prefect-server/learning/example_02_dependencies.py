"""
Prefect Learning Example 02: Dependencies
=========================================
Example demonstrating task dependencies in Prefect
"""
from prefect import flow, task
from typing import List


@task(name="fetch_data")
def fetch_data(source: str) -> List[dict]:
    """Simulate fetching data from a source"""
    print(f"Fetching data from {source}...")
    # Simulated data
    data = [
        {"id": 1, "name": "Product A", "price": 10.99},
        {"id": 2, "name": "Product B", "price": 15.99},
        {"id": 3, "name": "Product C", "price": 8.99}
    ]
    print(f"Fetched {len(data)} items")
    return data


@task(name="process_data")
def process_data(raw_data: List[dict]) -> List[dict]:
    """Process the raw data"""
    print("Processing data...")
    processed_data = []
    for item in raw_data:
        processed_item = {
            "id": item["id"],
            "name": item["name"].upper(),
            "price": item["price"],
            "discounted_price": round(item["price"] * 0.9, 2)  # 10% discount
        }
        processed_data.append(processed_item)
    
    print(f"Processed {len(processed_data)} items")
    return processed_data


@task(name="validate_data")
def validate_data(processed_data: List[dict]) -> bool:
    """Validate the processed data"""
    print("Validating data...")
    is_valid = len(processed_data) > 0 and all("discounted_price" in item for item in processed_data)
    print(f"Data validation result: {'PASS' if is_valid else 'FAIL'}")
    return is_valid


@task(name="save_data")
def save_data(validated_data: List[dict], is_valid: bool) -> str:
    """Save the validated data"""
    if not is_valid:
        raise ValueError("Data validation failed, cannot save")
    
    print(f"Saving {len(validated_data)} items to storage...")
    # Simulate saving data
    summary = f"Saved {len(validated_data)} items successfully"
    print(summary)
    return summary


@flow(name="Data Processing Flow with Dependencies")
def data_processing_flow(data_source: str = "API"):
    """
    A flow that demonstrates:
    - Task dependencies (data flows from one task to another)
    - Sequential execution based on dependencies
    - Error handling when validation fails
    """
    print("Starting data processing flow...")
    
    # Fetch data (no dependencies, runs first)
    raw_data = fetch_data(data_source)
    
    # Process data depends on fetched data
    processed_data = process_data(raw_data)
    
    # Validate data depends on processed data
    is_valid = validate_data(processed_data)
    
    # Save data depends on both processed data and validation result
    result = save_data(processed_data, is_valid)
    
    print(f"Flow completed with result: {result}")
    return result


if __name__ == "__main__":
    # Run the flow
    result = data_processing_flow("Database")
    print(f"Final result: {result}")