"""
EXAMPLE 6: Sensors
==================
Sensors watch for events and trigger runs automatically.
Unlike schedules (time-based), sensors are event-based.

Examples: New file appears, database changes, API update, etc.
"""

from dagster import (
    asset,
    sensor,
    RunRequest,
    SensorEvaluationContext,
    Definitions,
    AssetSelection,
    SensorResult,
    AssetExecutionContext
)
import os
import json
from pathlib import Path
from datetime import datetime


# Create directories for the example
WATCH_DIR = Path(__file__).parent / "incoming_files"
PROCESSED_DIR = Path(__file__).parent / "processed_files"
WATCH_DIR.mkdir(exist_ok=True)
PROCESSED_DIR.mkdir(exist_ok=True)


@asset
def process_incoming_file(context: AssetExecutionContext):
    """
    Process the most recent file from incoming_files directory
    """
    files = list(WATCH_DIR.glob("*.json"))
    
    if not files:
        context.log.info("No files to process")
        return None
    
    # Get the most recent file
    latest_file = max(files, key=os.path.getctime)
    
    # Read and process
    with open(latest_file, 'r') as f:
        data = json.load(f)
    
    context.log.info(f"Processing file: {latest_file.name}")
    context.log.info(f"Data: {data}")
    
    # Add timestamp
    data['processed_at'] = datetime.now().isoformat()
    
    # Move to processed directory
    processed_path = PROCESSED_DIR / latest_file.name
    with open(processed_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    # Remove from incoming
    latest_file.unlink()
    
    context.add_output_metadata({
        'file_name': latest_file.name,
        'data_keys': list(data.keys()),
        'processed_count': 1
    })
    
    return data


@asset
def file_summary(process_incoming_file):
    """
    Create a summary of processed files
    """
    processed_files = list(PROCESSED_DIR.glob("*.json"))
    
    return {
        'total_processed': len(processed_files),
        'files': [f.name for f in processed_files]
    }


# FILE WATCHER SENSOR
@sensor(
    target=AssetSelection.assets(process_incoming_file),
    minimum_interval_seconds=5,  # Check every 5 seconds
)
def file_sensor(context: SensorEvaluationContext):
    """
    Watch for new JSON files in the incoming_files directory
    """
    files = list(WATCH_DIR.glob("*.json"))
    
    if files:
        context.log.info(f"Found {len(files)} file(s) to process!")
        
        # Trigger a run for each file
        for file in files:
            yield RunRequest(
                run_key=f"process_{file.name}_{file.stat().st_mtime}",
                tags={
                    "file_name": file.name,
                    "triggered_by": "file_sensor"
                }
            )
    else:
        context.log.info("No new files found")


# ADVANCED: CURSOR-BASED SENSOR
# This tracks what it has already processed using a cursor
@sensor(
    target=AssetSelection.assets(process_incoming_file),
    minimum_interval_seconds=10,
)
def advanced_file_sensor(context: SensorEvaluationContext):
    """
    A more advanced sensor that uses a cursor to track processed files
    """
    # Get cursor (last processed file timestamp)
    last_timestamp = float(context.cursor) if context.cursor else 0.0
    
    files = list(WATCH_DIR.glob("*.json"))
    new_files = [f for f in files if f.stat().st_mtime > last_timestamp]
    
    if new_files:
        context.log.info(f"Found {len(new_files)} new file(s)")
        
        latest_timestamp = max(f.stat().st_mtime for f in new_files)
        
        for file in new_files:
            yield RunRequest(
                run_key=f"process_{file.name}",
                tags={
                    "file_name": file.name,
                    "triggered_by": "advanced_sensor"
                }
            )
        
        # Update cursor to latest timestamp
        return SensorResult(cursor=str(latest_timestamp))
    
    return SensorResult(cursor=context.cursor)


defs = Definitions(
    assets=[process_incoming_file, file_summary],
    sensors=[file_sensor, advanced_file_sensor],
)


"""
HOW TO RUN:
-----------
1. Run: dagster dev -f example_06_sensors.py
2. Go to "Automation" tab → "Sensors"
3. Turn ON the "file_sensor"
4. Create a test file:
   
   echo '{"name": "test", "value": 123}' > incoming_files/test_001.json
   
5. Watch the sensor detect it and trigger a run!

WHAT YOU'LL SEE:
----------------
- Sensor checking every 5 seconds
- Automatic run when file appears
- File moved from incoming_files/ to processed_files/
- Sensor logs showing detection

TEST IT:
--------
# Create multiple files
for i in {1..5}; do
    echo "{\"id\": $i, \"name\": \"item_$i\"}" > incoming_files/item_$i.json
    sleep 2
done

SENSOR USE CASES:
-----------------
1. File watching (this example)
2. Database changes (poll for new rows)
3. API polling (check for new data)
4. Slack messages
5. S3 bucket changes
6. Queue messages (SQS, RabbitMQ)

KEY CONCEPTS:
-------------
1. Sensors run on an interval (minimum_interval_seconds)
2. They can yield multiple RunRequests
3. Cursors help track state between runs
4. run_key prevents duplicate runs
5. Tags help identify what triggered the run
"""
