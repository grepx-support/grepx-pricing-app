from prefect import flow
import sys
from pathlib import Path

# Add business tasks to path - ensure path works from Prefect server directory
# When running as deployment, working directory is project root (due to pull step)
script_dir = Path(__file__).parent.parent.parent.parent  # Go from flows/ to src/main/
business_tasks_path = script_dir.parent.parent / "../../business-tasks"  # From servers/grepx-prefect-server/
sys.path.insert(0, str(business_tasks_path.resolve()))

@flow(name="hello-flow")
def say_hello():
    """Simple Hello World flow"""
    print("Hello, World!")
    message = "Hello from Prefect!"
    print(message)
    return message

if __name__ == "__main__":
    # Run the flow directly for testing
    result = say_hello()
    print(f"Flow returned: {result}")