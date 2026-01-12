"""
Prefect Learning Example 04: Resources
=======================================
Example demonstrating resource management in Prefect flows
"""
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.filesystems import LocalFileSystem, GitHub
from prefect.infrastructure import Process, DockerContainer
from typing import Dict, Any
import tempfile
import os


@task(name="setup_local_filesystem")
def setup_local_filesystem(base_path: str = "./data") -> str:
    """Setup local filesystem resource"""
    print(f"Setting up local filesystem at: {base_path}")
    
    # Create the directory if it doesn't exist
    os.makedirs(base_path, exist_ok=True)
    
    # Return the full path
    full_path = os.path.abspath(base_path)
    print(f"Local filesystem setup complete at: {full_path}")
    return full_path


@task(name="write_sample_data")
def write_sample_data(file_path: str, data: Dict[str, Any]) -> str:
    """Write sample data to the filesystem"""
    print(f"Writing sample data to: {file_path}")
    
    # Create a subdirectory for this operation
    timestamp_dir = os.path.join(file_path, "sample_data")
    os.makedirs(timestamp_dir, exist_ok=True)
    
    # Write data to a file
    data_file = os.path.join(timestamp_dir, "sample.json")
    import json
    with open(data_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Sample data written to: {data_file}")
    return data_file


@task(name="read_sample_data")
def read_sample_data(file_path: str) -> Dict[str, Any]:
    """Read sample data from the filesystem"""
    print(f"Reading sample data from: {file_path}")
    
    import json
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    print(f"Read data: {data}")
    return data


@task(name="setup_database_connection")
def setup_database_connection(db_type: str = "sqlite") -> Dict[str, str]:
    """Simulate setting up a database connection resource"""
    print(f"Setting up {db_type} database connection...")
    
    # Simulate database configuration
    db_config = {
        "type": db_type,
        "host": "localhost",
        "port": "5432" if db_type == "postgresql" else "3306" if db_type == "mysql" else "memory",
        "database": "sample_db",
        "username": "admin",
        "password": "secret123",  # In real apps, use secrets
        "connection_string": f"{db_type}://admin:secret123@localhost:5432/sample_db" 
                              if db_type in ["postgresql", "mysql"] 
                              else f"{db_type}:///sample.db"
    }
    
    print(f"Database connection configured: {db_config}")
    return db_config


@task(name="setup_secret_resource")
def setup_secret_resource(secret_name: str = "api-key") -> str:
    """Simulate setting up a secret resource"""
    print(f"Setting up secret resource: {secret_name}")
    
    # In a real application, we would load from a secure secret store
    # For this example, we'll simulate loading a secret
    secrets_map = {
        "api-key": "sk-1234567890abcdef",
        "database-password": "db-secret-password",
        "aws-access-key": "AKIAIOSFODNN7EXAMPLE",
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
    }
    
    secret_value = secrets_map.get(secret_name, f"default-{secret_name}-value")
    print(f"Secret resource '{secret_name}' loaded")
    return secret_value


@task(name="process_with_resources")
def process_with_resources(
    data_file: str, 
    db_config: Dict[str, str], 
    api_secret: str
) -> Dict[str, Any]:
    """Process data using various resources"""
    print("Processing data with resources...")
    
    # Read the data file
    import json
    with open(data_file, 'r') as f:
        data = json.load(f)
    
    # Simulate using database and secret
    processed_info = {
        "input_file": data_file,
        "database_used": db_config["type"],
        "secret_loaded": len(api_secret) > 0,
        "data_processed": len(data) if isinstance(data, list) else len(data.keys()) if isinstance(data, dict) else 1,
        "timestamp": "2024-01-01T00:00:00Z"  # In real app, use actual timestamp
    }
    
    print(f"Processing completed: {processed_info}")
    return processed_info


@flow(name="Resource Management Flow")
def resource_management_flow(
    data_dir: str = "./temp_data",
    db_type: str = "sqlite",
    secret_name: str = "api-key"
):
    """
    A flow that demonstrates:
    - Setting up filesystem resources
    - Managing database connections
    - Handling secrets securely
    - Coordinating resources in a workflow
    """
    print("Starting resource management flow...")
    
    # Setup filesystem resource
    fs_path = setup_local_filesystem(data_dir)
    
    # Create sample data to work with
    sample_data = {
        "users": [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
            {"id": 3, "name": "Charlie", "email": "charlie@example.com"}
        ],
        "timestamp": "2024-01-01T00:00:00Z"
    }
    
    # Write sample data to filesystem
    data_file = write_sample_data(fs_path, sample_data)
    
    # Setup database connection resource
    db_config = setup_database_connection(db_type)
    
    # Setup secret resource
    api_secret = setup_secret_resource(secret_name)
    
    # Process data using all resources
    result = process_with_resources(data_file, db_config, api_secret)
    
    print(f"Flow completed with result: {result}")
    return result


# Example with infrastructure resources
@flow(name="Infrastructure Configuration Flow")
def infrastructure_config_flow(
    infrastructure_type: str = "process",
    image_name: str = "prefecthq/prefect:latest"
):
    """
    A flow that demonstrates:
    - Infrastructure configuration
    - Different execution environments
    - Resource allocation
    """
    print(f"Configuring infrastructure: {infrastructure_type}")
    
    # Define infrastructure based on type
    if infrastructure_type == "docker":
        print("Setting up Docker container infrastructure...")
        infra_config = {
            "type": "docker",
            "image": image_name,
            "env": {"PREFECT_LOGGING_LEVEL": "INFO"},
            "network_mode": "bridge"
        }
    elif infrastructure_type == "kubernetes":
        print("Setting up Kubernetes job infrastructure...")
        infra_config = {
            "type": "kubernetes",
            "image": image_name,
            "env": {"PREFECT_LOGGING_LEVEL": "INFO"},
            "namespace": "prefect-jobs"
        }
    else:  # process (default)
        print("Setting up process infrastructure...")
        infra_config = {
            "type": "process",
            "env": {"PREFECT_LOGGING_LEVEL": "INFO"},
            "working_dir": os.getcwd()
        }
    
    print(f"Infrastructure configured: {infra_config}")
    
    # Simulate deploying with this infrastructure
    deployment_info = {
        "infrastructure": infra_config,
        "status": "configured",
        "ready_for_deployment": True
    }
    
    print(f"Deployment ready: {deployment_info}")
    return deployment_info


if __name__ == "__main__":
    # Run the resource management flow
    print("=== Running Resource Management Flow ===")
    result1 = resource_management_flow("./temp_data", "sqlite", "api-key")
    print(f"Result 1: {result1}")
    
    print("\n=== Running Infrastructure Configuration Flow ===")
    result2 = infrastructure_config_flow("process", "my-custom-image:latest")
    print(f"Result 2: {result2}")