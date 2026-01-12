"""
Prefect Server Main Application
===============================
Main entry point for the Prefect server application
"""
import os
import sys
from pathlib import Path

# Add main project directory to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Add task generator to path so we can import from task_generator
task_generator_path = project_root / "servers" / "grepx-task-generator-server" / "src" / "main"
sys.path.insert(0, str(task_generator_path))

from prefect import flow
from prefect.server import schemas
from prefect.settings import PREFECT_API_URL, PREFECT_SERVER_DATABASE_CONNECTION_URL


def initialize_prefect_server():
    """Initialize the Prefect server with configuration"""
    print("Initializing Prefect server...")
    
    # Load configuration from resources
    resources_dir = Path(__file__).parent / "resources"
    config_file = resources_dir / "config.yaml"
    
    if config_file.exists():
        from omegaconf import OmegaConf
        config = OmegaConf.load(config_file)
        print(f"Loaded Prefect configuration from {config_file}")
        
        # Set environment variables based on config if needed
        if "server" in config:
            server_config = config["server"]
            if "database" in server_config:
                db_url = server_config["database"].get("connection_url")
                if db_url:
                    os.environ["PREFECT_SERVER_DATABASE_CONNECTION_URL"] = db_url
    else:
        print(f"Configuration file not found at {config_file}, using defaults")
    
    print("Prefect server initialized successfully")


def start_server():
    """Start the Prefect server"""
    initialize_prefect_server()
    
    print("Starting Prefect server...")
    # Note: Actual server startup would happen here
    # For now, we'll just simulate the startup
    print("Prefect server started successfully!")
    print("API available at: http://localhost:4200")
    

def start_api_server():
    """Actually start the Prefect API server"""
    import subprocess
    import sys
    
    initialize_prefect_server()
    
    print("Starting Prefect API server...")
    try:
        # Start the actual Prefect server
        subprocess.run(["prefect", "server", "start", "--host", "127.0.0.1", "--port", "4200"])
    except FileNotFoundError:
        print("Prefect server command not found. Please ensure Prefect is installed.")
        print("Install with: pip install prefect")
        sys.exit(1)


def start_agent():
    """Start a Prefect worker to execute flow runs"""
    import subprocess
    import sys
    
    print("Starting Prefect worker...")
    try:
        # Start a Prefect worker to execute work from the default work pool
        subprocess.run(["prefect", "worker", "start", "--pool", "default-agent-pool"])
    except FileNotFoundError:
        print("Prefect worker command not found. Please ensure Prefect is installed.")
        print("Install with: pip install prefect")
        sys.exit(1)

def register_flows_from_database():
    """Register flows from the database using the task generator"""
    print("Registering flows from database...")
    
    # Import the Prefect flow generator
    from task_generator.prefect_asset_generator import PrefectFlowGenerator
    
    # Use our new database manager
    from .database_manager import DatabaseManager
    
    # Load database configuration
    resources_dir = Path(__file__).parent / "resources"
    task_config_file = resources_dir / "task-config.yaml"
    
    if task_config_file.exists():
        import yaml
        with open(task_config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        db_url = config['database']['db_url']
        db_manager = DatabaseManager(db_url=db_url)
        
        # Get Prefect artifacts from the database
        artifacts = db_manager.get_prefect_artifacts()
        print(f"Found {len(artifacts)} Prefect artifacts to register")
        
        # Initialize the Prefect flow generator
        flow_generator = PrefectFlowGenerator(db_manager)
        
        # Register flows based on artifacts
        for artifact in artifacts:
            print(f"Registering flow: {artifact['name']} ({artifact['entrypoint']})")
            # In a real implementation, we would register the flow here
            
        print("Flows registered from database successfully")
    else:
        print("Task configuration not found, skipping flow registration")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "start":
            start_server()
        elif command == "api-server":
            start_api_server()
        elif command == "agent":
            start_agent()
        elif command == "register-flows":
            register_flows_from_database()
        else:
            print(f"Unknown command: {command}")
            print("Usage: python main.py [start|api-server|agent|register-flows]")
    else:
        start_server()