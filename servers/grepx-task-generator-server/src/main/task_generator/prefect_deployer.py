#!/usr/bin/env python3
"""
Prefect Deployment Script
=========================
Reads Prefect artifacts from database and creates deployments
"""
import sys
import importlib
from pathlib import Path
from datetime import datetime
import subprocess
import os

import yaml

# Add the main directory to path to access prefect_app module
# The prefect_app package is located at src/main/prefect_app
import os
script_dir = Path(__file__).parent
main_dir = script_dir.parent.parent  # main/ (two levels up from task_generator)
sys.path.insert(0, str(main_dir))

# Also add Prefect server's src directory to path as backup
project_root = Path(__file__).parent.parent.parent.parent
prefect_server_src_path = project_root / "servers" / "grepx-prefect-server" / "src"
sys.path.insert(0, str(prefect_server_src_path))

# Also add the project root to make imports easier
sys.path.insert(0, str(project_root))

# Get project root (src/main/task_generator/prefect_deployer.py -> src/)
script_dir = Path(__file__).parent.parent
sys.path.insert(0, str(script_dir))

from task_generator.database import DatabaseManager
from grepx_models import PrefectArtifact


def load_config():
    """Load database configuration from task-config.yaml"""
    config_path = script_dir / "resources" / "task-config.yaml"
    
    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config


def create_deployment_from_artifact(artifact):
    """Create a Prefect deployment from a PrefectArtifact record using CLI command"""
    try:
        # Parse the entrypoint to get module path and function name
        module_path, function_name = artifact.entrypoint.rsplit(':', 1)
        
        # Import the module to verify it exists
        module = importlib.import_module(module_path)
        flow_func = getattr(module, function_name)
        
        # Get the path to the flow file from the module
        module_file = module.__file__
        
        # Deployment name from database
        deployment_name = artifact.deployment_name or artifact.name
        work_pool_name = artifact.work_pool_name
        parameters = artifact.parameters or {}
        
        # Construct the prefect deploy command
        cmd = [
            "prefect", "deploy",
            "-n", deployment_name,
            "-p", work_pool_name,
            f"{module_file}:{function_name}"
        ]
        
        # Add parameters if they exist
        if parameters:
            for key, value in parameters.items():
                cmd.extend(["--param", f"{key}={value}"])
        
        print(f"    Running command: {' '.join(cmd)}")
        
        # Execute the command
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(prefect_server_src_path / "main"))
        
        if result.returncode == 0:
            print(f"  [+] Created Prefect deployment: {deployment_name}")
            return True
        else:
            print(f"  [-] Failed to create deployment via CLI: {result.stderr}")
            # Fallback to the old method if CLI fails
            print("    Attempting fallback method...")
            return create_deployment_fallback(artifact)
        
    except ImportError as e:
        print(f"  [-] Failed to import flow for '{artifact.name}': {e}")
        return False
    except AttributeError as e:
        print(f"  [-] Function not found in '{artifact.entrypoint}': {e}")
        return False
    except Exception as e:
        print(f"  [-] Failed to create deployment for '{artifact.name}': {e}")
        # Try fallback method
        return create_deployment_fallback(artifact)


def create_deployment_fallback(artifact):
    """Fallback method using the old approach"""
    try:
        # Import the flow function from the entrypoint
        module_path, function_name = artifact.entrypoint.rsplit(':', 1)
        module = importlib.import_module(module_path)
        flow_func = getattr(module, function_name)
        
        # Debug: Print the deployment name we're trying to use
        deployment_name = artifact.deployment_name or artifact.name
        print(f"    Attempting fallback deployment: {deployment_name}")
        
        # Create deployment using the new Prefect 3.x approach
        deployment = flow_func.to_deployment(
            name=deployment_name,  # Use the database deployment name
            work_pool_name=artifact.work_pool_name,
            parameters=artifact.parameters or {},
            tags=[artifact.group_name, artifact.artifact_type]
        )
        
        print(f"  [+] Created Prefect deployment (fallback): {deployment_name}")
        return True
        
    except Exception as e:
        print(f"  [-] Fallback method also failed: {e}")
        return False


def main():
    """Main execution - read from database and create deployments"""
    print("Loading configuration...")
    config = load_config()
    
    # Get database URL
    db_url = config['database']['db_url']
    
    # Initialize database
    print(f"Connecting to database: {db_url}")
    db_manager = DatabaseManager(db_url=db_url)
    
    # Initialize database schema if needed
    print("Initializing database schema...")
    db_manager.initialize_schema()
    
    # Query active Prefect artifacts from database
    print("Querying Prefect artifacts from database...")
    with db_manager.get_session() as session:
        artifacts = session.query(PrefectArtifact).filter_by(is_active=True).all()
        
        if not artifacts:
            print("No active Prefect artifacts found in database.")
            return
        
        print(f"Found {len(artifacts)} active Prefect artifacts")
        
        success_count = 0
        for artifact in artifacts:
            print(f"Processing artifact: {artifact.name}")
            
            if create_deployment_from_artifact(artifact):
                success_count += 1
    
    print(f"\nDeployment completed: {success_count}/{len(artifacts)} artifacts deployed successfully")


if __name__ == '__main__':
    main()