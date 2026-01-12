"""
Prefect Learning Example 03: Configuration
==========================================
Example demonstrating configuration handling in Prefect flows
"""
from prefect import flow, task
from prefect.blocks.system import JSON
from prefect.filesystems import GitHub, LocalFileSystem
from typing import Dict, Any
import json


@task(name="load_config_from_block")
def load_config_from_block(block_name: str = "default-config") -> Dict[str, Any]:
    """Load configuration from a Prefect block"""
    print(f"Loading configuration from block: {block_name}")
    
    # In a real scenario, we would load from a registered block
    # For this example, we'll simulate loading a config
    default_config = {
        "api_url": "https://api.example.com",
        "timeout": 30,
        "retries": 3,
        "batch_size": 100,
        "data_source": "production",
        "features": {
            "enable_logging": True,
            "enable_monitoring": True,
            "enable_caching": False
        }
    }
    
    print(f"Loaded config: {default_config}")
    return default_config


@task(name="validate_config")
def validate_config(config: Dict[str, Any]) -> bool:
    """Validate the loaded configuration"""
    print("Validating configuration...")
    
    required_keys = ["api_url", "timeout", "retries"]
    is_valid = all(key in config for key in required_keys)
    
    if is_valid:
        print("Configuration validation: PASSED")
    else:
        print("Configuration validation: FAILED")
        missing_keys = [key for key in required_keys if key not in config]
        print(f"Missing required keys: {missing_keys}")
    
    return is_valid


@task(name="apply_config")
def apply_config(config: Dict[str, Any], environment: str = "dev") -> Dict[str, Any]:
    """Apply environment-specific overrides to the configuration"""
    print(f"Applying {environment} environment overrides...")
    
    # Environment-specific overrides
    env_overrides = {
        "dev": {
            "api_url": "https://api-dev.example.com",
            "timeout": 10,
            "enable_monitoring": False
        },
        "staging": {
            "api_url": "https://api-staging.example.com",
            "timeout": 20
        },
        "prod": {
            "timeout": 60,
            "retries": 5,
            "enable_caching": True
        }
    }
    
    # Apply overrides
    final_config = config.copy()
    if environment in env_overrides:
        for key, value in env_overrides[environment].items():
            final_config[key] = value
    
    print(f"Applied config for {environment}: {final_config}")
    return final_config


@task(name="setup_data_processing")
def setup_data_processing(config: Dict[str, Any]) -> str:
    """Setup data processing based on configuration"""
    print("Setting up data processing with configuration...")
    
    setup_info = {
        "api_url": config["api_url"],
        "timeout": config["timeout"],
        "batch_size": config["batch_size"],
        "source": config["data_source"],
        "logging_enabled": config["features"]["enable_logging"]
    }
    
    print(f"Data processing setup: {setup_info}")
    return json.dumps(setup_info, indent=2)


@flow(name="Configuration Management Flow")
def config_management_flow(
    config_block_name: str = "default-config",
    environment: str = "dev"
):
    """
    A flow that demonstrates:
    - Loading configuration from blocks
    - Validating configuration
    - Applying environment-specific overrides
    - Using configuration to set up processes
    """
    print("Starting configuration management flow...")
    
    # Load configuration
    config = load_config_from_block(config_block_name)
    
    # Validate configuration
    is_valid = validate_config(config)
    
    # Apply environment-specific overrides
    final_config = apply_config(config, environment)
    
    # Setup data processing based on configuration
    setup_result = setup_data_processing(final_config)
    
    result = {
        "config_valid": is_valid,
        "final_config": final_config,
        "setup_result": setup_result
    }
    
    print(f"Flow completed with result: {result}")
    return result


# Example with parameters
@flow(name="Parameterized Configuration Flow")
def parameterized_config_flow(
    api_url: str = "https://default-api.com",
    timeout: int = 30,
    environment: str = "dev",
    features: Dict[str, bool] = None
):
    """
    A parameterized flow that demonstrates:
    - Using flow parameters as configuration
    - Dynamic configuration based on inputs
    """
    if features is None:
        features = {"enable_logging": True, "enable_monitoring": False}
    
    print("Starting parameterized configuration flow...")
    
    # Construct config from parameters
    config = {
        "api_url": api_url,
        "timeout": timeout,
        "retries": 3,
        "batch_size": 100,
        "data_source": environment,
        "features": features
    }
    
    print(f"Constructed config from parameters: {config}")
    
    # Apply environment-specific settings
    if environment == "prod":
        config["timeout"] = 60
        config["retries"] = 5
        config["features"]["enable_monitoring"] = True
    elif environment == "dev":
        config["timeout"] = 10
        config["features"]["enable_logging"] = False
    
    print(f"Final config after environment adjustment: {config}")
    
    return config


if __name__ == "__main__":
    # Run the config management flow
    print("=== Running Config Management Flow ===")
    result1 = config_management_flow("my-config", "staging")
    print(f"Result 1: {result1}")
    
    print("\n=== Running Parameterized Config Flow ===")
    result2 = parameterized_config_flow(
        api_url="https://api.myapp.com",
        timeout=45,
        environment="prod",
        features={"enable_logging": True, "enable_monitoring": True, "enable_caching": True}
    )
    print(f"Result 2: {result2}")