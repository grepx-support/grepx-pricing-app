"""
Prefect Asset Factory
====================
Factory for creating Prefect assets with standardized configurations
"""
from typing import Dict, Any, List, Optional
from pathlib import Path
import importlib
from prefect import flow
from .builders.asset_builder import PrefectAssetBuilder


class PrefectAssetFactory:
    """
    Factory for creating Prefect assets with standardized configurations
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the Prefect asset factory
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        self.builder = PrefectAssetBuilder(config)
        
    def create_asset_from_entrypoint(self, name: str, entrypoint: str, 
                                   config: Optional[Dict[str, Any]] = None) -> flow:
        """
        Create a Prefect asset from an entrypoint string
        
        Args:
            name: Name of the asset
            entrypoint: Module path and function name (e.g. "module.submodule:function")
            config: Additional configuration
            
        Returns:
            Prefect flow object
        """
        # Parse the entrypoint
        module_path, function_name = entrypoint.rsplit(':', 1)
        
        # Import the module and function
        module = importlib.import_module(module_path)
        flow_func = getattr(module, function_name)
        
        return flow_func
    
    def create_assets_from_list(self, asset_configs: List[Dict[str, Any]]) -> List[flow]:
        """
        Create multiple Prefect assets from a list of configurations
        
        Args:
            asset_configs: List of asset configurations
            
        Returns:
            List of Prefect flow objects
        """
        assets = []
        for config in asset_configs:
            try:
                asset = self.create_asset_from_entrypoint(
                    config['name'],
                    config['entrypoint'],
                    config.get('config')
                )
                assets.append(asset)
            except Exception as e:
                print(f"Failed to create asset from config {config.get('name', 'unknown')}: {e}")
                continue
        
        return assets
    
    def create_deployments_from_artifacts(self, artifacts: List[Dict[str, Any]]):
        """
        Create deployments from artifact definitions
        
        Args:
            artifacts: List of artifact definitions
        """
        for artifact in artifacts:
            try:
                flow_obj = self.create_asset_from_entrypoint(
                    artifact['name'],
                    artifact['entrypoint']
                )
                
                # Register the flow with deployment settings
                self.builder.register_flow_with_deployment(flow_obj, artifact)
                
            except Exception as e:
                print(f"Failed to create deployment for artifact {artifact.get('name', 'unknown')}: {e}")
                continue