"""
Prefect Asset Builder
Mirrors the Dagster asset builder pattern for creating Prefect-based data assets
"""
from typing import Callable, Dict, Any, List, Optional
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
import pandas as pd


class PrefectAssetBuilder:
    """
    Builder class for creating Prefect assets that mirror Dagster asset patterns
    """
    
    def __init__(self):
        self.assets = {}
    
    def create_data_asset(
        self,
        name: str,
        description: str,
        compute_fn: Callable,
        dependencies: List[str] = None,
        metadata: Dict[str, Any] = None
    ):
        """
        Create a Prefect 'asset' (flow that produces/manages data) similar to Dagster assets
        """
        if dependencies is None:
            dependencies = []
        
        if metadata is None:
            metadata = {}
        
        @flow(
            name=name,
            description=description
        )
        def asset_flow(**kwargs):
            """
            Generic asset flow that computes data and creates artifacts
            """
            # Execute the compute function
            result = compute_fn(**kwargs)
            
            # Create a markdown artifact to track the result (similar to Dagster's materialization)
            if isinstance(result, pd.DataFrame):
                summary = f"""
## Asset: {name}
- Shape: {result.shape}
- Columns: {list(result.columns)}
- Processed at: {pd.Timestamp.now()}
"""
            else:
                summary = f"""
## Asset: {name}
- Type: {type(result).__name__}
- Processed at: {pd.Timestamp.now()}
- Result: {str(result)[:200]}...
"""
            
            artifact_id = create_markdown_artifact(
                markdown=summary,
                description=f"Asset {name} result summary"
            )
            
            return {
                "result": result,
                "artifact_id": artifact_id,
                "processed_at": pd.Timestamp.now().isoformat()
            }
        
        # Register the asset
        self.assets[name] = {
            "flow": asset_flow,
            "description": description,
            "dependencies": dependencies,
            "metadata": metadata
        }
        
        return asset_flow
    
    def create_etl_asset(
        self,
        name: str,
        extract_fn: Callable,
        transform_fn: Callable,
        load_fn: Callable,
        dependencies: List[str] = None
    ):
        """
        Create an ETL asset flow that follows the extract-transform-load pattern
        """
        if dependencies is None:
            dependencies = []
        
        @flow(
            name=name,
            description=f"ETL process for {name}"
        )
        def etl_flow(**kwargs):
            # Extract
            extracted_data = extract_fn(**kwargs)
            
            # Transform
            transformed_data = transform_fn(extracted_data, **kwargs)
            
            # Load
            load_result = load_fn(transformed_data, **kwargs)
            
            # Create artifact
            summary = f"""
## ETL Asset: {name}
- Extract: {type(extracted_data).__name__}
- Transform: {type(transformed_data).__name__}
- Load: {load_result}
- Processed at: {pd.Timestamp.now()}
"""
            artifact_id = create_markdown_artifact(
                markdown=summary,
                description=f"ETL Asset {name} result"
            )
            
            return {
                "extracted": extracted_data,
                "transformed": transformed_data,
                "loaded": load_result,
                "artifact_id": artifact_id
            }
        
        self.assets[name] = {
            "flow": etl_flow,
            "description": f"ETL process for {name}",
            "dependencies": dependencies,
            "metadata": {"type": "etl"}
        }
        
        return etl_flow
    
    def get_asset(self, name: str):
        """Get a registered asset by name"""
        return self.assets.get(name)
    
    def get_all_assets(self):
        """Get all registered assets"""
        return self.assets


# Global instance for convenience
asset_builder = PrefectAssetBuilder()


def build_asset_from_config(asset_config: Dict[str, Any]):
    """
    Build an asset from configuration, similar to how Dagster builds assets
    """
    name = asset_config["name"]
    description = asset_config.get("description", "")
    dependencies = asset_config.get("dependencies", [])
    
    # This would dynamically create an asset based on the config
    # Similar to how Dagster creates assets from configuration
    
    def default_compute_fn(**kwargs):
        # Placeholder computation
        return {"computed": True, "config": asset_config, **kwargs}
    
    return asset_builder.create_data_asset(
        name=name,
        description=description,
        compute_fn=default_compute_fn,
        dependencies=dependencies
    )


if __name__ == "__main__":
    # Example usage
    def sample_compute_function(data_input=None, **kwargs):
        print(f"Computing asset with input: {data_input}, kwargs: {kwargs}")
        return {"processed": True, "input": data_input, "kwargs": kwargs}
    
    # Create an asset
    my_asset = asset_builder.create_data_asset(
        name="sample_data_asset",
        description="A sample data asset for demonstration",
        compute_fn=sample_compute_function
    )
    
    # Run the asset
    result = my_asset(data_input="test_data", extra_param="extra_value")
    print("Asset result:", result)