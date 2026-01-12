"""
Prefect Resource Factory
=======================
Factory for creating Prefect resources with standardized configurations
"""
from typing import Dict, Any, Optional
from prefect.blocks.core import Block
from prefect.filesystems import LocalFileSystem, GitHub, S3
from prefect.infrastructure import Process, KubernetesJob, DockerContainer


class PrefectResourceFactory:
    """
    Factory for creating Prefect resources with standardized configurations
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the Prefect resource factory
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        
    def create_filesystem_block(self, fs_type: str, config: Dict[str, Any]) -> Block:
        """
        Create a filesystem block based on type
        
        Args:
            fs_type: Type of filesystem ('local', 'github', 's3')
            config: Configuration for the filesystem
            
        Returns:
            Prefect filesystem block
        """
        if fs_type.lower() == 'local':
            return LocalFileSystem(basepath=config.get('basepath', '.'))
        elif fs_type.lower() == 'github':
            return GitHub(
                repository=config['repository'],
                reference=config.get('reference', 'main')
            )
        elif fs_type.lower() == 's3':
            return S3(bucket_path=config['bucket_path'])
        else:
            raise ValueError(f"Unsupported filesystem type: {fs_type}")
    
    def create_infrastructure_block(self, infra_type: str, config: Dict[str, Any]) -> Block:
        """
        Create an infrastructure block based on type
        
        Args:
            infra_type: Type of infrastructure ('process', 'kubernetes', 'docker')
            config: Configuration for the infrastructure
            
        Returns:
            Prefect infrastructure block
        """
        if infra_type.lower() == 'process':
            return Process(env=config.get('env', {}))
        elif infra_type.lower() == 'kubernetes':
            return KubernetesJob(
                image=config.get('image'),
                namespace=config.get('namespace', 'default')
            )
        elif infra_type.lower() == 'docker':
            return DockerContainer(
                image=config.get('image', 'prefecthq/prefect:latest'),
                env=config.get('env', {})
            )
        else:
            raise ValueError(f"Unsupported infrastructure type: {infra_type}")
    
    def create_resource_from_config(self, resource_type: str, config: Dict[str, Any]) -> Block:
        """
        Create a resource block from configuration
        
        Args:
            resource_type: Type of resource
            config: Configuration for the resource
            
        Returns:
            Prefect resource block
        """
        if resource_type in ['filesystem', 'local', 'github', 's3']:
            return self.create_filesystem_block(resource_type, config)
        elif resource_type in ['infrastructure', 'process', 'kubernetes', 'docker']:
            return self.create_infrastructure_block(resource_type, config)
        else:
            raise ValueError(f"Unknown resource type: {resource_type}")