"""
Configuration Loader for Prefect Framework
"""
import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional


class ConfigLoader:
    """Loads configuration from YAML file"""

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize config loader

        Args:
            config_path: Path to config.yaml file. Defaults to resources/config.yaml
        """
        if config_path is None:
            config_path = Path(__file__).parent / "resources" / "config.yaml"
        self.config_path = Path(config_path)

    def load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file with environment variable substitution

        Returns:
            Configuration dictionary
        """
        if not self.config_path.exists():
            return self._get_default_config()

        with open(self.config_path, 'r') as f:
            content = f.read()

        # Simple environment variable substitution: ${VAR:default}
        content = self._substitute_env_vars(content)

        config = yaml.safe_load(content) or {}
        return config

    def _substitute_env_vars(self, content: str) -> str:
        """Substitute environment variables in format ${VAR} or ${VAR:default}"""
        import re

        def replace_var(match):
            var_expr = match.group(1)
            # Find the first colon that is NOT part of a drive letter path (like C:/)
            # Look for colon that is NOT preceded by a letter and followed by /
            pos = -1
            for i, char in enumerate(var_expr):
                if char == ':':
                    # Check if this is a drive letter pattern (X:/)
                    if i > 0 and i < len(var_expr) - 1 and var_expr[i+1] == '/' and var_expr[i-1].isalpha():
                        continue  # Skip this colon, it's part of a path
                    else:
                        pos = i
                        break
            
            if pos != -1:
                var_name = var_expr[:pos]
                default = var_expr[pos+1:]
                value = os.getenv(var_name, default)
            else:
                value = os.getenv(var_expr)
                if value is None:
                    raise ValueError(f"Environment variable {var_expr} not set. Ensure env.common is loaded.")
            
            # Escape special YAML characters in the value to prevent parsing issues
            # We need to ensure the result is still valid YAML
            if value is None:
                return ''
            
            # Return the value as a properly quoted string for YAML
            # If the value contains special characters, wrap it in quotes
            value_str = str(value)
                        
            if any(c in value_str for c in [' ', ':', '#', '[', ']', '{', '}', ',', '&', '*', '!', '|', '>', '%', '@', '`']):
                # Use double quotes and escape any existing quotes
                value_str = value_str.replace('"', '\"')
                return f'"{value_str}"'
            return value_str

        pattern = r'\$\{([^}]+)\}'
        return re.sub(pattern, replace_var, content)

    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration if config file doesn't exist"""
        return {
            'app': {
                'name': 'grepx-prefect-server',
                'version': '1.0.0'
            },
            'prefect': {
                'api_url': 'http://127.0.0.1:4200/api',
                'home': './.prefect'
            },
            'database': {
                'db_url': 'sqlite:///./prefect_config.db'
                
            },
            'celery': {
                'broker_url': 'redis://localhost:6379/0',
                'result_backend': 'redis://localhost:6379/0'
            }
        }

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation (e.g., 'prefect.home')

        Args:
            key_path: Dot-separated path to config value
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        config = self.load_config()
        keys = key_path.split('.')
        value = config

        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default

        return value
