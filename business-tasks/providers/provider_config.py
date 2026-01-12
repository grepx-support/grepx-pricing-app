"""Provider configuration model."""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from enum import Enum


class ProviderType(Enum):
    """Available data provider types."""
    YAHOO = "yahoo"
    YAHOO_BALANCE_SHEET = "yahoo_for_balance_sheet"
    JUGAD = "jugad"
    CUSTOM = "custom"


@dataclass
class ProviderConfig:
    """Configuration for data providers."""
    provider_type: str = "yahoo"
    interval: str = "1d"
    extra_params: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate provider type."""
        valid_types = [p.value for p in ProviderType]
        if self.provider_type not in valid_types:
            raise ValueError(f"Invalid provider_type: {self.provider_type}. Must be one of {valid_types}")


@dataclass
class DataDownloadConfig:
    """Configuration for data download tasks."""
    provider: ProviderConfig = field(default_factory=ProviderConfig)
    default_period: str = "1y"
    default_tickers: list = field(default_factory=lambda: ['AAPL', 'MSFT', 'GOOG'])
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    batch_size: int = 10
    retry_attempts: int = 3
    timeout_seconds: int = 300

