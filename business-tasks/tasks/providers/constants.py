"""Constants for provider configuration."""
from enum import Enum


class Providers(Enum):
    """Available data provider types."""
    YAHOO = "yahoo"
    YAHOO_FOR_BALANCE_SHEET = "yahoo_for_balance_sheet"
    JUGAD = "jugad"
    CUSTOM = "custom"

