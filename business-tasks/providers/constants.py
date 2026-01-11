"""Constants for provider configuration."""
from enum import Enum


class Providers(Enum):
    """Available data provider types."""
    YAHOO = "yahoo"
    YAHOO_FOR_BALANCE_SHEET = "yahoo_for_balance_sheet"
    YAHOO_FOR_CASH_FLOW = "yahoo_for_cash_flow"
    JUGAD = "jugad"
    CUSTOM = "custom"

