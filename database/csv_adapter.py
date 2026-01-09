"""CSV storage adapter using pandas."""
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
from database.adapter import DatabaseAdapter


class CSVAdapter(DatabaseAdapter):
    """CSV file storage adapter using pandas."""
    
    def __init__(self, storage_path: str = "storage"):
        """
        Initialize CSV adapter.
        
        Args:
            storage_path: Path to storage directory
        """
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(exist_ok=True)
        self.price_data_file = self.storage_path / "price_data.csv"
        self.indicators_file = self.storage_path / "indicators.csv"
    
    def connect(self):
        """No connection needed for CSV storage."""
        pass
    
    def disconnect(self):
        """No disconnection needed for CSV storage."""
        pass
    
    def insert_many(self, table: str, records: List[Dict[str, Any]]):
        """
        Insert multiple records into a CSV file.
        
        Args:
            table: Table name (price_data or indicators)
            records: List of records to insert
        """
        if not records:
            return
        
        df_new = pd.DataFrame(records)
        
        if table == "price_data":
            file_path = self.price_data_file
            if file_path.exists():
                df_existing = pd.read_csv(file_path)
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                df_combined = df_combined.drop_duplicates(subset=['ticker', 'date'], keep='last')
            else:
                df_combined = df_new
            df_combined.to_csv(file_path, index=False)
            
        elif table == "indicators":
            file_path = self.indicators_file
            if file_path.exists():
                df_existing = pd.read_csv(file_path)
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                df_combined = df_combined.drop_duplicates(subset=['ticker', 'date', 'name'], keep='last')
            else:
                df_combined = df_new
            df_combined.to_csv(file_path, index=False)
    
    def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL-like query (simplified for CSV).
        Note: This is a basic implementation. For complex queries, use DuckDB.
        """
        raise NotImplementedError("Use get_price_data or get_indicators methods for CSV adapter")
    
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None):
        """Execute SQL statement (not supported for CSV)."""
        raise NotImplementedError("CSV adapter does not support SQL execution")
    
    def get_price_data(self, ticker: str, start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get price data for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            List of price records
        """
        if not self.price_data_file.exists():
            return []
        
        df = pd.read_csv(self.price_data_file)
        df = df[df['ticker'] == ticker]
        
        if start_date:
            df = df[df['date'] >= start_date]
        if end_date:
            df = df[df['date'] <= end_date]
        
        df = df.sort_values('date')
        return df.to_dict('records')
    
    def get_indicators(self, ticker: str, indicator_name: str,
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get indicator data for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            indicator_name: Name of the indicator
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            List of indicator records
        """
        if not self.indicators_file.exists():
            return []
        
        df = pd.read_csv(self.indicators_file)
        df = df[(df['ticker'] == ticker) & (df['name'] == indicator_name)]
        
        if start_date:
            df = df[df['date'] >= start_date]
        if end_date:
            df = df[df['date'] <= end_date]
        
        df = df.sort_values('date')
        return df.to_dict('records')

