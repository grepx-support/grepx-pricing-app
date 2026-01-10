"""MongoDB adapter implementation (optional)."""
from typing import List, Dict, Any, Optional
from pymongo import MongoClient, ASCENDING
from database.adapter import DatabaseAdapter


class MongoAdapter(DatabaseAdapter):
    """MongoDB database adapter."""
    
    def __init__(self, connection_string: str, database_name: str):
        """
        Initialize MongoDB adapter.
        
        Args:
            connection_string: MongoDB connection string
            database_name: Database name
        """
        self.connection_string = connection_string
        self.database_name = database_name
        self.client = None
        self.db = None
    
    def connect(self):
        """Establish connection to MongoDB."""
        self.client = MongoClient(self.connection_string)
        self.db = self.client[self.database_name]
        
        self.db.price_data.create_index(
            [("ticker", ASCENDING), ("date", ASCENDING)], 
            unique=True
        )
        self.db.indicators.create_index(
            [("ticker", ASCENDING), ("date", ASCENDING), ("name", ASCENDING)]
        )
    
    def disconnect(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
    
    def insert_many(self, table: str, records: List[Dict[str, Any]]):
        """
        Insert multiple records into a collection.
        
        Args:
            table: Collection name
            records: List of records to insert
        """
        if not records:
            return
        
        try:
            self.db[table].insert_many(records, ordered=False)
        except Exception:
            pass
    
    def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        MongoDB doesn't support SQL queries directly.
        This method is not fully implemented for MongoDB.
        Use specific methods instead.
        """
        raise NotImplementedError("Use specific query methods for MongoDB")
    
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None):
        """
        MongoDB doesn't support SQL execution.
        This method is not implemented for MongoDB.
        """
        raise NotImplementedError("Use specific methods for MongoDB")
    
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
        query = {'ticker': ticker}
        
        if start_date or end_date:
            date_query = {}
            if start_date:
                date_query['$gte'] = start_date
            if end_date:
                date_query['$lte'] = end_date
            query['date'] = date_query
        
        cursor = self.db.price_data.find(query).sort('date', ASCENDING)
        return list(cursor)
    
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
        query = {'ticker': ticker, 'name': indicator_name}
        
        if start_date or end_date:
            date_query = {}
            if start_date:
                date_query['$gte'] = start_date
            if end_date:
                date_query['$lte'] = end_date
            query['date'] = date_query
        
        cursor = self.db.indicators.find(query).sort('date', ASCENDING)
        return list(cursor)

