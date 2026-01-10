"""DuckDB database adapter implementation."""
import duckdb
from typing import List, Dict, Any, Optional
from database.adapter import DatabaseAdapter


class DuckDBAdapter(DatabaseAdapter):
    """DuckDB database adapter for local storage."""
    
    def __init__(self, database_path: str = "pricing_data.duckdb"):
        """
        Initialize DuckDB adapter.
        
        Args:
            database_path: Path to DuckDB database file
        """
        self.database_path = database_path
        self.conn = None
    
    def connect(self):
        """Establish connection to DuckDB database."""
        self.conn = duckdb.connect(self.database_path)
        self._create_tables()
    
    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def _create_tables(self):
        """Create necessary tables if they don't exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS price_data (
                ticker VARCHAR,
                date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                adj_close DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, date)
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS indicators (
                ticker VARCHAR,
                date DATE,
                indicator_name VARCHAR,
                value DOUBLE,
                metadata JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, date, indicator_name)
            )
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_price_ticker_date 
            ON price_data(ticker, date)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_indicators_ticker_date_name 
            ON indicators(ticker, date, indicator_name)
        """)
    
    def insert_many(self, table: str, records: List[Dict[str, Any]]):
        """
        Insert multiple records into a table.
        
        Args:
            table: Table name
            records: List of records to insert
        """
        if not records:
            return
        
        try:
            if table == "price_data":
                self.conn.executemany("""
                    INSERT OR IGNORE INTO price_data 
                    (ticker, date, open, high, low, close, volume, adj_close)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    (r['ticker'], r['date'], r['open'], r['high'], 
                     r['low'], r['close'], r['volume'], r.get('adj_close', r['close']))
                    for r in records
                ])
            elif table == "indicators":
                self.conn.executemany("""
                    INSERT OR REPLACE INTO indicators 
                    (ticker, date, indicator_name, value, metadata)
                    VALUES (?, ?, ?, ?, ?)
                """, [
                    (r['ticker'], r['date'], r['name'], r['value'], 
                     str(r.get('metadata', {})))
                    for r in records
                ])
        except Exception as e:
            print(f"Error inserting records: {e}")
            raise
    
    def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as list of dictionaries.
        
        Args:
            sql: SQL query string
            params: Optional query parameters
            
        Returns:
            List of records as dictionaries
        """
        if params:
            result = self.conn.execute(sql, params).fetchdf()
        else:
            result = self.conn.execute(sql).fetchdf()
        
        return result.to_dict('records')
    
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None):
        """
        Execute a SQL statement without returning results.
        
        Args:
            sql: SQL statement
            params: Optional statement parameters
        """
        if params:
            self.conn.execute(sql, params)
        else:
            self.conn.execute(sql)
    
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
        sql = "SELECT * FROM price_data WHERE ticker = ?"
        params = [ticker]
        
        if start_date:
            sql += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            sql += " AND date <= ?"
            params.append(end_date)
        
        sql += " ORDER BY date ASC"
        
        result = self.conn.execute(sql, params).fetchdf()
        return result.to_dict('records')
    
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
        sql = "SELECT * FROM indicators WHERE ticker = ? AND indicator_name = ?"
        params = [ticker, indicator_name]
        
        if start_date:
            sql += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            sql += " AND date <= ?"
            params.append(end_date)
        
        sql += " ORDER BY date ASC"
        
        result = self.conn.execute(sql, params).fetchdf()
        return result.to_dict('records')

