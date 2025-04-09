"""
Cassandra client for the Messenger application.
This provides a connection to the Cassandra database.
"""
import os
import uuid
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, dict_factory

logger = logging.getLogger(__name__)

class CassandraClient:
    """Singleton Cassandra client for the application."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CassandraClient, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the Cassandra connection."""
        if self._initialized:
            return
        
        self.host = os.getenv("CASSANDRA_HOST", "localhost")
        self.port = int(os.getenv("CASSANDRA_PORT", "9042"))
        self.keyspace = os.getenv("CASSANDRA_KEYSPACE", "messenger")
        
        self.cluster = None
        self.session = None
        self._initialized = True
    
    def connect(self) -> None:
        """Connect to the Cassandra cluster."""
        try:
            self.cluster = Cluster([self.host])
            # First connect without a keyspace to avoid NoHostAvailable error
            self.session = self.cluster.connect()
            
            # Check if keyspace exists, if not, create it
            keyspace_exists = False
            rows = self.session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
            keyspace_exists = any(row.keyspace_name == self.keyspace for row in rows)
            
            if not keyspace_exists:
                logger.info(f"Creating keyspace {self.keyspace}...")
                # Create keyspace if it doesn't exist
                self.session.execute(f"""
                    CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
                    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
                """)
                logger.info(f"Keyspace {self.keyspace} created.")
            
            # Connect to the keyspace after ensuring it exists
            self.session.set_keyspace(self.keyspace)
            self.session.row_factory = dict_factory
            logger.info(f"Connected to Cassandra at {self.host}:{self.port}, keyspace: {self.keyspace}")
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {str(e)}")
            raise
    
    def close(self) -> None:
        """Close the Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")
    
    def execute(self, query: str, params: dict = None) -> List[Dict[str, Any]]:
        """
        Execute a CQL query.
        
        Args:
            query: The CQL query string
            params: The parameters for the query
            
        Returns:
            List of rows as dictionaries
        """
        if not self.session:
            self.connect()
        
        try:
            statement = SimpleStatement(query)
            result = self.session.execute(statement, params or {})
            return list(result)
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def execute_async(self, query: str, params: dict = None):
        """
        Execute a CQL query asynchronously.
        
        Args:
            query: The CQL query string
            params: The parameters for the query
            
        Returns:
            Async result object
        """
        if not self.session:
            self.connect()
        
        try:
            statement = SimpleStatement(query)
            return self.session.execute_async(statement, params or {})
        except Exception as e:
            logger.error(f"Async query execution failed: {str(e)}")
            raise
    
    def get_session(self) -> Session:
        """Get the Cassandra session."""
        if not self.session:
            self.connect()
        return self.session

# Create a global instance
cassandra_client = CassandraClient()