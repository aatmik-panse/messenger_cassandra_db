"""
Script to initialize Cassandra keyspace and tables for the Messenger application.
"""
import os
import time
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

def wait_for_cassandra():
    """Wait for Cassandra to be ready before proceeding."""
    logger.info("Waiting for Cassandra to be ready...")
    cluster = None
    
    for _ in range(10):  # Try 10 times
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()
            logger.info("Cassandra is ready!")
            return cluster
        except Exception as e:
            logger.warning(f"Cassandra not ready yet: {str(e)}")
            time.sleep(5)  # Wait 5 seconds before trying again
    
    logger.error("Failed to connect to Cassandra after multiple attempts.")
    raise Exception("Could not connect to Cassandra")

def create_keyspace(session):
    """
    Create the keyspace if it doesn't exist.
    
    Using NetworkTopologyStrategy with a replication factor of 3 for better
    distribution and fault tolerance in a multi-datacenter environment.
    """
    logger.info(f"Creating keyspace {CASSANDRA_KEYSPACE} if it doesn't exist...")
    
    # For development we can use SimpleStrategy with a replication factor of 1
    # In production, you might want to use NetworkTopologyStrategy with proper replication factor
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """ % CASSANDRA_KEYSPACE)
    
    logger.info(f"Keyspace {CASSANDRA_KEYSPACE} is ready.")

def create_tables(session):
    """
    Create the tables for the application.
    
    Creating tables optimized for the required query patterns:
    1. Sending messages between users
    2. Retrieving conversations for a user
    3. Retrieving messages in a conversation
    4. Retrieving messages before a specific timestamp
    """
    logger.info("Creating tables...")
    
    # Users table to store user information
    session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id int,
            username text,
            created_at timestamp,
            PRIMARY KEY (user_id)
        )
    """)
    
    # Messages table organized by conversation for efficient retrieval
    # Using conversation_id as partition key and timestamp as clustering column
    session.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            conversation_id int,
            timestamp timestamp,
            message_id uuid,
            sender_id int,
            receiver_id int,
            content text,
            PRIMARY KEY (conversation_id, timestamp, message_id)
        ) WITH CLUSTERING ORDER BY (timestamp DESC, message_id ASC)
    """)
    
    # Messages by user - for retrieving user's messages
    session.execute("""
        CREATE TABLE IF NOT EXISTS messages_by_user (
            user_id int,
            conversation_id int,
            timestamp timestamp,
            message_id uuid,
            sender_id int,
            receiver_id int,
            content text,
            PRIMARY KEY ((user_id), conversation_id, timestamp, message_id)
        ) WITH CLUSTERING ORDER BY (conversation_id ASC, timestamp DESC, message_id ASC)
    """)
    
    # Conversations for a user - for retrieving user's conversations sorted by last activity
    session.execute("""
        CREATE TABLE IF NOT EXISTS conversations_by_user (
            user_id int,
            conversation_id int,
            other_user_id int,
            last_message_at timestamp,
            last_message_content text,
            PRIMARY KEY (user_id, last_message_at, conversation_id)
        ) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC)
    """)
    
    # Conversations by ID - for directly accessing conversation details
    session.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            conversation_id int,
            user1_id int,
            user2_id int,
            created_at timestamp,
            last_message_at timestamp,
            last_message_content text,
            PRIMARY KEY (conversation_id)
        )
    """)
    
    logger.info("Tables created successfully.")

def main():
    """Initialize the database."""
    logger.info("Starting Cassandra initialization...")
    
    # Wait for Cassandra to be ready
    cluster = wait_for_cassandra()
    
    try:
        # Connect to the server without specifying keyspace
        session = cluster.connect()
        
        # Create keyspace
        create_keyspace(session)
        
        # Set keyspace after creation
        session.set_keyspace(CASSANDRA_KEYSPACE)
        
        # Create tables
        create_tables(session)
        
        logger.info("Cassandra initialization completed successfully.")
    except Exception as e:
        logger.error(f"Error during initialization: {str(e)}")
        raise
    finally:
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    main()