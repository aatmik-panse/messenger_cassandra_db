"""
Script to generate test data for the Messenger application.
"""
import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10  # Number of users to create
NUM_CONVERSATIONS = 15  # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def tables_exist(session):
    """Check if required tables exist in the keyspace."""
    logger.info("Checking if required tables exist...")
    
    required_tables = ['users', 'messages', 'messages_by_user', 'conversations', 'conversations_by_user']
    
    keyspace_metadata = session.cluster.metadata.keyspaces[CASSANDRA_KEYSPACE]
    existing_tables = keyspace_metadata.tables.keys()
    
    missing_tables = [table for table in required_tables if table not in existing_tables]
    
    if missing_tables:
        logger.info(f"Missing tables: {', '.join(missing_tables)}")
        return False
    
    logger.info("All required tables exist.")
    return True

def create_tables(session):
    """Create the necessary tables for the application."""
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

def generate_test_data(session):
    """
    Generate test data in Cassandra.
    """
    logger.info("Generating test data...")
    
    # 1. Create users
    logger.info("Creating users...")
    for user_id in range(1, NUM_USERS + 1):
        username = f"user{user_id}"
        created_at = datetime.utcnow() - timedelta(days=random.randint(1, 30))
        
        session.execute(
            """
            INSERT INTO users (user_id, username, created_at)
            VALUES (%s, %s, %s)
            """,
            (user_id, username, created_at)
        )
    
    # 2. Create conversations between random pairs of users
    logger.info("Creating conversations...")
    conversations = []
    
    for conv_id in range(1, NUM_CONVERSATIONS + 1):
        # Pick two different random users
        user_ids = random.sample(range(1, NUM_USERS + 1), 2)
        user1_id, user2_id = user_ids
        
        created_at = datetime.utcnow() - timedelta(days=random.randint(1, 20))
        last_message_at = created_at
        last_message_content = None
        
        # Insert into conversations table
        session.execute(
            """
            INSERT INTO conversations 
            (conversation_id, user1_id, user2_id, created_at, last_message_at, last_message_content)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (conv_id, user1_id, user2_id, created_at, last_message_at, last_message_content)
        )
        
        conversations.append((conv_id, user1_id, user2_id, created_at))
    
    # 3. Generate messages for each conversation
    logger.info("Creating messages...")
    
    message_contents = [
        "Hey, how are you?", 
        "What's up?", 
        "Can we meet tomorrow?",
        "I'm busy right now",
        "Let's catch up soon",
        "Did you see that movie?",
        "Have you done the assignment?",
        "I'll call you later",
        "Thanks for your help!",
        "Congratulations!"
    ]
    
    for conv_id, user1_id, user2_id, created_at in conversations:
        # Determine how many messages to create for this conversation
        num_messages = random.randint(5, MAX_MESSAGES_PER_CONVERSATION)
        
        # Generate messages with increasing timestamps starting from conversation creation
        current_time = created_at
        latest_content = None
        
        for i in range(num_messages):
            # Increment time randomly between messages (1 min to 1 hour)
            current_time += timedelta(minutes=random.randint(1, 60))
            
            # Alternate sender between the two users
            sender_id = user1_id if i % 2 == 0 else user2_id
            receiver_id = user2_id if i % 2 == 0 else user1_id
            
            # Generate message content
            content = random.choice(message_contents)
            latest_content = content
            
            # Create unique message ID
            message_id = uuid.uuid4()
            
            # Insert into messages table
            session.execute(
                """
                INSERT INTO messages 
                (conversation_id, timestamp, message_id, sender_id, receiver_id, content)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (conv_id, current_time, message_id, sender_id, receiver_id, content)
            )
            
            # Insert into messages_by_user for both users
            for user_id in [sender_id, receiver_id]:
                session.execute(
                    """
                    INSERT INTO messages_by_user 
                    (user_id, conversation_id, timestamp, message_id, sender_id, receiver_id, content)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (user_id, conv_id, current_time, message_id, sender_id, receiver_id, content)
                )
        
        # Update last message timestamp and content in conversations table
        session.execute(
            """
            UPDATE conversations 
            SET last_message_at = %s, last_message_content = %s
            WHERE conversation_id = %s
            """,
            (current_time, latest_content, conv_id)
        )
        
        # Update conversations_by_user for both users
        for user_id in [user1_id, user2_id]:
            other_user_id = user2_id if user_id == user1_id else user1_id
            session.execute(
                """
                INSERT INTO conversations_by_user 
                (user_id, conversation_id, other_user_id, last_message_at, last_message_content)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (user_id, conv_id, other_user_id, current_time, latest_content)
            )
    
    logger.info(f"Generated {NUM_CONVERSATIONS} conversations with messages")
    logger.info(f"User IDs range from 1 to {NUM_USERS}")
    logger.info("Use these IDs for testing the API endpoints")

def main():
    """Main function to generate test data."""
    cluster = None
    
    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()
        
        # Check if tables exist, create them if they don't
        if not tables_exist(session):
            create_tables(session)
        
        # Generate test data
        generate_test_data(session)
        
        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main()