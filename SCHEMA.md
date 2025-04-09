# Cassandra Schema Design for FB Messenger

This document outlines the schema design for implementing a Facebook Messenger-like application using Apache Cassandra. The design prioritizes the query patterns required for a messaging application while following Cassandra data modeling best practices.

## Design Considerations

1. **Denormalization:** Data is duplicated across tables to optimize for read performance
2. **Query-first approach:** Tables are designed based on the specific query patterns needed
3. **Partition keys:** Chosen to ensure even distribution of data across the cluster
4. **Clustering columns:** Used to sort data within partitions for efficient retrieval

## Schema

### 1. Users Table

Stores basic user information.

```cql
CREATE TABLE users (
    user_id int,
    username text,
    created_at timestamp,
    PRIMARY KEY (user_id)
)
```

### 2. Messages Table

Stores all messages organized by conversation for efficient retrieval of conversation messages.

```cql
CREATE TABLE messages (
    conversation_id int,
    timestamp timestamp,
    message_id uuid,
    sender_id int,
    receiver_id int,
    content text,
    PRIMARY KEY (conversation_id, timestamp, message_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, message_id ASC)
```

- **Partition Key:** `conversation_id` - Groups all messages of a conversation together
- **Clustering Columns:**
  - `timestamp` (DESC) - Shows newest messages first
  - `message_id` - Ensures uniqueness when timestamps are identical

### 3. Messages By User Table

Enables retrieval of a user's messages across all conversations.

```cql
CREATE TABLE messages_by_user (
    user_id int,
    conversation_id int,
    timestamp timestamp,
    message_id uuid,
    sender_id int,
    receiver_id int,
    content text,
    PRIMARY KEY ((user_id), conversation_id, timestamp, message_id)
) WITH CLUSTERING ORDER BY (conversation_id ASC, timestamp DESC, message_id ASC)
```

- **Partition Key:** `user_id` - Groups all messages by a user
- **Clustering Columns:**
  - `conversation_id` - Groups messages by conversation
  - `timestamp` (DESC) - Shows newest messages first
  - `message_id` - Ensures uniqueness

### 4. Conversations By User Table

Enables efficient retrieval of a user's conversations, sorted by most recent activity.

```cql
CREATE TABLE conversations_by_user (
    user_id int,
    conversation_id int,
    other_user_id int,
    last_message_at timestamp,
    last_message_content text,
    PRIMARY KEY (user_id, last_message_at, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC)
```

- **Partition Key:** `user_id` - Groups all conversations of a user
- **Clustering Columns:**
  - `last_message_at` (DESC) - Shows conversations with recent activity first
  - `conversation_id` - Ensures uniqueness

### 5. Conversations Table

Stores conversation metadata for direct access by ID.

```cql
CREATE TABLE conversations (
    conversation_id int,
    user1_id int,
    user2_id int,
    created_at timestamp,
    last_message_at timestamp,
    last_message_content text,
    PRIMARY KEY (conversation_id)
)
```

## Query Patterns

### 1. Send Message Between Users

- Create or retrieve conversation ID between sender and recipient
- Insert into `messages` table
- Update `conversations_by_user` tables for both users
- Update `conversations` table

### 2. Fetch User Conversations

- Query `conversations_by_user` table with user_id
- Pagination via last_message_at and conversation_id

### 3. Fetch Messages in Conversation

- Query `messages` table with conversation_id
- Pagination via timestamp and message_id

### 4. Fetch Messages Before Timestamp

- Query `messages` table with conversation_id and timestamp < X
- Useful for "load more" functionality in chat UI

## Trade-offs and Optimizations

1. **Write Amplification:** Each message write requires multiple table updates (3-4 tables)
2. **Storage:** Data duplication increases storage needs but improves read performance
3. **Eventual Consistency:** Updates to different tables may not be instantly consistent
4. **Tombstones:** Message deletion requires careful handling to avoid tombstone buildup
