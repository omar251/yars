import re
import json
import psycopg2
from psycopg2 import OperationalError
from datetime import datetime

# Function to create a database connection
def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

# Function to create tables
def create_tables(connection):
    commands = (
        """
        CREATE TABLE IF NOT EXISTS posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            author TEXT,
            created_utc FLOAT,
            num_comments INTEGER,
            score INTEGER,
            upvote_ratio FLOAT,
            permalink TEXT UNIQUE,
            url TEXT,
            image_url TEXT,
            thumbnail_url TEXT,
            selftext TEXT,
            body TEXT,
            scraped_at TIMESTAMP,
            subreddit TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS comments (
            id SERIAL PRIMARY KEY,
            post_id INTEGER REFERENCES posts(id),
            author TEXT,
            body TEXT,
            score INTEGER,
            parent_comment_id INTEGER REFERENCES comments(id)
        )
        """
    )
    try:
        cursor = connection.cursor()
        for command in commands:
            cursor.execute(command)
        cursor.close()
        connection.commit()
    except OperationalError as e:
        print(f"The error '{e}' occurred")

# Function to extract subreddit name from permalink
def extract_subreddit(permalink):
    match = re.search(r"/r/([^/]+)/", permalink)
    return match.group(1) if match else None

# Function to insert a post and its comments
def insert_post(connection, post):
    try:
        cursor = connection.cursor()

        # Debugging: Print the type and content of the post
        # print(f"Processing post: {post}")
        # print(f"Type of post: {type(post)}")

        if not isinstance(post, dict):
            # print(f"Skipping invalid post: {post}")
            return

        # Extract subreddit name from permalink
        subreddit = extract_subreddit(post.get('permalink', ''))

        # Check if post already exists
        cursor.execute("SELECT id FROM posts WHERE permalink = %s", (post.get('permalink', ''),))
        post_id = cursor.fetchone()

        if post_id is None:
            # Insert post
            cursor.execute("""
            INSERT INTO posts (title, author, created_utc, num_comments, score, upvote_ratio, permalink, url, image_url, thumbnail_url, selftext, body, scraped_at, subreddit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """, (
                post.get('title', ''),
                post.get('author', ''),
                post.get('created_utc', 0.0),
                post.get('num_comments', 0),
                post.get('score', 0),
                post.get('upvote_ratio', 0.0),
                post.get('permalink', ''),
                post.get('url', ''),
                post.get('image_url', ''),
                post.get('thumbnail_url', ''),
                post.get('selftext', ''),
                post.get('body', ''),
                post.get('scraped_at', datetime.now()),  # Use current timestamp if scraped_at is missing
                subreddit
            ))

            post_id = cursor.fetchone()[0]

            # Function to insert comments recursively
            def insert_comments(comment, parent_id=None):
                if comment.get('body', '') not in ('[removed]', '[deleted]'):
                    cursor.execute("""
                    INSERT INTO comments (post_id, author, body, score, parent_comment_id)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                    """, (
                        post_id,
                        comment.get('author', ''),
                        comment.get('body', ''),
                        comment.get('score', 0),
                        parent_id
                    ))
                    comment_id = cursor.fetchone()[0]

                    # Insert replies recursively
                    for reply in comment.get('replies', []):
                        insert_comments(reply, comment_id)

            # Insert comments
            for comment in post.get('comments', []):
                insert_comments(comment)

        connection.commit()
        cursor.close()
    except OperationalError as e:
        print(f"The error '{e}' occurred")

# Main function to process JSON files
def process_json_files(db_name, db_user, db_password, db_host, db_port, json_files):
    connection = create_connection(db_name, db_user, db_password, db_host, db_port)
    if connection is not None:
        create_tables(connection)

        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    for post in data:
                        insert_post(connection, post)
            except FileNotFoundError:
                print(f"File not found: {json_file}")
            except json.JSONDecodeError:
                print(f"Error decoding JSON from file: {json_file}")

        connection.close()

# Example usage
# json_files = ['data.json', 'tech_data.json']  # Add your JSON file paths here
import os
json_files = [f for f in os.listdir('.') if f.endswith('.json')]
process_json_files(
    db_name="mydatabase",
    db_user="myuser",
    db_password="mypassword",
    db_host="localhost",
    db_port="5432",
    json_files=json_files
)
