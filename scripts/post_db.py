import re
import json
import psycopg2
from psycopg2 import OperationalError

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

        # Extract subreddit name from permalink
        subreddit = extract_subreddit(post['permalink'])

        # Check if post already exists
        cursor.execute("SELECT id FROM posts WHERE permalink = %s", (post['permalink'],))
        post_id = cursor.fetchone()

        if post_id is None:
            # Insert post
            cursor.execute("""
            INSERT INTO posts (title, author, created_utc, num_comments, score, upvote_ratio, permalink, url, image_url, thumbnail_url, selftext, body, scraped_at, subreddit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """, (
                post['title'],
                post['author'],
                post['created_utc'],
                post['num_comments'],
                post['score'],
                post['upvote_ratio'],
                post['permalink'],
                post.get('url', ''),
                post.get('image_url', ''),
                post.get('thumbnail_url', ''),
                post.get('selftext', ''),
                post.get('body', ''),
                post['scraped_at'],
                subreddit
            ))

            post_id = cursor.fetchone()[0]

            # Function to insert comments recursively
            def insert_comments(comment, parent_id=None):
                if comment['body'] not in ('[removed]', '[deleted]'):
                    cursor.execute("""
                    INSERT INTO comments (post_id, author, body, score, parent_comment_id)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                    """, (
                        post_id,
                        comment['author'],
                        comment['body'],
                        comment['score'],
                        parent_id
                    ))
                    comment_id = cursor.fetchone()[0]

                    # Insert replies recursively
                    for reply in comment.get('replies', []):
                        insert_comments(reply, comment_id)

            # Insert comments
            for comment in post['comments']:
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
            with open(json_file, 'r', encoding='utf-8') as file:
                data = json.load(file)
                for post in data:
                    insert_post(connection, post)

        connection.close()

# Example usage
json_files = ['data.json', 'tech_data.json']  # Add your JSON file paths here
process_json_files(
    db_name="mydatabase",
    db_user="myuser",
    db_password="mypassword",
    db_host="localhost",
    db_port="5432",
    json_files=json_files
)
