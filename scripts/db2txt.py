import os
import re
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

# Function to fetch posts and their comments from the database
def fetch_posts_with_comments(connection):
    posts = []
    try:
        with connection.cursor() as cursor:
            # Fetch posts
            cursor.execute("SELECT id, title, body, permalink FROM posts")
            posts_data = cursor.fetchall()

            for post in posts_data:
                post_id, title, body, permalink = post
                # Fetch comments for each post
                cursor.execute("""
                    SELECT body
                    FROM comments
                    WHERE post_id = %s
                    ORDER BY id
                """, (post_id,))
                comments = cursor.fetchall()
                posts.append((title, body, comments, permalink))
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return posts

# Function to extract subreddit name from permalink
def extract_subreddit(permalink):
    match = re.search(r"/r/([^/]+)/", permalink)
    return match.group(1) if match else "unknown_subreddit"

# Function to sanitize the title for use as a filename
def sanitize_filename(title):
    # Remove any characters that are not alphanumeric, spaces, or basic punctuation
    sanitized = re.sub(r'[^\w\s\-_.]', '', title)
    # Replace spaces with underscores
    sanitized = sanitized.replace(' ', '_')
    return sanitized

# Function to truncate the filename if it's too long
def truncate_filename(filename, max_length=50):
    if len(filename) > max_length:
        return filename[:max_length]
    return filename

# Function to write posts and comments to text files
def write_posts_to_files(posts, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for title, body, comments, permalink in posts:
        subreddit = extract_subreddit(permalink)
        subreddit_dir = os.path.join(output_dir, subreddit)

        if not os.path.exists(subreddit_dir):
            os.makedirs(subreddit_dir)

        # Sanitize the title to create a valid filename
        sanitized_title = sanitize_filename(title)
        truncated_title = truncate_filename(sanitized_title)
        file_path = os.path.join(subreddit_dir, f"{truncated_title}.txt")

        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(f"Title: {title}\n\n")
            file.write(f"Body: {body}\n\n")
            file.write("Comments:\n")
            for comment in comments:
                file.write(f"- {comment[0]}\n")

# Main function to execute the process
def main():
    db_name = "mydatabase"
    db_user = "myuser"
    db_password = "mypassword"
    db_host = "localhost"
    db_port = "5432"

    connection = create_connection(db_name, db_user, db_password, db_host, db_port)
    if connection is not None:
        posts = fetch_posts_with_comments(connection)
        write_posts_to_files(posts, "posts_output")
        connection.close()

if __name__ == "__main__":
    main()
