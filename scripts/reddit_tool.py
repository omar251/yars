import json
import os
import sys
import argparse

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
src_path = os.path.join(project_root, "src")
sys.path.append(src_path)

from yars.yars import YARS
from yars.utils import display_results, download_image

# Initialize the YARS Reddit miner
miner = YARS()

# Function to display search results and fetch posts from the subreddits in the search results
def display_and_fetch_posts(miner, search_query, limit=5):
    search_results = miner.search_reddit(search_query, limit=limit)
    display_results(search_results, "SEARCH")

    subreddit_posts = []
    for result in search_results:
        subreddit_name = result["link"].split("/r/")[1].split("/")[0]
        posts = miner.fetch_subreddit_posts(
            subreddit_name, limit=limit, category="new", time_filter="week"
        )
        subreddit_posts.extend(posts)

    display_results(subreddit_posts, "SUBREDDIT Top Posts")

    # Attempt to download images from the first few posts
    for idx, post in enumerate(subreddit_posts[:3]):
        try:
            image_url = post.get("image_url", post.get("thumbnail_url", ""))
            if image_url:
                download_image(image_url)
        except Exception as e:
            print(f"Error downloading image from post {idx}: {e}")

    return subreddit_posts

# Function to scrape post details and comments and save to JSON
def scrape_and_save_posts(posts, filename="subreddit_data.json"):
    try:
        # Load existing data from the JSON file, if available
        try:
            with open(filename, "r") as json_file:
                existing_data = json.load(json_file)
        except (FileNotFoundError, json.JSONDecodeError):
            existing_data = []

        # Scrape details and comments for each post
        for i, post in enumerate(posts, 1):
            permalink = post["permalink"]
            post_details = miner.scrape_post_details(permalink)
            print(f"Processing post {i}")

            if post_details:
                post_data = {
                    "title": post.get("title", ""),
                    "author": post.get("author", ""),
                    "created_utc": post.get("created_utc", ""),
                    "num_comments": post.get("num_comments", 0),
                    "score": post.get("score", 0),
                    "permalink": post.get("permalink", ""),
                    "image_url": post.get("image_url", ""),
                    "thumbnail_url": post.get("thumbnail_url", ""),
                    "body": post_details.get("body", ""),
                    "comments": post_details.get("comments", []),
                }

                # Append new post data to existing data
                existing_data.append(post_data)

                # Save the data incrementally to the JSON file
                save_to_json(existing_data, filename)
            else:
                print(f"Failed to scrape details for post: {post['title']}")

    except Exception as e:
        print(f"Error occurred while scraping posts: {e}")

# Function to save post data to a JSON file
def save_to_json(data, filename="subreddit_data.json"):
    try:
        with open(filename, "w") as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Data successfully saved to {filename}")
    except Exception as e:
        print(f"Error saving data to JSON file: {e}")

# Main execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reddit Scraper Tool")
    parser.add_argument("query", help="The search query to use")
    parser.add_argument("--limit", type=int, default=5, help="The number of posts to retrieve")
    parser.add_argument("--filename", default="subreddit_data.json", help="The name of the output JSON file")

    args = parser.parse_args()

    search_query = args.query
    limit = args.limit
    filename = args.filename

    # Display search results and fetch posts from the subreddits in the search results
    posts = display_and_fetch_posts(miner, search_query, limit)

    # Scrape and save post data to JSON
    scrape_and_save_posts(posts, filename)
