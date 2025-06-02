#!/bin/bash
set -e  # Exit immediately if any command exits with a non-zero status

# Run the first command with cli.py and wait until it is finished.
# uv run scripts/cli.py --subreddit AlexandriaEgy --search-term "confessions" --username techuser --limit 30 --output masr.json --action both
read -p "Enter the search term: " search_term
uv run scripts/reddit_tool.py "$search_term" --limit 3 --filename data.json
# Run the second command with post_db.py and wait until it is finished.
uv run scripts/post_db.py

# Run the third command with db2txt.py and wait until it is finished.
uv run scripts/db2txt.py

# Run the fourth command with indexer.py and wait until it is finished.
uv run scripts/indexer.py
