#!/usr/bin/env python3
"""
Enhanced Reddit Scraper with improved error handling, logging, and configuration
"""

import json
import os
import sys
import argparse
import logging
from urllib.parse import urlparse
from time import sleep
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reddit_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Add project paths
try:
    current_dir = Path(__file__).parent.absolute()
except NameError:
    current_dir = Path.cwd()

project_root = current_dir.parent
src_path = project_root / "src"
sys.path.append(str(src_path))

try:
    from yars.yars import YARS
    from yars.utils import display_results, download_image
except ImportError as e:
    logger.error(f"Failed to import YARS modules: {e}")
    sys.exit(1)

@dataclass
class ScrapingConfig:
    """Configuration class for scraping parameters"""
    subreddit: str
    search_term: str
    username: str
    limit: int
    output_file: str
    action: str
    rate_limit_delay: float = 1.0
    max_retries: int = 3
    download_images: bool = True
    backup_enabled: bool = True

class RedditScraper:
    """Enhanced Reddit Scraper with improved functionality"""

    def __init__(self, config: ScrapingConfig):
        self.config = config
        self.miner = YARS()
        self.scraped_count = 0
        self.error_count = 0

    def _safe_execute(self, func, *args, **kwargs) -> Optional[Any]:
        """Execute function with retry logic and error handling"""
        for attempt in range(self.config.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < self.config.max_retries - 1:
                    sleep(self.config.rate_limit_delay * (attempt + 1))
                else:
                    logger.error(f"All attempts failed for {func.__name__}")
                    self.error_count += 1
        return None

    def search_reddit_posts(self, search_term: str, limit: int) -> Optional[List[Dict]]:
        """Search Reddit with error handling"""
        logger.info(f"Searching Reddit for '{search_term}' (limit: {limit})")

        def _search():
            return self.miner.search_reddit(search_term, limit=limit)

        results = self._safe_execute(_search)
        if results:
            logger.info(f"Found {len(results)} search results")
            display_results(results, "SEARCH RESULTS")
        return results

    def scrape_post_details(self, permalink: str) -> Optional[Dict]:
        """Scrape detailed post information"""
        logger.info(f"Scraping post details for: {permalink}")

        def _scrape_post():
            return self.miner.scrape_post_details(permalink)

        details = self._safe_execute(_scrape_post)
        if details:
            logger.info("Successfully scraped post details")
            display_results(details, "POST DETAILS")
        return details

    def scrape_user_data(self, username: str, limit: int) -> Optional[List[Dict]]:
        """Scrape user data with validation"""
        logger.info(f"Scraping data for user '{username}' (limit: {limit})")

        def _scrape_user():
            return self.miner.scrape_user_data(username, limit=limit)

        user_data = self._safe_execute(_scrape_user)
        if user_data:
            logger.info(f"Successfully scraped {len(user_data)} user posts")
            display_results(user_data, f"USER DATA - {username}")
        return user_data

    def fetch_subreddit_posts(self, subreddit: str, limit: int,
                            category: str = "new", time_filter: str = "week") -> Optional[List[Dict]]:
        """Fetch subreddit posts with enhanced filtering"""
        logger.info(f"Fetching {category} posts from r/{subreddit} (limit: {limit}, time: {time_filter})")

        def _fetch_posts():
            return self.miner.fetch_subreddit_posts(
                subreddit, limit=limit, category=category, time_filter=time_filter
            )

        posts = self._safe_execute(_fetch_posts)
        if posts:
            logger.info(f"Successfully fetched {len(posts)} posts from r/{subreddit}")
            display_results(posts, f"SUBREDDIT r/{subreddit} - {category.upper()} Posts")
        return posts

    def download_post_images(self, posts: List[Dict], max_images: int = 5) -> None:
        """Download images from posts with validation"""
        if not self.config.download_images:
            logger.info("Image downloading is disabled")
            return

        logger.info(f"Attempting to download images from {min(len(posts), max_images)} posts")
        downloaded_count = 0

        for idx, post in enumerate(posts[:max_images]):
            if downloaded_count >= max_images:
                break

            image_url = post.get("image_url") or post.get("thumbnail_url", "")

            if not image_url:
                logger.debug(f"No image URL in post {idx + 1}")
                continue

            if not self._is_valid_image_url(image_url):
                logger.debug(f"Invalid image URL in post {idx + 1}: {image_url}")
                continue

            try:
                download_image(image_url)
                downloaded_count += 1
                logger.info(f"Downloaded image {downloaded_count} from post {idx + 1}")
                sleep(self.config.rate_limit_delay)
            except Exception as e:
                logger.error(f"Failed to download image from post {idx + 1}: {e}")

    def _is_valid_image_url(self, url: str) -> bool:
        """Validate image URL"""
        try:
            parsed = urlparse(url)
            return (
                parsed.scheme in ["http", "https"] and
                any(url.lower().endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"])
            )
        except Exception:
            return False

    def comprehensive_scrape(self, subreddit: str, limit: int, output_file: str) -> None:
        """Perform comprehensive subreddit scraping"""
        logger.info(f"Starting comprehensive scrape of r/{subreddit}")

        # Backup existing data
        if self.config.backup_enabled:
            self._backup_existing_file(output_file)

        # Load existing data
        existing_data = self._load_existing_data(output_file)
        logger.info(f"Loaded {len(existing_data)} existing records")

        # Fetch posts
        posts = self.fetch_subreddit_posts(subreddit, limit, "top", "all")
        if not posts:
            logger.error("Failed to fetch posts for comprehensive scraping")
            return

        new_data = []
        existing_permalinks = {item.get("permalink") for item in existing_data}

        for i, post in enumerate(posts, 1):
            permalink = post.get("permalink", "")

            # Skip if already scraped
            if permalink in existing_permalinks:
                logger.info(f"Skipping already scraped post {i}: {post.get('title', 'Unknown')}")
                continue

            logger.info(f"Processing post {i}/{len(posts)}: {post.get('title', 'Unknown')}")

            # Scrape detailed post information
            post_details = self.scrape_post_details(permalink)

            if post_details:
                post_data = self._create_post_record(post, post_details)
                new_data.append(post_data)
                self.scraped_count += 1
                logger.info(f"Successfully processed post {i}")
            else:
                logger.warning(f"Failed to scrape details for post {i}")

            sleep(self.config.rate_limit_delay)

        # Combine and save data
        all_data = existing_data + new_data
        self._save_to_json(all_data, output_file)

        logger.info(f"Comprehensive scraping completed. New posts: {len(new_data)}, Total: {len(all_data)}")

    def _create_post_record(self, post: Dict, post_details: Dict) -> Dict:
        """Create a standardized post record"""
        return {
            "title": post.get("title", ""),
            "author": post.get("author", ""),
            "created_utc": post.get("created_utc", ""),
            "num_comments": post.get("num_comments", 0),
            "score": post.get("score", 0),
            "upvote_ratio": post.get("upvote_ratio", 0.0),
            "permalink": post.get("permalink", ""),
            "url": post.get("url", ""),
            "image_url": post.get("image_url", ""),
            "thumbnail_url": post.get("thumbnail_url", ""),
            "selftext": post.get("selftext", ""),
            "body": post_details.get("body", ""),
            "comments": post_details.get("comments", []),
            "scraped_at": self._get_current_timestamp()
        }

    def _backup_existing_file(self, filename: str) -> None:
        """Create backup of existing file"""
        file_path = Path(filename)
        if file_path.exists():
            backup_path = file_path.with_suffix(f".backup{file_path.suffix}")
            try:
                file_path.rename(backup_path)
                logger.info(f"Created backup: {backup_path}")
            except Exception as e:
                logger.warning(f"Failed to create backup: {e}")

    def _load_existing_data(self, filename: str) -> List[Dict]:
        """Load existing JSON data"""
        try:
            with open(filename, "r", encoding="utf-8") as file:
                return json.load(file)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.info(f"No existing data found or invalid JSON: {e}")
            return []

    def _save_to_json(self, data: List[Dict], filename: str) -> None:
        """Save data to JSON file with error handling"""
        try:
            with open(filename, "w", encoding="utf-8") as file:
                json.dump(data, file, indent=2, ensure_ascii=False)
            logger.info(f"Successfully saved {len(data)} records to {filename}")
        except Exception as e:
            logger.error(f"Failed to save data to {filename}: {e}")
            raise

    def _get_current_timestamp(self) -> str:
        """Get current timestamp string"""
        from datetime import datetime
        return datetime.now().isoformat()

    def display_demo_data(self) -> None:
        """Display demonstration of various scraping capabilities"""
        logger.info("Running demonstration mode")

        # Search Reddit
        self.search_reddit_posts(self.config.search_term, self.config.limit)

        # Scrape specific post (example)
        demo_permalink = "/r/getdisciplined/comments/1frb5ib/what_single_health_test_or_practice_has/"
        self.scrape_post_details(demo_permalink)

        # Scrape user data
        self.scrape_user_data(self.config.username, self.config.limit)

        # Fetch and display subreddit posts
        posts = self.fetch_subreddit_posts(self.config.subreddit, self.config.limit)
        if posts:
            self.download_post_images(posts, 3)

    def get_statistics(self) -> Dict[str, int]:
        """Get scraping statistics"""
        return {
            "scraped_count": self.scraped_count,
            "error_count": self.error_count
        }

def create_config_from_args(args) -> ScrapingConfig:
    """Create configuration from command line arguments"""
    return ScrapingConfig(
        subreddit=args.subreddit,
        search_term=args.search_term,
        username=args.username,
        limit=args.limit,
        output_file=args.output,
        action=args.action,
        rate_limit_delay=args.delay,
        max_retries=args.retries,
        download_images=args.download_images,
        backup_enabled=args.backup
    )

def main():
    """Main function with enhanced argument parsing"""
    parser = argparse.ArgumentParser(
        description="Enhanced Reddit Scraper with improved functionality",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --subreddit python --search-term "machine learning" --username someuser --limit 10 --output python_data.json --action scrape
  %(prog)s --subreddit askreddit --search-term "life advice" --username anotheruser --limit 5 --output askreddit_data.json --action display
  %(prog)s --subreddit technology --search-term "AI" --username techuser --limit 20 --output tech_data.json --action both
        """
    )

    # Required arguments
    parser.add_argument("--subreddit", required=True,
                       help="Subreddit to scrape (required)")
    parser.add_argument("--search-term", required=True,
                       help="Term to search on Reddit (required)")
    parser.add_argument("--username", required=True,
                       help="Username to scrape data for (required)")
    parser.add_argument("--limit", type=int, required=True,
                       help="Number of posts to fetch (required)")
    parser.add_argument("--output", required=True,
                       help="Output JSON file name (required)")

    # Action options
    parser.add_argument("--action", choices=["display", "scrape", "both"], required=True,
                       help="Action to perform (required)")

    # Advanced options
    parser.add_argument("--delay", type=float, default=1.0,
                       help="Rate limit delay in seconds (default: 1.0)")
    parser.add_argument("--retries", type=int, default=3,
                       help="Maximum retry attempts (default: 3)")
    parser.add_argument("--no-images", dest="download_images", action="store_false",
                       help="Disable image downloading")
    parser.add_argument("--no-backup", dest="backup", action="store_false",
                       help="Disable automatic backup of existing files")

    # Logging options
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")
    parser.add_argument("--quiet", "-q", action="store_true",
                       help="Suppress non-error output")

    args = parser.parse_args()

    # Adjust logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.quiet:
        logging.getLogger().setLevel(logging.ERROR)

    # Create configuration and scraper
    config = create_config_from_args(args)
    scraper = RedditScraper(config)

    try:
        logger.info("Starting Reddit scraper")

        if args.action in ["display", "both"]:
            scraper.display_demo_data()

        if args.action in ["scrape", "both"]:
            scraper.comprehensive_scrape(config.subreddit, config.limit, config.output_file)

        # Display statistics
        stats = scraper.get_statistics()
        logger.info(f"Scraping completed. Scraped: {stats['scraped_count']}, Errors: {stats['error_count']}")

    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
