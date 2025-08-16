import os
import requests
import time
from datetime import datetime
from supabase import create_client, Client
import logging
from typing import List, Dict, Any, Optional
import xml.etree.ElementTree as ET
from urllib.parse import urljoin
import hashlib

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CryptoRSSIngestion:
    def __init__(self):
        """
        Initialize the Crypto RSS News Ingestion service using environment variables
        """
        # Get credentials from environment variables
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        # Debug logging for environment variables
        logger.info(f"🔍 Supabase URL: {supabase_url[:50]}..." if supabase_url else "❌ SUPABASE_URL not found")
        logger.info(f"🔍 Supabase Key length: {len(supabase_key) if supabase_key else 0} characters")
        
        # Validate environment variables
        if not all([supabase_url, supabase_key]):
            raise ValueError("Missing required environment variables: SUPABASE_URL, SUPABASE_KEY")
        
        # Initialize Supabase client with debug info
        try:
            self.supabase: Client = create_client(supabase_url, supabase_key)
            logger.info("✅ Supabase client initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Supabase client: {e}")
            raise
        
        # RSS feed URL for Cointelegraph Editor's Pick
        self.rss_feed_url = "https://cointelegraph.com/editors_pick_rss"
        
        # Table name for crypto RSS news data
        self.crypto_news_table = "crypto_rss_news"
        
        # Maximum number of articles to keep in database
        self.max_articles = 100
        
    def fetch_rss_feed(self) -> List[Dict[str, Any]]:
        """
        Fetch and parse RSS feed from Cointelegraph
        
        Returns:
            List of parsed crypto news items from RSS feed
        """
        try:
            logger.info(f"Fetching crypto RSS feed from {self.rss_feed_url}...")
            
            # Set headers to mimic a browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(self.rss_feed_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parse XML content
            root = ET.fromstring(response.content)
            
            # Find all items in the RSS feed
            items = []
            
            # Navigate through the XML structure (RSS 2.0 format)
            channel = root.find('channel')
            if channel is None:
                logger.error("No channel found in RSS feed")
                return []
            
            rss_items = channel.findall('item')
            logger.info(f"Found {len(rss_items)} items in crypto RSS feed")
            
            for item in rss_items:
                try:
                    title_elem = item.find('title')
                    link_elem = item.find('link')
                    # Look for dc:creator element (Dublin Core namespace)
                    author_elem = item.find('{http://purl.org/dc/elements/1.1/}creator')
                    # Fallback to regular author if dc:creator not found
                    if author_elem is None:
                        author_elem = item.find('author')
                    pubdate_elem = item.find('pubDate')
                    description_elem = item.find('description')
                    guid_elem = item.find('guid')
                    
                    title = title_elem.text if title_elem is not None else ""
                    link = link_elem.text if link_elem is not None else ""
                    author = author_elem.text if author_elem is not None else "Cointelegraph"
                    pubdate = pubdate_elem.text if pubdate_elem is not None else ""
                    description = description_elem.text if description_elem is not None else ""
                    guid = guid_elem.text if guid_elem is not None else ""
                    
                    # Create a unique ID based on the GUID or link + title
                    unique_source = guid if guid else (link + title)
                    unique_id = hashlib.md5(unique_source.encode()).hexdigest()
                    
                    # Parse the publication date
                    parsed_pubdate = None
                    if pubdate:
                        try:
                            # Parse RFC 2822 date format with timezone (e.g., "Sat, 16 Aug 2025 08:49:44 +0100")
                            parsed_pubdate = datetime.strptime(pubdate, "%a, %d %b %Y %H:%M:%S %z")
                        except ValueError:
                            try:
                                # Try without timezone
                                parsed_pubdate = datetime.strptime(pubdate, "%a, %d %b %Y %H:%M:%S")
                            except ValueError:
                                try:
                                    # Try with GMT timezone
                                    parsed_pubdate = datetime.strptime(pubdate, "%a, %d %b %Y %H:%M:%S %Z")
                                except ValueError:
                                    logger.warning(f"Could not parse date: {pubdate}")
                    
                    item_data = {
                        'unique_id': unique_id,
                        'title': title.strip(),
                        'link': link.strip(),
                        'author': author.strip(),
                        'description': description.strip(),
                        'guid': guid.strip(),
                        'pubdate_raw': pubdate.strip(),
                        'pubdate_parsed': parsed_pubdate.isoformat() if parsed_pubdate else None,
                    }
                    
                    # Only add items with required fields
                    if item_data['title'] and item_data['link']:
                        items.append(item_data)
                    else:
                        logger.warning(f"Skipping item with missing title or link: {item_data}")
                        
                except Exception as e:
                    logger.error(f"Error parsing RSS item: {e}")
                    continue
            
            # Sort by publication date (newest first) and take only the latest 100
            items_with_dates = []
            items_without_dates = []
            
            for item in items:
                if item.get('pubdate_parsed'):
                    items_with_dates.append(item)
                else:
                    items_without_dates.append(item)
            
            # Sort items with dates by publication date (newest first)
            items_with_dates.sort(key=lambda x: x['pubdate_parsed'], reverse=True)
            
            # Combine: items with dates first (sorted), then items without dates
            sorted_items = items_with_dates + items_without_dates
            
            # Take only the latest 100 items
            latest_items = sorted_items[:100]
            
            logger.info(f"Selected latest {len(latest_items)} crypto articles from {len(items)} total items")
            return latest_items
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching crypto RSS feed: {e}")
            raise
        except ET.ParseError as e:
            logger.error(f"Error parsing crypto RSS XML: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching crypto RSS feed: {e}")
            raise
    
    def check_existing_articles(self, unique_ids: List[str]) -> List[str]:
        """
        Check which articles already exist in Supabase
        
        Args:
            unique_ids: List of unique article IDs to check
            
        Returns:
            List of IDs that already exist in the database
        """
        if not unique_ids:
            return []
        
        try:
            result = self.supabase.table(self.crypto_news_table)\
                .select('unique_id')\
                .in_('unique_id', unique_ids)\
                .execute()
            
            existing_ids = [item['unique_id'] for item in result.data] if result.data else []
            logger.info(f"Found {len(existing_ids)} existing crypto articles in database")
            return existing_ids
            
        except Exception as e:
            logger.error(f"Error checking existing crypto articles: {e}")
            return []
    
    def prepare_articles_for_storage(self, rss_items: List[Dict]) -> List[Dict]:
        """
        Prepare RSS articles for storage in Supabase
        Filters out articles that already exist in the database
        
        Args:
            rss_items: Parsed RSS items
            
        Returns:
            List of formatted articles ready for database insertion
        """
        # Extract all unique IDs to check
        all_ids = [item['unique_id'] for item in rss_items if item.get('unique_id')]
        
        # Check which articles already exist
        existing_ids = self.check_existing_articles(all_ids)
        existing_ids_set = set(existing_ids)
        
        formatted_articles = []
        current_timestamp = datetime.now().isoformat()
        duplicates_count = 0
        
        for item in rss_items:
            unique_id = item.get('unique_id', '')
            
            # Skip if article already exists
            if unique_id in existing_ids_set:
                duplicates_count += 1
                logger.debug(f"Skipping duplicate crypto article ID: {unique_id}")
                continue
            
            # Format article for database storage
            formatted_article = {
                'unique_id': unique_id,
                'title': item.get('title', ''),
                'link': item.get('link', ''),
                'author': item.get('author', ''),
                'description': item.get('description', ''),  # Added back description
                'guid': item.get('guid', ''),              # Added back guid
                'pubdate_raw': item.get('pubdate_raw', ''),
                'pubdate_parsed': item.get('pubdate_parsed'),
                'ingested_at': current_timestamp,
                'processed': False
            }
            
            formatted_articles.append(formatted_article)
        
        logger.info(f"Prepared {len(formatted_articles)} new crypto articles for storage ({duplicates_count} duplicates skipped)")
        return formatted_articles
    
    def cleanup_old_articles(self, new_articles_count: int) -> bool:
        """
        Remove oldest articles to maintain the maximum limit of articles in database
        
        Args:
            new_articles_count: Number of new articles being added
            
        Returns:
            Boolean indicating success
        """
        try:
            if new_articles_count == 0:
                return True
            
            # Get current article count
            count_result = self.supabase.table(self.crypto_news_table)\
                .select('*', count='exact')\
                .execute()
            
            current_count = count_result.count if hasattr(count_result, 'count') else len(count_result.data)
            
            # Calculate how many articles to delete
            total_after_insert = current_count + new_articles_count
            articles_to_delete = max(0, total_after_insert - self.max_articles)
            
            if articles_to_delete == 0:
                logger.info(f"No crypto cleanup needed. Current: {current_count}, adding: {new_articles_count}, max: {self.max_articles}")
                return True
            
            logger.info(f"Need to delete {articles_to_delete} oldest crypto articles to maintain limit of {self.max_articles}")
            
            # Get the oldest articles to delete (ordered by pubdate_parsed, then by ingested_at)
            oldest_articles = self.supabase.table(self.crypto_news_table)\
                .select('id, title')\
                .order('pubdate_parsed', desc=False)\
                .order('ingested_at', desc=False)\
                .limit(articles_to_delete)\
                .execute()
            
            if not oldest_articles.data:
                logger.warning("No crypto articles found to delete")
                return True
            
            # Extract IDs to delete
            ids_to_delete = [article['id'] for article in oldest_articles.data]
            
            # Log which articles are being deleted
            logger.info(f"Deleting {len(ids_to_delete)} oldest crypto articles:")
            for article in oldest_articles.data[:3]:  # Log first 3
                logger.info(f"  - {article['title'][:60]}...")
            if len(oldest_articles.data) > 3:
                logger.info(f"  ... and {len(oldest_articles.data) - 3} more")
            
            # Delete the oldest articles
            delete_result = self.supabase.table(self.crypto_news_table)\
                .delete()\
                .in_('id', ids_to_delete)\
                .execute()
            
            logger.info(f"Successfully deleted {len(ids_to_delete)} oldest crypto articles")
            return True
            
        except Exception as e:
            logger.error(f"Error during cleanup of old crypto articles: {e}")
            return False
    
    def store_articles_in_supabase(self, formatted_articles: List[Dict]) -> bool:
        """
        Store formatted articles in Supabase
        
        Args:
            formatted_articles: List of formatted articles
            
        Returns:
            Boolean indicating success
        """
        if not formatted_articles:
            logger.info("No new crypto articles to store")
            return True
        
        try:
            # First, cleanup old articles if we're adding new ones
            cleanup_success = self.cleanup_old_articles(len(formatted_articles))
            if not cleanup_success:
                logger.warning("Crypto cleanup failed, but continuing with insertion...")
            
            # Insert articles
            result = self.supabase.table(self.crypto_news_table).insert(
                formatted_articles
            ).execute()
            
            logger.info(f"Successfully stored {len(formatted_articles)} new crypto articles in Supabase")
            return True
            
        except Exception as e:
            logger.error(f"Error storing crypto articles in Supabase: {e}")
            raise
    
    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the current state of the database
        
        Returns:
            Dictionary with database statistics
        """
        try:
            logger.info(f"🔍 Testing connection to table: {self.crypto_news_table}")
            
            # Get total count
            count_result = self.supabase.table(self.crypto_news_table)\
                .select('*', count='exact')\
                .execute()
            
            total_count = count_result.count if hasattr(count_result, 'count') else len(count_result.data)
            
            # Get latest article
            latest_result = self.supabase.table(self.crypto_news_table)\
                .select('unique_id, title, pubdate_parsed')\
                .order('ingested_at', desc=True)\
                .limit(1)\
                .execute()
            
            stats = {
                'total_articles': total_count,
                'latest_article': latest_result.data[0] if latest_result.data else None
            }
            
            logger.info(f"✅ Database connection successful. Found {total_count} articles")
            return stats
            
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            logger.error(f"❌ Error type: {type(e)}")
            # Try a simple test query
            try:
                logger.info("🔄 Attempting simple test query...")
                test_result = self.supabase.table(self.crypto_news_table).select('*').limit(1).execute()
                logger.info(f"✅ Simple query worked: {len(test_result.data)} rows")
            except Exception as test_e:
                logger.error(f"❌ Simple test query also failed: {test_e}")
            
            return {'total_articles': 0, 'latest_article': None}
    
    def run_ingestion(self) -> bool:
        """
        Run the complete crypto RSS news ingestion process
        
        Returns:
            Boolean indicating success
        """
        try:
            logger.info("🪙 Starting crypto RSS news ingestion from Cointelegraph...")
            logger.info(f"📊 Maintaining maximum of {self.max_articles} crypto articles in database")
            
            # Get database stats before ingestion
            stats_before = self.get_database_stats()
            logger.info(f"Database stats before ingestion: {stats_before['total_articles']} total crypto articles")
            
            # Fetch RSS feed
            rss_items = self.fetch_rss_feed()
            
            if not rss_items:
                logger.info("No crypto articles fetched from RSS feed")
                return True
            
            # Log the fetched articles
            logger.info("Fetched crypto articles:")
            for i, article in enumerate(rss_items[:5], 1):
                logger.info(f"  {i}. {article.get('title', 'No title')[:80]}...")
            if len(rss_items) > 5:
                logger.info(f"  ... and {len(rss_items) - 5} more crypto articles")
            
            # Prepare articles for storage (this will filter out duplicates)
            formatted_articles = self.prepare_articles_for_storage(rss_items)
            
            if not formatted_articles:
                logger.info("✅ All fetched crypto articles already exist in database - no new articles to add")
                return True
            
            # Store in Supabase
            success = self.store_articles_in_supabase(formatted_articles)
            
            if success:
                # Get database stats after ingestion
                stats_after = self.get_database_stats()
                logger.info(f"Database stats after ingestion: {stats_after['total_articles']} total crypto articles")
                logger.info(f"✅ Crypto RSS ingestion completed successfully! Added {len(formatted_articles)} new articles")
            else:
                logger.error("❌ Crypto RSS ingestion failed during storage")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Fatal error during crypto ingestion: {e}")
            return False


def run_once():
    """
    Run the ingestion process once
    """
    try:
        # Initialize the ingestion service
        ingestion_service = CryptoRSSIngestion()
        
        # Run the ingestion
        success = ingestion_service.run_ingestion()
        
        if success:
            logger.info("🎉 Crypto ingestion process completed successfully")
            return True
        else:
            logger.error("💥 Crypto ingestion process failed")
            return False
            
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Please set the required environment variables in Railway:")
        logger.error("- SUPABASE_URL") 
        logger.error("- SUPABASE_KEY")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False


def main():
    """
    Main function to run the crypto RSS news ingestion continuously
    Runs every minute in an infinite loop
    """
    import time
    
    logger.info("=" * 60)
    logger.info("🪙 Starting Crypto RSS News Ingestion Service")
    logger.info("📰 Will fetch Cointelegraph RSS feed every minute")
    logger.info("=" * 60)
    
    # Run mode from environment variable (default to continuous)
    run_mode = os.getenv('RUN_MODE', 'continuous').lower()
    
    if run_mode == 'once':
        # Run once and exit (useful for testing)
        logger.info("Running in ONCE mode - will execute once and exit")
        success = run_once()
        exit(0 if success else 1)
    
    # Continuous mode - run every minute
    logger.info("Running in CONTINUOUS mode - will execute every minute")
    
    consecutive_failures = 0
    max_consecutive_failures = 5
    interval_seconds = 60  # 1 minute
    
    while True:
        try:
            iteration_start = datetime.now()
            logger.info(f"\n{'=' * 50}")
            logger.info(f"⏰ Starting crypto ingestion at {iteration_start.strftime('%Y-%m-%d %H:%M:%S')}")
            
            success = run_once()
            
            if success:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                logger.warning(f"Failed attempt {consecutive_failures}/{max_consecutive_failures}")
                
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"❌ Too many consecutive failures ({max_consecutive_failures}). Exiting...")
                    exit(1)
            
            # Calculate time until next run
            iteration_end = datetime.now()
            processing_time = (iteration_end - iteration_start).total_seconds()
            logger.info(f"⏱️  Processing took {processing_time:.2f} seconds")
            
            # Wait for the specified interval
            sleep_time = max(interval_seconds - processing_time, 1)  # Minimum 1 second sleep
            logger.info(f"💤 Sleeping for {sleep_time:.2f} seconds until next run...")
            logger.info(f"{'=' * 50}\n")
            
            time.sleep(sleep_time)
            
        except KeyboardInterrupt:
            logger.info("\n⛔ Received interrupt signal. Shutting down gracefully...")
            break
        except Exception as e:
            logger.error(f"❌ Unexpected error in main loop: {e}")
            consecutive_failures += 1
            
            if consecutive_failures >= max_consecutive_failures:
                logger.error(f"❌ Too many consecutive failures ({max_consecutive_failures}). Exiting...")
                exit(1)
            
            # Wait a bit before retrying
            logger.info(f"⏳ Waiting {interval_seconds} seconds before retry...")
            time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
