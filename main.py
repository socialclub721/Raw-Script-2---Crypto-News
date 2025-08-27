import os
import requests
import time
from datetime import datetime, timedelta
from supabase import create_client, Client
import logging
from typing import List, Dict, Any, Optional, Set
import xml.etree.ElementTree as ET
import re
import hashlib

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CryptoRSSNewsIngestion:
    def __init__(self):
        """
        Initialize the Crypto RSS News Ingestion service using environment variables
        """
        # Get credentials from environment variables (Railway will set these)
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        # Validate environment variables
        if not all([supabase_url, supabase_key]):
            raise ValueError("Missing required environment variables: SUPABASE_URL, SUPABASE_KEY")
        
        # Initialize Supabase client
        self.supabase: Client = create_client(supabase_url, supabase_key)
        
        # RSS feed URLs
        self.rss_feeds = {
            'coindesk': 'https://www.coindesk.com/arc/outboundfeeds/rss',
            'cointelegraph': 'https://cointelegraph.com/rss',
            'theblock': 'https://www.theblock.co/rss.xml'
        }
        
        # Table name for crypto news articles
        self.news_table = "crypto_news_articles"
        
        # Maximum number of articles to fetch per feed per run
        self.max_articles_per_feed = 10
        
        # Maximum number of articles to keep in database
        self.max_articles_in_db = 100
        
        # Track processed articles in this session to avoid duplicates across feeds
        self.session_processed_headlines: Set[str] = set()
        self.session_processed_links: Set[str] = set()
        
    def normalize_headline(self, headline: str) -> str:
        """
        Normalize headline for duplicate detection
        
        Args:
            headline: Original headline
            
        Returns:
            Normalized headline for comparison
        """
        if not headline:
            return ""
        
        # Convert to lowercase
        normalized = headline.lower()
        
        # Remove special characters and extra spaces
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = ' '.join(normalized.split())
        
        return normalized.strip()
    
    def get_headline_hash(self, headline: str) -> str:
        """
        Generate a hash of the normalized headline for efficient duplicate detection
        
        Args:
            headline: Original headline
            
        Returns:
            Hash of normalized headline
        """
        normalized = self.normalize_headline(headline)
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def is_duplicate_article(self, headline: str, link: str) -> bool:
        """
        Check if article is a duplicate based on headline or link
        
        Args:
            headline: Article headline
            link: Article link
            
        Returns:
            True if duplicate, False otherwise
        """
        # Check exact link match
        if link and link in self.session_processed_links:
            logger.debug(f"Duplicate link found: {link}")
            return True
        
        # Check normalized headline match
        if headline:
            headline_hash = self.get_headline_hash(headline)
            if headline_hash in self.session_processed_headlines:
                logger.debug(f"Duplicate headline found: {headline[:50]}...")
                return True
        
        return False
    
    def mark_as_processed(self, headline: str, link: str):
        """
        Mark article as processed in this session
        
        Args:
            headline: Article headline
            link: Article link
        """
        if link:
            self.session_processed_links.add(link)
        
        if headline:
            headline_hash = self.get_headline_hash(headline)
            self.session_processed_headlines.add(headline_hash)
    
    def parse_rfc2822_date(self, date_str: str) -> Optional[datetime]:
        """
        Parse RFC 2822 date format used in RSS feeds with proper timezone handling
        
        Args:
            date_str: Date string from RSS feed
            
        Returns:
            Parsed datetime object in UTC or None if parsing fails
        """
        if not date_str:
            return None
        
        # Clean up the date string
        date_str = date_str.strip()
        
        # Import required modules for timezone handling
        from email.utils import parsedate_to_datetime
        import email.utils
        
        try:
            # Use email.utils.parsedate_to_datetime which properly handles RFC 2822 dates
            dt = parsedate_to_datetime(date_str)
            
            # Convert to UTC if it has timezone info
            if dt.tzinfo is not None:
                dt = dt.utctimetuple()
                dt = datetime(*dt[:6])
            
            return dt
            
        except (ValueError, TypeError, OverflowError):
            pass
        
        # Fallback: Manual parsing for common RSS date formats
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%a, %d %b %Y %H:%M:%S",
            "%d %b %Y %H:%M:%S %Z",
            "%Y-%m-%d %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ"
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
                
        logger.warning(f"Could not parse date: {date_str}")
        return None
    
    def extract_description_from_html(self, html_content: str) -> str:
        """
        Extract clean description from HTML content
        
        Args:
            html_content: HTML content from RSS description
            
        Returns:
            Clean text description
        """
        if not html_content:
            return ""
        
        # Remove HTML tags using regex
        clean_text = re.sub(r'<[^>]+>', ' ', html_content)
        
        # Remove extra whitespace and decode HTML entities
        import html
        clean_text = html.unescape(clean_text)
        clean_text = ' '.join(clean_text.split())
        
        return clean_text.strip()
    
    def clean_cdata(self, text: str) -> str:
        """
        Remove CDATA wrapper from text content
        
        Args:
            text: Text that may contain CDATA wrapper
            
        Returns:
            Clean text without CDATA wrapper
        """
        if not text:
            return ""
        
        text = text.strip()
        if text.startswith('<![CDATA[') and text.endswith(']]>'):
            text = text[9:-3]
        
        return text.strip()
    
    def fetch_rss_feed(self, feed_name: str, feed_url: str) -> List[Dict[str, Any]]:
        """
        Generic RSS feed fetcher for any crypto news source
        
        Args:
            feed_name: Name of the feed (for logging)
            feed_url: URL of the RSS feed
            
        Returns:
            List of parsed news items
        """
        try:
            logger.info(f"Fetching {feed_name} articles...")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(feed_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            items = []
            
            # Parse RSS feed
            channel = root.find('channel')
            if channel is None:
                return items
                
            rss_items = channel.findall('item')
            logger.info(f"Found {len(rss_items)} {feed_name} articles in feed")
            
            # Sort by publication date and take only the latest
            items_with_dates = []
            for item in rss_items:
                try:
                    pub_date = item.find('pubDate')
                    pub_date_text = pub_date.text if pub_date is not None else ""
                    parsed_date = self.parse_rfc2822_date(pub_date_text)
                    
                    if parsed_date:
                        items_with_dates.append((item, parsed_date))
                except:
                    continue
            
            # Sort by date (newest first) and take only the latest
            items_with_dates.sort(key=lambda x: x[1], reverse=True)
            latest_items = items_with_dates[:self.max_articles_per_feed]
            
            logger.info(f"Processing latest {len(latest_items)} {feed_name} articles")
            
            processed_count = 0
            duplicate_count = 0
            
            for item, parsed_date in latest_items:
                try:
                    title = item.find('title')
                    link = item.find('link')
                    description = item.find('description')
                    
                    title_text = self.clean_cdata(title.text if title is not None else "")
                    link_text = link.text if link is not None else ""
                    description_text = self.clean_cdata(description.text if description is not None else "")
                    
                    # Skip if missing essential fields
                    if not title_text or not link_text:
                        continue
                    
                    # Check for duplicates BEFORE adding to results
                    if self.is_duplicate_article(title_text, link_text):
                        duplicate_count += 1
                        logger.debug(f"Skipping duplicate from {feed_name}: {title_text[:50]}...")
                        continue
                    
                    # Mark as processed for this session
                    self.mark_as_processed(title_text, link_text)
                    
                    # Extract clean description from HTML
                    clean_description = self.extract_description_from_html(description_text)
                    
                    item_data = {
                        'headline': title_text.strip(),
                        'description': clean_description if clean_description else None,
                        'link': link_text.strip(),
                        'published_at': parsed_date.isoformat(),
                        'source_name': feed_name
                    }
                    
                    items.append(item_data)
                    processed_count += 1
                    
                except Exception as e:
                    logger.error(f"Error parsing {feed_name} item: {e}")
                    continue
            
            logger.info(f"{feed_name}: {processed_count} unique articles added, {duplicate_count} duplicates skipped")
            return items
            
        except Exception as e:
            logger.error(f"Error fetching {feed_name} feed: {e}")
            return []
    
    def fetch_all_feeds(self) -> List[Dict[str, Any]]:
        """
        Fetch articles from all RSS feeds
        
        Returns:
            Combined list of all articles from all feeds
        """
        all_articles = []
        
        # Reset session tracking for this fetch cycle
        self.session_processed_headlines.clear()
        self.session_processed_links.clear()
        
        # Fetch from each feed
        for feed_name, feed_url in self.rss_feeds.items():
            display_name = feed_name.replace('_', ' ').title()
            if feed_name == 'coindesk':
                display_name = 'CoinDesk'
            elif feed_name == 'cointelegraph':
                display_name = 'Cointelegraph'
            elif feed_name == 'theblock':
                display_name = 'The Block'
                
            articles = self.fetch_rss_feed(display_name, feed_url)
            all_articles.extend(articles)
        
        # Sort all articles by publication date
        def get_sort_key(article):
            try:
                dt_str = article['published_at']
                if dt_str.endswith('Z'):
                    dt_str = dt_str[:-1] + '+00:00'
                return datetime.fromisoformat(dt_str)
            except:
                return datetime.now()
        
        all_articles.sort(key=get_sort_key)
        
        logger.info(f"Total unique articles fetched: {len(all_articles)}")
        
        # Log summary
        if all_articles:
            logger.info("Article chronological order (first 3):")
            for i, article in enumerate(all_articles[:3], 1):
                dt_obj = get_sort_key(article)
                pub_time = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
                source = article['source_name']
                title = article['headline'][:40]
                logger.info(f"  {i}. {pub_time} [{source}] {title}...")
        
        return all_articles
    
    def check_existing_articles(self, articles: List[Dict]) -> Set[str]:
        """
        Check which articles already exist in database using multiple criteria
        
        Args:
            articles: List of articles to check
            
        Returns:
            Set of identifiers (links and headline hashes) that already exist
        """
        if not articles:
            return set()
        
        existing_identifiers = set()
        
        try:
            # Check by links
            links = [article['link'] for article in articles if article.get('link')]
            if links:
                result = self.supabase.table(self.news_table)\
                    .select('link')\
                    .in_('link', links)\
                    .execute()
                
                for item in result.data:
                    existing_identifiers.add(item['link'])
            
            # Check by headlines (to catch duplicates with different links)
            headlines = [article['headline'] for article in articles if article.get('headline')]
            if headlines:
                # Check recent articles with same headline (last 48 hours)
                cutoff_time = (datetime.now() - timedelta(hours=48)).isoformat()
                
                result = self.supabase.table(self.news_table)\
                    .select('headline')\
                    .in_('headline', headlines)\
                    .gte('created_at', cutoff_time)\
                    .execute()
                
                for item in result.data:
                    headline_hash = self.get_headline_hash(item['headline'])
                    existing_identifiers.add(f"headline:{headline_hash}")
            
            logger.info(f"Found {len(existing_identifiers)} existing article identifiers in database")
            return existing_identifiers
            
        except Exception as e:
            logger.error(f"Error checking existing articles: {e}")
            return set()
    
    def prepare_articles_for_storage(self, articles: List[Dict]) -> List[Dict]:
        """
        Prepare articles for storage, filtering out database duplicates
        
        Args:
            articles: List of parsed articles
            
        Returns:
            List of formatted articles ready for database insertion
        """
        if not articles:
            return []
        
        # Check which articles already exist in database
        existing_identifiers = self.check_existing_articles(articles)
        
        new_articles = []
        db_duplicates_count = 0
        
        for article in articles:
            # Check if article exists by link
            if article.get('link') and article['link'] in existing_identifiers:
                db_duplicates_count += 1
                logger.debug(f"Skipping existing article (link): {article['headline'][:50]}...")
                continue
            
            # Check if article exists by headline hash
            headline_hash = self.get_headline_hash(article.get('headline', ''))
            if f"headline:{headline_hash}" in existing_identifiers:
                db_duplicates_count += 1
                logger.debug(f"Skipping existing article (headline): {article['headline'][:50]}...")
                continue
            
            new_articles.append(article)
        
        logger.info(f"Prepared {len(new_articles)} new articles ({db_duplicates_count} already in database)")
        
        # Log new articles
        if new_articles:
            logger.info("New articles to be added:")
            for i, article in enumerate(new_articles[:3], 1):
                source = article['source_name']
                title = article['headline'][:50]
                logger.info(f"  {i}. [{source}] {title}...")
            if len(new_articles) > 3:
                logger.info(f"  ... and {len(new_articles) - 3} more")
        
        return new_articles
    
    def cleanup_old_articles(self, new_articles_count: int) -> bool:
        """
        Remove oldest articles to maintain the maximum limit of 100 articles
        
        Args:
            new_articles_count: Number of new articles being added
            
        Returns:
            Boolean indicating success
        """
        try:
            if new_articles_count == 0:
                return True
            
            # Get current article count
            count_result = self.supabase.table(self.news_table)\
                .select('id', count='exact')\
                .execute()
            
            current_count = count_result.count if hasattr(count_result, 'count') else len(count_result.data)
            
            # Calculate how many articles to delete
            total_after_insert = current_count + new_articles_count
            articles_to_delete = max(0, total_after_insert - self.max_articles_in_db)
            
            if articles_to_delete == 0:
                logger.info(f"No cleanup needed. Current: {current_count}, adding: {new_articles_count}, max: {self.max_articles_in_db}")
                return True
            
            logger.info(f"Need to delete {articles_to_delete} oldest articles to maintain limit of {self.max_articles_in_db}")
            
            # Get the oldest articles to delete
            oldest_articles = self.supabase.table(self.news_table)\
                .select('id, headline')\
                .order('published_at', desc=False)\
                .order('created_at', desc=False)\
                .limit(articles_to_delete)\
                .execute()
            
            if not oldest_articles.data:
                return True
            
            # Delete the oldest articles
            ids_to_delete = [article['id'] for article in oldest_articles.data]
            
            self.supabase.table(self.news_table)\
                .delete()\
                .in_('id', ids_to_delete)\
                .execute()
            
            logger.info(f"Deleted {len(ids_to_delete)} oldest articles")
            return True
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return False
    
    def store_articles_in_supabase(self, articles: List[Dict]) -> bool:
        """
        Store articles in Supabase
        
        Args:
            articles: List of formatted articles
            
        Returns:
            Boolean indicating success
        """
        if not articles:
            logger.info("No new articles to store")
            return True
        
        try:
            # Cleanup old articles first
            self.cleanup_old_articles(len(articles))
            
            # Insert articles
            result = self.supabase.table(self.news_table).insert(articles).execute()
            
            logger.info(f"Successfully stored {len(articles)} new articles in Supabase")
            return True
            
        except Exception as e:
            logger.error(f"Error storing articles: {e}")
            return False
    
    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the current state of the database
        
        Returns:
            Dictionary with database statistics
        """
        try:
            # Get total count
            count_result = self.supabase.table(self.news_table)\
                .select('id', count='exact')\
                .execute()
            
            total_count = count_result.count if hasattr(count_result, 'count') else len(count_result.data)
            
            # Get count by source
            source_counts = {}
            for source in ['CoinDesk', 'Cointelegraph', 'The Block']:
                source_result = self.supabase.table(self.news_table)\
                    .select('id', count='exact')\
                    .eq('source_name', source)\
                    .execute()
                source_counts[source] = source_result.count if hasattr(source_result, 'count') else len(source_result.data)
            
            return {
                'total_articles': total_count,
                'by_source': source_counts
            }
            
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {'total_articles': 0, 'by_source': {}}
    
    def run_ingestion(self) -> bool:
        """
        Run the complete RSS news ingestion process
        
        Returns:
            Boolean indicating success
        """
        try:
            logger.info("🚀 Starting crypto RSS news ingestion...")
            logger.info(f"📊 Fetching latest {self.max_articles_per_feed} articles from each feed")
            
            # Get database stats before
            stats_before = self.get_database_stats()
            logger.info(f"📊 Database before: {stats_before['total_articles']} total articles")
            if stats_before.get('by_source'):
                for source, count in stats_before['by_source'].items():
                    logger.info(f"   - {source}: {count}")
            
            # Fetch all articles (with duplicate prevention across feeds)
            all_articles = self.fetch_all_feeds()
            
            if not all_articles:
                logger.info("No articles fetched from RSS feeds")
                return True
            
            # Prepare articles (check against database)
            new_articles = self.prepare_articles_for_storage(all_articles)
            
            if not new_articles:
                logger.info("✅ All fetched articles already exist in database")
                return True
            
            # Store in Supabase
            success = self.store_articles_in_supabase(new_articles)
            
            if success:
                # Get database stats after
                stats_after = self.get_database_stats()
                logger.info(f"📊 Database after: {stats_after['total_articles']} total articles")
                if stats_after.get('by_source'):
                    for source, count in stats_after['by_source'].items():
                        logger.info(f"   - {source}: {count}")
                
                articles_added = stats_after['total_articles'] - stats_before['total_articles']
                logger.info(f"✅ Added {articles_added} new articles successfully")
            
            return success
            
        except Exception as e:
            logger.error(f"Fatal error during ingestion: {e}")
            return False


def run_once():
    """
    Run the ingestion process once
    """
    try:
        ingestion_service = CryptoRSSNewsIngestion()
        success = ingestion_service.run_ingestion()
        
        if success:
            logger.info("🎉 Ingestion completed successfully")
            return True
        else:
            logger.error("💥 Ingestion failed")
            return False
            
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False


def main():
    """
    Main function to run the crypto RSS news ingestion continuously
    """
    import time
    
    logger.info("=" * 60)
    logger.info("🚀 Crypto RSS News Ingestion Service (No Duplicates)")
    logger.info("📰 Sources: CoinDesk, Cointelegraph & The Block")
    logger.info("=" * 60)
    
    run_mode = os.getenv('RUN_MODE', 'continuous').lower()
    
    if run_mode == 'once':
        logger.info("Running in ONCE mode")
        success = run_once()
        exit(0 if success else 1)
    
    # Continuous mode
    logger.info("Running in CONTINUOUS mode - every 60 seconds")
    
    consecutive_failures = 0
    max_consecutive_failures = 5
    interval_seconds = 60
    
    while True:
        try:
            iteration_start = datetime.now()
            logger.info(f"\n{'=' * 50}")
            logger.info(f"⏰ Starting at {iteration_start.strftime('%Y-%m-%d %H:%M:%S')}")
            
            success = run_once()
            
            if success:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                logger.warning(f"Failed attempt {consecutive_failures}/{max_consecutive_failures}")
                
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"Too many failures. Exiting...")
                    exit(1)
            
            # Calculate sleep time
            processing_time = (datetime.now() - iteration_start).total_seconds()
            sleep_time = max(interval_seconds - processing_time, 1)
            logger.info(f"💤 Sleeping for {sleep_time:.0f} seconds...")
            
            time.sleep(sleep_time)
            
        except KeyboardInterrupt:
            logger.info("\n⛔ Shutting down...")
            break
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            consecutive_failures += 1
            
            if consecutive_failures >= max_consecutive_failures:
                logger.error(f"Too many failures. Exiting...")
                exit(1)
            
            time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
