import os
import requests
import time
from datetime import datetime, timedelta
from supabase import create_client, Client
import logging
from typing import List, Dict, Any, Optional
import xml.etree.ElementTree as ET
import re

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
            # This handles GMT, EST, PST, +0000, -0500, etc. properly
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
            "%a, %d %b %Y %H:%M:%S %z",      # Wed, 27 Aug 2025 13:30:00 +0100
            "%a, %d %b %Y %H:%M:%S %Z",      # Wed, 27 Aug 2025 09:18:56 GMT
            "%a, %d %b %Y %H:%M:%S",         # Without timezone
            "%d %b %Y %H:%M:%S %Z",          # Without day name
            "%Y-%m-%d %H:%M:%S %Z",          # ISO-like format
            "%Y-%m-%dT%H:%M:%S%z",           # ISO format with timezone
            "%Y-%m-%dT%H:%M:%SZ"             # ISO format UTC
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
            text = text[9:-3]  # Remove CDATA wrapper
        
        return text.strip()
    
    def fetch_coindesk_rss(self) -> List[Dict[str, Any]]:
        """
        Fetch CoinDesk articles from their RSS feed
        
        Returns:
            List of parsed news items
        """
        try:
            url = self.rss_feeds['coindesk']
            logger.info(f"Fetching CoinDesk articles...")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            items = []
            
            # Parse RSS feed
            channel = root.find('channel')
            if channel is None:
                return items
                
            rss_items = channel.findall('item')
            logger.info(f"Found {len(rss_items)} CoinDesk articles in feed")
            
            # Sort by publication date and take only the latest 10
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
            
            # Sort by date (newest first) and take only the latest 10
            items_with_dates.sort(key=lambda x: x[1], reverse=True)
            latest_items = items_with_dates[:self.max_articles_per_feed]
            
            logger.info(f"Processing latest {len(latest_items)} CoinDesk articles")
            
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
                    
                    # Extract clean description from HTML
                    clean_description = self.extract_description_from_html(description_text)
                    
                    item_data = {
                        'headline': title_text.strip(),
                        'description': clean_description if clean_description else None,
                        'link': link_text.strip(),
                        'published_at': parsed_date.isoformat(),
                        'source_name': 'CoinDesk'
                    }
                    
                    items.append(item_data)
                    
                except Exception as e:
                    logger.error(f"Error parsing CoinDesk item: {e}")
                    continue
            
            return items
            
        except Exception as e:
            logger.error(f"Error fetching CoinDesk feed: {e}")
            return []
    
    def fetch_cointelegraph_rss(self) -> List[Dict[str, Any]]:
        """
        Fetch Cointelegraph articles from their RSS feed
        
        Returns:
            List of parsed news items
        """
        try:
            url = self.rss_feeds['cointelegraph']
            logger.info(f"Fetching Cointelegraph articles...")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            items = []
            
            # Parse RSS feed
            channel = root.find('channel')
            if channel is None:
                return items
                
            rss_items = channel.findall('item')
            logger.info(f"Found {len(rss_items)} Cointelegraph articles in feed")
            
            # Sort by publication date and take only the latest 10
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
            
            # Sort by date (newest first) and take only the latest 10
            items_with_dates.sort(key=lambda x: x[1], reverse=True)
            latest_items = items_with_dates[:self.max_articles_per_feed]
            
            logger.info(f"Processing latest {len(latest_items)} Cointelegraph articles")
            
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
                    
                    # Extract clean description from HTML
                    clean_description = self.extract_description_from_html(description_text)
                    
                    item_data = {
                        'headline': title_text.strip(),
                        'description': clean_description if clean_description else None,
                        'link': link_text.strip(),
                        'published_at': parsed_date.isoformat(),
                        'source_name': 'Cointelegraph'
                    }
                    
                    items.append(item_data)
                    
                except Exception as e:
                    logger.error(f"Error parsing Cointelegraph item: {e}")
                    continue
            
            return items
            
        except Exception as e:
            logger.error(f"Error fetching Cointelegraph feed: {e}")
            return []
    
    def fetch_theblock_rss(self) -> List[Dict[str, Any]]:
        """
        Fetch The Block articles from their RSS feed
        
        Returns:
            List of parsed news items
        """
        try:
            url = self.rss_feeds['theblock']
            logger.info(f"Fetching The Block articles...")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            items = []
            
            # Parse RSS feed
            channel = root.find('channel')
            if channel is None:
                return items
                
            rss_items = channel.findall('item')
            logger.info(f"Found {len(rss_items)} The Block articles in feed")
            
            # Sort by publication date and take only the latest 10
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
            
            # Sort by date (newest first) and take only the latest 10
            items_with_dates.sort(key=lambda x: x[1], reverse=True)
            latest_items = items_with_dates[:self.max_articles_per_feed]
            
            logger.info(f"Processing latest {len(latest_items)} The Block articles")
            
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
                    
                    # Extract clean description from HTML
                    clean_description = self.extract_description_from_html(description_text)
                    
                    item_data = {
                        'headline': title_text.strip(),
                        'description': clean_description if clean_description else None,
                        'link': link_text.strip(),
                        'published_at': parsed_date.isoformat(),
                        'source_name': 'The Block'
                    }
                    
                    items.append(item_data)
                    
                except Exception as e:
                    logger.error(f"Error parsing The Block item: {e}")
                    continue
            
            return items
            
        except Exception as e:
            logger.error(f"Error fetching The Block feed: {e}")
            return []
    
    def fetch_all_feeds(self) -> List[Dict[str, Any]]:
        """
        Fetch articles from all RSS feeds
        
        Returns:
            Combined list of all articles from all feeds
        """
        all_articles = []
        
        # Fetch CoinDesk articles
        coindesk_articles = self.fetch_coindesk_rss()
        all_articles.extend(coindesk_articles)
        
        # Fetch Cointelegraph articles
        cointelegraph_articles = self.fetch_cointelegraph_rss()
        all_articles.extend(cointelegraph_articles)
        
        # Fetch The Block articles
        theblock_articles = self.fetch_theblock_rss()
        all_articles.extend(theblock_articles)
        
        # Sort all articles by publication date (oldest first for proper chronological order)
        # Convert ISO strings to datetime objects for proper sorting
        def get_sort_key(article):
            try:
                # Parse the ISO datetime string back to a datetime object for sorting
                dt_str = article['published_at']
                if dt_str.endswith('Z'):
                    dt_str = dt_str[:-1] + '+00:00'
                return datetime.fromisoformat(dt_str)
            except:
                # Fallback to current time if parsing fails
                return datetime.now()
        
        all_articles.sort(key=get_sort_key)
        
        logger.info(f"Total articles fetched: {len(all_articles)} (CoinDesk: {len(coindesk_articles)}, Cointelegraph: {len(cointelegraph_articles)}, The Block: {len(theblock_articles)})")
        
        # Debug: Log the sorting to verify it's working
        logger.info("Article chronological order check (first 5):")
        for i, article in enumerate(all_articles[:5], 1):
            dt_obj = get_sort_key(article)
            pub_time = dt_obj.strftime('%Y-%m-%d %H:%M:%S')  # Clean format for logging
            source = article['source_name']
            title = article['headline'][:40]
            logger.info(f"  {i}. {pub_time} [{source}] {title}...")
        
        if len(all_articles) > 5:
            logger.info("  ...")
            # Also log the last few to verify proper sorting
            logger.info("Last 3 articles (newest):")
            for i, article in enumerate(all_articles[-3:], len(all_articles)-2):
                dt_obj = get_sort_key(article)  
                pub_time = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
                source = article['source_name']
                title = article['headline'][:40]
                logger.info(f"  {i}. {pub_time} [{source}] {title}...")
        
        return all_articles
    
    def check_existing_articles(self, links: List[str]) -> List[str]:
        """
        Check which articles already exist in Supabase based on their links
        
        Args:
            links: List of article links to check
            
        Returns:
            List of links that already exist in the database
        """
        if not links:
            return []
        
        try:
            # Check all links in one query
            result = self.supabase.table(self.news_table)\
                .select('link')\
                .in_('link', links)\
                .execute()
            
            existing_links = [item['link'] for item in result.data] if result.data else []
            logger.info(f"Found {len(existing_links)} existing articles out of {len(links)} checked")
            return existing_links
            
        except Exception as e:
            logger.error(f"Error checking existing articles: {e}")
            return []
    
    def prepare_articles_for_storage(self, articles: List[Dict]) -> List[Dict]:
        """
        Prepare articles for storage in Supabase
        Filters out articles that already exist in the database
        
        Args:
            articles: List of parsed articles
            
        Returns:
            List of formatted articles ready for database insertion
        """
        if not articles:
            return []
        
        # Extract all links to check for duplicates
        all_links = [article['link'] for article in articles]
        
        # Check which articles already exist
        existing_links = self.check_existing_articles(all_links)
        existing_links_set = set(existing_links)
        
        new_articles = []
        duplicates_count = 0
        
        for article in articles:
            # Skip if article already exists
            if article['link'] in existing_links_set:
                duplicates_count += 1
                continue
                
            new_articles.append(article)
        
        logger.info(f"Prepared {len(new_articles)} new articles for storage ({duplicates_count} duplicates skipped)")
        
        # Log which articles are being added (for debugging)
        if new_articles:
            logger.info("New articles to be added:")
            for i, article in enumerate(new_articles[:3], 1):
                source = article['source_name']
                title = article['headline'][:50]
                pub_time = article['published_at'][:19]  # Remove timezone part for cleaner log
                logger.info(f"  {i}. [{source}] {pub_time} - {title}...")
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
                logger.info(f"✅ No cleanup needed. Current: {current_count}, adding: {new_articles_count}, max: {self.max_articles_in_db}")
                return True
            
            logger.info(f"🧹 Need to delete {articles_to_delete} oldest articles to maintain limit of {self.max_articles_in_db}")
            
            # Get the oldest articles to delete (ordered by published_at, then by created_at)
            oldest_articles = self.supabase.table(self.news_table)\
                .select('id, headline, published_at')\
                .order('published_at', desc=False)\
                .order('created_at', desc=False)\
                .limit(articles_to_delete)\
                .execute()
            
            if not oldest_articles.data:
                logger.warning("⚠️  No articles found to delete")
                return True
            
            # Extract IDs to delete
            ids_to_delete = [article['id'] for article in oldest_articles.data]
            
            # Log which articles are being deleted
            logger.info(f"🗑️  Deleting {len(ids_to_delete)} oldest articles:")
            for article in oldest_articles.data[:3]:  # Log first 3
                pub_time = article['published_at'][:19] if article['published_at'] else 'Unknown'
                logger.info(f"    - {pub_time}: {article['headline'][:50]}...")
            if len(oldest_articles.data) > 3:
                logger.info(f"    ... and {len(oldest_articles.data) - 3} more")
            
            # Delete the oldest articles
            delete_result = self.supabase.table(self.news_table)\
                .delete()\
                .in_('id', ids_to_delete)\
                .execute()
            
            logger.info(f"✅ Successfully deleted {len(ids_to_delete)} oldest articles")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during cleanup of old articles: {e}")
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
            # First, cleanup old articles if we're adding new ones
            cleanup_success = self.cleanup_old_articles(len(articles))
            if not cleanup_success:
                logger.warning("⚠️  Cleanup failed, but continuing with insertion...")
            
            # Insert articles
            result = self.supabase.table(self.news_table).insert(articles).execute()
            
            # Debug: Check what was actually inserted
            if hasattr(result, 'data') and result.data:
                actual_inserted = len(result.data)
                logger.info(f"✅ Supabase confirmed {actual_inserted} articles were inserted")
            else:
                logger.warning("⚠️  Supabase insert result has no data - checking response structure")
                logger.info(f"Result object: {type(result)}")
            
            # Double-check by querying database count
            try:
                count_check = self.supabase.table(self.news_table).select('id', count='exact').execute()
                current_total = count_check.count if hasattr(count_check, 'count') else len(count_check.data)
                logger.info(f"📊 Current total articles in database: {current_total} (max: {self.max_articles_in_db})")
                
                # Warn if we're over the limit
                if current_total > self.max_articles_in_db:
                    logger.warning(f"⚠️  Database has {current_total} articles, which exceeds limit of {self.max_articles_in_db}")
                    
            except Exception as count_e:
                logger.warning(f"Could not verify database count: {count_e}")
            
            logger.info(f"✅ Successfully stored {len(articles)} new articles in Supabase")
            return True
            
        except Exception as e:
            logger.error(f"Error storing articles in Supabase: {e}")
            logger.error(f"Error type: {type(e)}")
            logger.error(f"Articles that failed to insert: {len(articles)}")
            
            # Log first article structure for debugging
            if articles:
                logger.error(f"Sample article structure: {list(articles[0].keys())}")
            
            raise
    
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
            
            # Get latest article
            latest_result = self.supabase.table(self.news_table)\
                .select('headline, source_name, published_at')\
                .order('created_at', desc=True)\
                .limit(1)\
                .execute()
            
            stats = {
                'total_articles': total_count,
                'latest_article': latest_result.data[0] if latest_result.data else None
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {'total_articles': 0, 'latest_article': None}
    
    def run_ingestion(self) -> bool:
        """
        Run the complete RSS news ingestion process
        
        Returns:
            Boolean indicating success
        """
        try:
            logger.info("🚀 Starting crypto RSS news ingestion from CoinDesk, Cointelegraph, and The Block...")
            logger.info(f"📊 Fetching latest {self.max_articles_per_feed} articles from each feed")
            logger.info(f"🎯 Maintaining maximum of {self.max_articles_in_db} articles in database")
            
            # Get database stats before ingestion
            stats_before = self.get_database_stats()
            logger.info(f"📊 Database contains {stats_before['total_articles']} articles BEFORE ingestion")
            
            # Fetch all articles from all feeds
            all_articles = self.fetch_all_feeds()
            
            if not all_articles:
                logger.info("No articles fetched from RSS feeds")
                return True
            
            # Log sample of fetched articles
            logger.info("Sample of fetched articles:")
            for i, article in enumerate(all_articles[:5], 1):
                source = article.get('source_name', 'Unknown')
                title = article.get('headline', 'No title')[:60]
                logger.info(f"  {i}. [{source}] {title}...")
            if len(all_articles) > 5:
                logger.info(f"  ... and {len(all_articles) - 5} more articles")
            
            # Prepare articles for storage (filters out duplicates)
            new_articles = self.prepare_articles_for_storage(all_articles)
            
            if not new_articles:
                logger.info("✅ All fetched articles already exist in database - no new articles to add")
                return True
            
            # Store in Supabase
            success = self.store_articles_in_supabase(new_articles)
            
            if success:
                # Get database stats after ingestion to verify
                stats_after = self.get_database_stats()
                articles_added = stats_after['total_articles'] - stats_before['total_articles']
                
                logger.info(f"📊 Database contains {stats_after['total_articles']} articles AFTER ingestion")
                logger.info(f"📈 Net change: +{articles_added} articles (Expected: +{len(new_articles)})")
                
                if articles_added != len(new_articles):
                    logger.warning(f"⚠️  MISMATCH: Expected to add {len(new_articles)} but database increased by {articles_added}")
                    logger.warning("This could indicate:")
                    logger.warning("  - Duplicate articles slipped through")
                    logger.warning("  - Database constraints rejecting some inserts")
                    logger.warning("  - Multiple instances of script running")
                else:
                    logger.info("✅ Database count matches expected additions")
                
                logger.info(f"✅ Crypto RSS ingestion completed successfully!")
            else:
                logger.error("❌ Crypto RSS ingestion failed during storage")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Fatal error during ingestion: {e}")
            return False


def run_once():
    """
    Run the ingestion process once
    """
    try:
        # Initialize the ingestion service
        ingestion_service = CryptoRSSNewsIngestion()
        
        # Run the ingestion
        success = ingestion_service.run_ingestion()
        
        if success:
            logger.info("🎉 Ingestion process completed successfully")
            return True
        else:
            logger.error("💥 Ingestion process failed")
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
    logger.info("🚀 Starting Crypto RSS News Ingestion Service")
    logger.info("📰 Will fetch CoinDesk, Cointelegraph & The Block RSS feeds every minute")
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
            logger.info(f"⏰ Starting ingestion at {iteration_start.strftime('%Y-%m-%d %H:%M:%S')}")
            
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
            sleep_time = max(interval_seconds - processing_time, 1)
            next_run = datetime.now().replace(second=0, microsecond=0) + timedelta(seconds=sleep_time)
            logger.info(f"💤 Sleeping for {sleep_time:.0f} seconds until next run at {next_run.strftime('%H:%M:%S')}...")
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
