import os
import requests
import time
from datetime import datetime
from supabase import create_client, Client
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FinnhubCryptoIngestion:
    def __init__(self):
        """Initialize the service with environment variables"""
        # Get credentials from environment variables
        self.api_key = os.getenv('FINNHUB_API_KEY')
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        # Validate environment variables
        if not all([self.api_key, supabase_url, supabase_key]):
            raise ValueError("Missing required environment variables")
        
        # Initialize Supabase client
        self.supabase = create_client(supabase_url, supabase_key)
        
        # Configuration
        self.base_url = "https://finnhub.io/api/v1/news"
        self.table_name = "crypto_news"
        self.min_id = 10  # Starting minId
        
    def get_last_news_id(self):
        """Get the highest news ID from database to use as next minId"""
        try:
            result = self.supabase.table(self.table_name)\
                .select('id')\
                .order('id', desc=True)\
                .limit(1)\
                .execute()
            
            if result.data and len(result.data) > 0:
                last_id = result.data[0]['id']
                logger.info(f"Last ID in database: {last_id}")
                return last_id
            else:
                logger.info(f"No previous records, using default minId: {self.min_id}")
                return self.min_id
                
        except Exception as e:
            logger.warning(f"Could not get last ID, using default: {e}")
            return self.min_id
    
    def fetch_crypto_news(self):
        """Fetch crypto news from Finnhub API"""
        try:
            # Get the last ID to use as minId
            min_id = self.get_last_news_id()
            
            # API parameters - CRYPTO ONLY
            params = {
                'category': 'crypto',  # CRYPTO CATEGORY ONLY
                'minId': min_id,
                'token': self.api_key
            }
            
            logger.info(f"Fetching CRYPTO news with minId: {min_id}")
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            news_data = response.json()
            logger.info(f"Fetched {len(news_data)} crypto news articles")
            
            # Sort by datetime and take only latest 10
            news_data.sort(key=lambda x: x.get('datetime', 0), reverse=True)
            latest_10 = news_data[:10]
            
            return latest_10
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching news: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return []
    
    def check_duplicates(self, news_ids):
        """Check which articles already exist in database"""
        if not news_ids:
            return set()
        
        try:
            result = self.supabase.table(self.table_name)\
                .select('id')\
                .in_('id', news_ids)\
                .execute()
            
            existing = {item['id'] for item in result.data} if result.data else set()
            logger.info(f"Found {len(existing)} duplicates")
            return existing
            
        except Exception as e:
            logger.error(f"Error checking duplicates: {e}")
            return set()
    
    def store_news(self, articles):
        """Store news articles in Supabase crypto_news table"""
        if not articles:
            logger.info("No new articles to store")
            return True
        
        try:
            # Check for duplicates
            article_ids = [a['id'] for a in articles]
            existing_ids = self.check_duplicates(article_ids)
            
            # Filter out duplicates
            new_articles = []
            for article in articles:
                if article['id'] not in existing_ids:
                    # Prepare article with only required fields
                    formatted = {
                        'id': article['id'],
                        'category': article.get('category', 'crypto'),
                        'datetime': article.get('datetime'),
                        'headline': article.get('headline', ''),
                        'source': article.get('source', ''),
                        'summary': article.get('summary', ''),
                        'url': article.get('url', ''),
                        'ingested_at': datetime.now().isoformat()
                    }
                    new_articles.append(formatted)
            
            if not new_articles:
                logger.info("All articles already exist - no new articles to add")
                return True
            
            # Insert new articles into crypto_news table
            result = self.supabase.table(self.table_name).insert(new_articles).execute()
            logger.info(f"✅ Stored {len(new_articles)} new CRYPTO articles")
            
            # Log first few headlines
            for i, article in enumerate(new_articles[:3], 1):
                logger.info(f"  {i}. {article['headline'][:60]}...")
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing articles: {e}")
            return False
    
    def run(self):
        """Run the ingestion process"""
        try:
            logger.info("🪙 Starting CRYPTO news ingestion...")
            
            # Fetch crypto news
            articles = self.fetch_crypto_news()
            
            if not articles:
                logger.info("No crypto articles fetched")
                return True
            
            # Store in database
            success = self.store_news(articles)
            
            if success:
                logger.info("✅ Crypto ingestion completed successfully")
            else:
                logger.error("❌ Crypto ingestion failed")
            
            return success
            
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            return False


def main():
    """Main function - runs continuously every minute"""
    logger.info("=" * 60)
    logger.info("🪙 Finnhub CRYPTO News Ingestion Service")
    logger.info("📊 Fetches latest 10 CRYPTO articles every minute")
    logger.info("📍 Stores in 'crypto_news' table")
    logger.info("=" * 60)
    
    # Check for run mode
    run_mode = os.getenv('RUN_MODE', 'continuous').lower()
    
    # Initialize service
    try:
        service = FinnhubCryptoIngestion()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Required environment variables:")
        logger.error("- FINNHUB_API_KEY")
        logger.error("- SUPABASE_URL")
        logger.error("- SUPABASE_KEY")
        exit(1)
    
    # Run once mode
    if run_mode == 'once':
        logger.info("Running in ONCE mode")
        success = service.run()
        exit(0 if success else 1)
    
    # Continuous mode
    logger.info("Running in CONTINUOUS mode")
    failures = 0
    max_failures = 5
    
    while True:
        try:
            start_time = datetime.now()
            logger.info(f"\n{'=' * 50}")
            logger.info(f"⏰ Run started at {start_time.strftime('%H:%M:%S')}")
            
            # Run ingestion
            if service.run():
                failures = 0
            else:
                failures += 1
                if failures >= max_failures:
                    logger.error(f"Too many failures ({max_failures}). Exiting...")
                    exit(1)
            
            # Calculate sleep time
            elapsed = (datetime.now() - start_time).total_seconds()
            sleep_time = max(60 - elapsed, 1)
            
            logger.info(f"⏱️  Took {elapsed:.1f}s. Sleeping {sleep_time:.1f}s...")
            time.sleep(sleep_time)
            
        except KeyboardInterrupt:
            logger.info("\n⛔ Shutting down...")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            failures += 1
            if failures >= max_failures:
                exit(1)
            time.sleep(60)


if __name__ == "__main__":
    main()
