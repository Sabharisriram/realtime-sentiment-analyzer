import os
import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FinancialNewsProducer:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.news_api_key = os.getenv('NEWS_API_KEY')
        self.fetch_interval = int(os.getenv('FETCH_INTERVAL', 300))
        self.topic = 'financial_news_raw'
        
        if not self.news_api_key:
            raise ValueError("NEWS_API_KEY environment variable not set")
        
        self.producer = None
        self.seen_urls = set()
        
    def connect_kafka(self):
        """Establish connection to Kafka with retry logic"""
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    max_block_ms=10000
                )
                logger.info(f"Successfully connected to Kafka at {self.kafka_servers}")
                return True
            except Exception as e:
                logger.warning(f"Kafka connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to Kafka after all retries")
                    return False
    
    def fetch_financial_news(self):
        """Fetch financial news from NewsAPI"""
        url = 'https://newsapi.org/v2/everything'
        params = {
            'q': 'finance OR stocks OR market OR economy OR trading',
            'language': 'en',
            'sortBy': 'publishedAt',
            'pageSize': 20,
            'apiKey': self.news_api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') == 'ok':
                articles = data.get('articles', [])
                logger.info(f"Fetched {len(articles)} articles from NewsAPI")
                return articles
            else:
                logger.error(f"API returned error status: {data.get('message')}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching news: {e}")
            return []
    
    def process_and_send(self, articles):
        """Process articles and send to Kafka"""
        new_articles = 0
        
        for article in articles:
            url = article.get('url')
            if not url or url in self.seen_urls:
                continue
            
            # Extract relevant information
            news_item = {
                'title': article.get('title', ''),
                'description': article.get('description', ''),
                'content': article.get('content', ''),
                'source': article.get('source', {}).get('name', 'Unknown'),
                'url': url,
                'published_at': article.get('publishedAt', ''),
                'fetched_at': datetime.utcnow().isoformat()
            }
            
            # Skip if no meaningful content
            if not news_item['title'] or not news_item['description']:
                continue
            
            try:
                # Send to Kafka
                future = self.producer.send(self.topic, value=news_item)
                future.get(timeout=10)
                
                self.seen_urls.add(url)
                new_articles += 1
                logger.info(f"Sent article: {news_item['title'][:50]}...")
                
            except Exception as e:
                logger.error(f"Error sending article to Kafka: {e}")
        
        logger.info(f"Processed {new_articles} new articles")
    
    def run(self):
        """Main loop to continuously fetch and produce news"""
        logger.info("Starting Financial News Producer")
        
        if not self.connect_kafka():
            logger.error("Cannot start producer without Kafka connection")
            return
        
        try:
            while True:
                logger.info("Fetching latest financial news...")
                articles = self.fetch_financial_news()
                
                if articles:
                    self.process_and_send(articles)
                else:
                    logger.warning("No articles fetched this cycle")
                
                logger.info(f"Sleeping for {self.fetch_interval} seconds...")
                time.sleep(self.fetch_interval)
                
        except KeyboardInterrupt:
            logger.info("Shutting down producer...")
        except Exception as e:
            logger.error(f"Unexpected error in producer loop: {e}")
        finally:
            if self.producer:
                self.producer.close()
                logger.info("Producer closed")


if __name__ == "__main__":
    producer = FinancialNewsProducer()
    producer.run()