import os
import json
import logging
import time
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
import requests
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SentimentConsumer:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.ollama_url = os.getenv('OLLAMA_URL', 'http://ollama:11434')
        self.ollama_model = os.getenv('OLLAMA_MODEL', 'llama3')
        
        self.input_topic = 'financial_news_raw'
        self.output_topic = 'financial_sentiment_results'
        
        self.consumer = None
        self.producer = None
        
        # Initialize sentence transformer for embeddings
        logger.info("Loading sentence transformer model...")
        self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Initialize FAISS index
        self.dimension = 384  # Dimension of all-MiniLM-L6-v2
        self.index = faiss.IndexFlatL2(self.dimension)
        self.article_store = []
        
        logger.info("Sentiment Consumer initialized")
    
    def connect_kafka(self):
        """Connect to Kafka for consuming and producing"""
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.consumer = KafkaConsumer(
                    self.input_topic,
                    bootstrap_servers=self.kafka_servers,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    group_id='sentiment_consumer_group'
                )
                
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
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
    
    def add_to_vector_store(self, article):
        """Add article to FAISS vector store"""
        text = f"{article['title']} {article['description']}"
        embedding = self.embedder.encode([text])[0]
        
        self.index.add(np.array([embedding], dtype=np.float32))
        self.article_store.append({
            'title': article['title'],
            'description': article['description'],
            'source': article['source']
        })
        
        logger.debug(f"Added article to vector store. Total articles: {len(self.article_store)}")
    
    def retrieve_similar_articles(self, article, k=3):
        """Retrieve k most similar articles from FAISS index"""
        if len(self.article_store) < 1:
            return []
        
        text = f"{article['title']} {article['description']}"
        embedding = self.embedder.encode([text])[0]
        
        k_actual = min(k, len(self.article_store))
        distances, indices = self.index.search(
            np.array([embedding], dtype=np.float32), 
            k_actual
        )
        
        similar_articles = []
        for idx in indices[0]:
            if idx < len(self.article_store):
                similar_articles.append(self.article_store[idx])
        
        return similar_articles
    
    def analyze_sentiment_with_llm(self, article, context_articles):
        """Analyze sentiment using Ollama with RAG context"""
        # Build context from similar articles
        context = ""
        if context_articles:
            context = "Related financial news context:\n"
            for i, ctx_article in enumerate(context_articles, 1):
                context += f"{i}. {ctx_article['title']} - {ctx_article['description'][:100]}...\n"
            context += "\n"
        
        # Create prompt
        prompt = f"""You are a financial sentiment analyzer. Analyze the following financial news article and determine its market sentiment.

{context}Current Article:
Title: {article['title']}
Description: {article['description']}
Source: {article['source']}

Based on this article and the related context, provide:
1. A sentiment classification (BULLISH, BEARISH, or NEUTRAL)
2. A confidence score (0.0 to 1.0)
3. A brief 1-2 sentence justification for your classification

Respond in this exact JSON format:
{{
    "sentiment": "BULLISH/BEARISH/NEUTRAL",
    "confidence": 0.85,
    "justification": "Your justification here"
}}

JSON Response:"""
        
        try:
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": self.ollama_model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.3,
                        "top_p": 0.9
                    }
                },
                timeout=60
            )
            response.raise_for_status()
            
            result = response.json()
            response_text = result.get('response', '')
            
            # Parse JSON from response
            try:
                # Find JSON in response
                start_idx = response_text.find('{')
                end_idx = response_text.rfind('}') + 1
                if start_idx != -1 and end_idx > start_idx:
                    json_str = response_text[start_idx:end_idx]
                    sentiment_data = json.loads(json_str)
                    return sentiment_data
                else:
                    logger.warning("No JSON found in LLM response")
                    return self._default_sentiment()
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse LLM JSON response: {e}")
                return self._default_sentiment()
                
        except Exception as e:
            logger.error(f"Error calling Ollama API: {e}")
            return self._default_sentiment()
    
    def _default_sentiment(self):
        """Return default neutral sentiment"""
        return {
            "sentiment": "NEUTRAL",
            "confidence": 0.5,
            "justification": "Unable to determine sentiment due to processing error"
        }
    
    def process_article(self, article):
        """Process a single article through the pipeline"""
        try:
            logger.info(f"Processing article: {article['title'][:50]}...")
            
            # Retrieve similar articles for context
            similar_articles = self.retrieve_similar_articles(article, k=3)
            
            # Analyze sentiment with LLM
            sentiment_result = self.analyze_sentiment_with_llm(article, similar_articles)
            
            # Add article to vector store for future context
            self.add_to_vector_store(article)
            
            # Create enriched result
            enriched_article = {
                'title': article['title'],
                'description': article['description'],
                'source': article['source'],
                'url': article['url'],
                'published_at': article['published_at'],
                'sentiment': sentiment_result['sentiment'],
                'confidence': sentiment_result['confidence'],
                'justification': sentiment_result['justification'],
                'processed_at': datetime.utcnow().isoformat()
            }
            
            # Send to output topic
            self.producer.send(self.output_topic, value=enriched_article)
            logger.info(f"Sentiment: {sentiment_result['sentiment']} ({sentiment_result['confidence']:.2f})")
            
        except Exception as e:
            logger.error(f"Error processing article: {e}")
    
    def run(self):
        """Main consumer loop"""
        logger.info("Starting Sentiment Consumer")
        
        if not self.connect_kafka():
            logger.error("Cannot start consumer without Kafka connection")
            return
        
        try:
            logger.info(f"Consuming from topic: {self.input_topic}")
            for message in self.consumer:
                article = message.value
                self.process_article(article)
                
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        except Exception as e:
            logger.error(f"Unexpected error in consumer loop: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.close()
            logger.info("Consumer closed")


if __name__ == "__main__":
    consumer = SentimentConsumer()
    consumer.run()