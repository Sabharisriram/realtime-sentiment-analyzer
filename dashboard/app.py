import os
import json
import logging
import threading
from collections import deque
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Store for analyzed articles (last 100)
articles_store = deque(maxlen=100)
store_lock = threading.Lock()


def kafka_consumer_thread():
    """Background thread to consume sentiment results"""
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    topic = 'financial_sentiment_results'
    
    logger.info("Starting Kafka consumer thread...")
    
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=kafka_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='dashboard_consumer_group'
            )
            
            logger.info(f"Connected to Kafka, consuming from {topic}")
            
            for message in consumer:
                article = message.value
                with store_lock:
                    articles_store.append(article)
                logger.info(f"Stored article: {article['title'][:50]}... - {article['sentiment']}")
                
        except Exception as e:
            logger.error(f"Kafka consumer error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                import time
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect to Kafka after all retries")
                break


# Start Kafka consumer in background thread
consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
consumer_thread.start()


@app.get("/api/articles")
async def get_articles():
    """API endpoint to fetch analyzed articles"""
    with store_lock:
        # Return most recent articles first
        return list(reversed(list(articles_store)))


@app.get("/api/stats")
async def get_stats():
    """API endpoint to fetch sentiment statistics"""
    with store_lock:
        articles = list(articles_store)
    
    if not articles:
        return {
            "total": 0,
            "bullish": 0,
            "bearish": 0,
            "neutral": 0,
            "bullish_pct": 0,
            "bearish_pct": 0,
            "neutral_pct": 0
        }
    
    total = len(articles)
    bullish = sum(1 for a in articles if a['sentiment'] == 'BULLISH')
    bearish = sum(1 for a in articles if a['sentiment'] == 'BEARISH')
    neutral = sum(1 for a in articles if a['sentiment'] == 'NEUTRAL')
    
    return {
        "total": total,
        "bullish": bullish,
        "bearish": bearish,
        "neutral": neutral,
        "bullish_pct": round((bullish / total) * 100, 1),
        "bearish_pct": round((bearish / total) * 100, 1),
        "neutral_pct": round((neutral / total) * 100, 1)
    }


@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    """Serve the dashboard HTML"""
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Real-Time Financial Sentiment Analyzer</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        
        header {
            text-align: center;
            color: white;
            margin-bottom: 30px;
        }
        
        h1 {
            font-size: 2.5rem;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        
        .subtitle {
            font-size: 1.1rem;
            opacity: 0.9;
        }
        
        .dashboard {
            display: grid;
            grid-template-columns: 1fr 2fr;
            gap: 20px;
            margin-bottom: 20px;
        }
        
        @media (max-width: 968px) {
            .dashboard {
                grid-template-columns: 1fr;
            }
        }
        
        .card {
            background: white;
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }
        
        .card h2 {
            color: #333;
            margin-bottom: 20px;
            font-size: 1.5rem;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 15px;
            margin-bottom: 20px;
        }
        
        .stat-box {
            text-align: center;
            padding: 20px;
            border-radius: 8px;
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        }
        
        .stat-box.bullish {
            background: linear-gradient(135deg, #d4fc79 0%, #96e6a1 100%);
        }
        
        .stat-box.bearish {
            background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
        }
        
        .stat-box.neutral {
            background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
        }
        
        .stat-number {
            font-size: 2rem;
            font-weight: bold;
            color: #333;
        }
        
        .stat-label {
            font-size: 0.9rem;
            color: #666;
            margin-top: 5px;
        }
        
        .chart-container {
            height: 300px;
            display: flex;
            align-items: flex-end;
            justify-content: space-around;
            padding: 20px 0;
            border-top: 2px solid #e0e0e0;
            margin-top: 20px;
        }
        
        .bar {
            width: 80px;
            background: linear-gradient(to top, #667eea, #764ba2);
            border-radius: 8px 8px 0 0;
            position: relative;
            transition: height 0.3s ease;
            min-height: 10px;
        }
        
        .bar-label {
            position: absolute;
            bottom: -30px;
            left: 0;
            right: 0;
            text-align: center;
            font-size: 0.9rem;
            color: #666;
            font-weight: 600;
        }
        
        .bar-value {
            position: absolute;
            top: -25px;
            left: 0;
            right: 0;
            text-align: center;
            font-size: 0.9rem;
            font-weight: bold;
            color: #667eea;
        }
        
        .articles-list {
            max-height: 600px;
            overflow-y: auto;
        }
        
        .article {
            padding: 20px;
            border-bottom: 1px solid #e0e0e0;
            transition: background-color 0.2s;
        }
        
        .article:hover {
            background-color: #f5f7fa;
        }
        
        .article:last-child {
            border-bottom: none;
        }
        
        .article-header {
            display: flex;
            justify-content: space-between;
            align-items: start;
            margin-bottom: 10px;
        }
        
        .article-title {
            font-size: 1.1rem;
            font-weight: 600;
            color: #333;
            margin-bottom: 8px;
            flex: 1;
        }
        
        .sentiment-badge {
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.85rem;
            font-weight: bold;
            margin-left: 15px;
            white-space: nowrap;
        }
        
        .sentiment-badge.bullish {
            background: #d4fc79;
            color: #2d5016;
        }
        
        .sentiment-badge.bearish {
            background: #fa709a;
            color: #5a1428;
        }
        
        .sentiment-badge.neutral {
            background: #a8edea;
            color: #1a4d4a;
        }
        
        .article-meta {
            display: flex;
            gap: 15px;
            font-size: 0.85rem;
            color: #666;
            margin-bottom: 10px;
        }
        
        .article-description {
            color: #555;
            line-height: 1.6;
            margin-bottom: 10px;
        }
        
        .article-justification {
            background: #f5f7fa;
            padding: 12px;
            border-left: 3px solid #667eea;
            border-radius: 4px;
            font-size: 0.9rem;
            color: #444;
            font-style: italic;
            margin-top: 10px;
        }
        
        .confidence {
            display: inline-block;
            font-size: 0.85rem;
            color: #666;
            margin-left: 10px;
        }
        
        .loading {
            text-align: center;
            padding: 40px;
            color: #666;
        }
        
        .spinner {
            border: 3px solid #f3f3f3;
            border-top: 3px solid #667eea;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin: 0 auto 20px;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: #999;
        }
        
        .empty-state-icon {
            font-size: 4rem;
            margin-bottom: 20px;
        }
        
        a {
            color: #667eea;
            text-decoration: none;
        }
        
        a:hover {
            text-decoration: underline;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📈 Real-Time Financial Sentiment Analyzer</h1>
            <p class="subtitle">AI-Powered Market Sentiment Analysis with RAG</p>
        </header>
        
        <div class="dashboard">
            <div class="card">
                <h2>Sentiment Overview</h2>
                <div class="stats-grid">
                    <div class="stat-box bullish">
                        <div class="stat-number" id="bullish-count">0</div>
                        <div class="stat-label">Bullish</div>
                    </div>
                    <div class="stat-box bearish">
                        <div class="stat-number" id="bearish-count">0</div>
                        <div class="stat-label">Bearish</div>
                    </div>
                    <div class="stat-box neutral">
                        <div class="stat-number" id="neutral-count">0</div>
                        <div class="stat-label">Neutral</div>
                    </div>
                </div>
                
                <div class="chart-container">
                    <div class="bar" id="bar-bullish">
                        <div class="bar-value" id="bar-bullish-value">0%</div>
                        <div class="bar-label">Bullish</div>
                    </div>
                    <div class="bar" id="bar-bearish">
                        <div class="bar-value" id="bar-bearish-value">0%</div>
                        <div class="bar-label">Bearish</div>
                    </div>
                    <div class="bar" id="bar-neutral">
                        <div class="bar-value" id="bar-neutral-value">0%</div>
                        <div class="bar-label">Neutral</div>
                    </div>
                </div>
            </div>
            
            <div class="card">
                <h2>Latest Analysis <span style="font-size: 0.9rem; font-weight: normal; color: #666;" id="total-count">(0 articles)</span></h2>
                <div class="articles-list" id="articles-list">
                    <div class="loading">
                        <div class="spinner"></div>
                        <p>Waiting for sentiment analysis...</p>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        let lastUpdateTime = 0;
        
        async function fetchData() {
            try {
                const [articlesRes, statsRes] = await Promise.all([
                    fetch('/api/articles'),
                    fetch('/api/stats')
                ]);
                
                const articles = await articlesRes.json();
                const stats = await statsRes.json();
                
                updateStats(stats);
                updateArticles(articles);
                
            } catch (error) {
                console.error('Error fetching data:', error);
            }
        }
        
        function updateStats(stats) {
            document.getElementById('bullish-count').textContent = stats.bullish;
            document.getElementById('bearish-count').textContent = stats.bearish;
            document.getElementById('neutral-count').textContent = stats.neutral;
            document.getElementById('total-count').textContent = `(${stats.total} articles)`;
            
            // Update chart bars
            const maxHeight = 250;
            document.getElementById('bar-bullish').style.height = 
                `${(stats.bullish_pct / 100) * maxHeight}px`;
            document.getElementById('bar-bearish').style.height = 
                `${(stats.bearish_pct / 100) * maxHeight}px`;
            document.getElementById('bar-neutral').style.height = 
                `${(stats.neutral_pct / 100) * maxHeight}px`;
            
            document.getElementById('bar-bullish-value').textContent = 
                `${stats.bullish_pct}%`;
            document.getElementById('bar-bearish-value').textContent = 
                `${stats.bearish_pct}%`;
            document.getElementById('bar-neutral-value').textContent = 
                `${stats.neutral_pct}%`;
        }
        
        function updateArticles(articles) {
            const container = document.getElementById('articles-list');
            
            if (articles.length === 0) {
                container.innerHTML = `
                    <div class="empty-state">
                        <div class="empty-state-icon">📰</div>
                        <h3>No articles analyzed yet</h3>
                        <p>Waiting for financial news to be processed...</p>
                    </div>
                `;
                return;
            }
            
            container.innerHTML = articles.map(article => {
                const sentiment = article.sentiment.toLowerCase();
                const publishedDate = new Date(article.published_at);
                const formattedDate = publishedDate.toLocaleDateString() + ' ' + 
                                     publishedDate.toLocaleTimeString();
                
                return `
                    <div class="article">
                        <div class="article-header">
                            <div class="article-title">
                                <a href="${article.url}" target="_blank">${article.title}</a>
                            </div>
                            <div>
                                <span class="sentiment-badge ${sentiment}">${article.sentiment}</span>
                                <span class="confidence">Confidence: ${(article.confidence * 100).toFixed(0)}%</span>
                            </div>
                        </div>
                        <div class="article-meta">
                            <span>📰 ${article.source}</span>
                            <span>🕐 ${formattedDate}</span>
                        </div>
                        <div class="article-description">${article.description}</div>
                        <div class="article-justification">
                            <strong>AI Analysis:</strong> ${article.justification}
                        </div>
                    </div>
                `;
            }).join('');
        }
        
        // Fetch data every 3 seconds
        fetchData();
        setInterval(fetchData, 3000);
    </script>
</body>
</html>
    """
    return HTMLResponse(content=html_content)