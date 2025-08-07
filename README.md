# Web Crawler System

A scalable web crawler system designed to efficiently crawl, index, and store web page data while handling large-scale web crawling tasks with robustness and scalability. This implementation follows system design principles outlined by Alex Xu and industry best practices for distributed systems.

## 🚀 Features

- **Distributed Architecture**: Scalable design with multiple components working in coordination
- **Politeness Policies**: Respects robots.txt and implements rate limiting per domain
- **Duplicate Detection**: Multiple strategies for detecting and avoiding duplicate content
- **Storage Flexibility**: Support for both Cassandra (production) and file-based storage (development)
- **Fault Tolerance**: Comprehensive error handling and retry mechanisms
- **Real-time Monitoring**: Prometheus metrics and comprehensive logging
- **Configurable**: YAML-based configuration for easy customization

## 📋 Table of Contents

- [Architecture](#architecture)
- [System Requirements](#system-requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Components](#components)
- [Monitoring](#monitoring)
- [Scaling Considerations](#scaling-considerations)
- [Development](#development)
- [Contributing](#contributing)

## 🏗️ Architecture

The system follows a distributed architecture with the following components:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   URL Frontier  │    │     Fetcher     │    │     Parser      │
│   (Redis-based) │───▶│  (HTTP Client)  │───▶│  (Content Ext.) │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         ▲                        │                        │
         │                        │                        ▼
         │                        │              ┌─────────────────┐
         │                        │              │ Duplicate       │
         │                        │              │ Detector        │
         │                        │              └─────────────────┘
         │                        │                        │
         │                        ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Scheduler     │    │   Monitoring    │    │    Storage      │
│  (Coordinator)  │───▶│  (Prometheus)   │    │ (Cassandra/File)│
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Data Flow

1. **Seed URLs** are added to the URL Frontier
2. **Fetcher** retrieves URLs from the Frontier and downloads web pages
3. **Parser** processes pages, extracts content and links
4. **Duplicate Detector** filters out duplicate content
5. **Storage** persists crawled data
6. **Scheduler** coordinates the entire process and manages re-crawling

## 🔧 System Requirements

### Functional Requirements
- Crawl web pages starting from seed URLs
- Extract and store content (text, metadata, links)
- Respect robots.txt and crawl politely
- Detect and avoid duplicate content
- Support continuous crawling and re-crawling

### Non-Functional Requirements
- **Scalability**: Handle millions of web pages
- **Reliability**: Fault-tolerant with minimal data loss
- **Performance**: Efficient crawling with low latency
- **Extensibility**: Easy to add new features

### Prerequisites

- Python 3.8+
- Redis (for URL frontier and duplicate detection)
- Apache Cassandra (optional, for production storage)
- 4GB+ RAM recommended
- Network connectivity

## 📦 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/alexnthnz/web-crawler.git
cd web-crawler
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Setup Infrastructure

**Redis Installation:**
```bash
# macOS
brew install redis
brew services start redis

# Ubuntu
sudo apt-get install redis-server
sudo systemctl start redis

# Docker
docker run -d -p 6379:6379 redis:alpine
```

**Cassandra Installation (Optional):**
```bash
# Docker
docker run -d -p 9042:9042 cassandra:latest

# Or use file-based storage for development
```

### 4. Configure the System

Copy and modify the configuration file:
```bash
cp config.yaml config-local.yaml
# Edit config-local.yaml with your settings
```

## ⚙️ Configuration

The system is configured via YAML files. Here's the configuration structure:

```yaml
crawler:
  seed_urls:
    - https://example.com
    - https://example.org
  max_depth: 5
  politeness_delay: 1.0
  max_concurrent_requests: 10
  request_timeout: 30
  user_agent: "WebCrawler/1.0"
  respect_robots_txt: true

database:
  type: "file"  # or "cassandra"
  file:
    data_directory: "./data"
  cassandra:
    hosts: ["localhost"]
    port: 9042
    keyspace: "crawler_data"

redis:
  host: "localhost"
  port: 6379
  db: 0

logging:
  level: "INFO"
  file: "./logs/crawler.log"

monitoring:
  prometheus_port: 8000
  metrics_enabled: true
```

## 🚀 Usage

### Basic Usage

```bash
# Run with default configuration
python main.py

# Run with custom configuration
python main.py --config config-local.yaml

# Limit crawling
python main.py --max-pages 1000
python main.py --max-duration 3600  # 1 hour

# Test configuration
python main.py --dry-run
```

### Advanced Usage

```bash
# Run with Prometheus monitoring
python main.py --config config-production.yaml

# Monitor metrics at http://localhost:8000/metrics
```

### Example Output

```
2024-01-20 10:30:15 - INFO - === WEB CRAWLER STARTING ===
2024-01-20 10:30:15 - INFO - Configuration loaded from: config.yaml
2024-01-20 10:30:15 - INFO - Seed URLs: ['https://example.com']
2024-01-20 10:30:15 - INFO - Max depth: 5
2024-01-20 10:30:16 - INFO - Crawler scheduler initialized successfully
2024-01-20 10:30:16 - INFO - Added 1 seed URLs to frontier
2024-01-20 10:30:16 - INFO - Started crawling with 10 workers
2024-01-20 10:30:46 - INFO - Crawl Progress: Crawled=45, Stored=42, Queued=127, Errors=3, Rate=90.0 pages/min
```

## 🧩 Components

### URL Frontier
- **Purpose**: Manages URLs to be crawled with politeness policies
- **Features**: Per-domain queues, priority scheduling, rate limiting
- **Storage**: Redis-backed for persistence and distribution

### Fetcher
- **Purpose**: Downloads web pages efficiently
- **Features**: Robots.txt compliance, concurrent requests, timeout handling
- **Technology**: aiohttp for async HTTP operations

### Parser
- **Purpose**: Extracts structured data from HTML content
- **Features**: Content extraction, metadata parsing, link discovery
- **Technology**: BeautifulSoup for HTML parsing

### Duplicate Detector
- **Purpose**: Identifies and filters duplicate content
- **Strategies**: URL normalization, content hashing, fuzzy matching
- **Storage**: Redis-based hash storage

### Storage Layer
- **Purpose**: Persists crawled data
- **Backends**: Cassandra (production), File system (development)
- **Features**: Scalable storage, query optimization

### Scheduler
- **Purpose**: Coordinates crawling process
- **Features**: Worker management, statistics, graceful shutdown
- **Monitoring**: Real-time progress tracking

## 📊 Monitoring

### Prometheus Metrics

Available at `http://localhost:8000/metrics`:

- `crawler_urls_crawled_total`: Total URLs crawled
- `crawler_pages_stored_total`: Total pages stored
- `crawler_errors_total`: Total errors by type
- `crawler_response_time_seconds`: HTTP response times
- `crawler_queue_size`: URLs in queue
- `crawler_bytes_downloaded_total`: Total bytes downloaded

### Logging

- **Application logs**: `logs/crawler.log`
- **Error logs**: `logs/errors.log`
- **Structured logging**: JSON format available
- **Log rotation**: Automatic rotation and retention

### Statistics

Real-time statistics include:
- Crawl rate (pages/minute)
- Error rates by type
- Queue depth
- Storage metrics
- Response time percentiles

## 📈 Scaling Considerations

### Horizontal Scaling
- **Multiple Crawlers**: Run multiple crawler instances
- **Load Balancing**: Use Redis for distributed queues
- **Database Sharding**: Partition data by domain or date

### Performance Optimization
- **Concurrent Workers**: Adjust based on available resources
- **Request Batching**: Group requests for efficiency
- **Caching**: Cache robots.txt and DNS lookups
- **Storage Optimization**: Use appropriate database schemas

### Infrastructure Requirements

| Scale | URLs/day | Workers | Memory | Storage |
|-------|----------|---------|--------|---------|
| Small | <100K | 5-10 | 2GB | 10GB |
| Medium | 1M | 20-50 | 8GB | 100GB |
| Large | 10M+ | 100+ | 32GB+ | 1TB+ |

## 🛠️ Development

### Project Structure

```
web-crawler/
├── src/
│   ├── crawler/          # Core crawler components
│   │   ├── url_frontier.py
│   │   ├── fetcher.py
│   │   ├── parser.py
│   │   └── scheduler.py
│   ├── storage/          # Storage layer
│   │   ├── database.py
│   │   └── duplicate_detector.py
│   └── utils/            # Utilities
│       ├── config.py
│       ├── monitoring.py
│       └── logger.py
├── main.py               # Entry point
├── config.yaml          # Configuration
├── requirements.txt      # Dependencies
└── README.md
```

### Adding New Features

1. **New Parser**: Extend `ContentParser` class
2. **New Storage Backend**: Implement `StorageBackend` interface
3. **New Metrics**: Add to `MetricsCollector`
4. **New Filters**: Extend URL filtering logic

### Testing

```bash
# Test configuration
python main.py --dry-run

# Test with limited scope
python main.py --max-pages 10 --config test-config.yaml
```

## 🔧 Troubleshooting

### Common Issues

**Redis Connection Failed**
```bash
# Check Redis is running
redis-cli ping
# Should return PONG
```

**High Memory Usage**
- Reduce `max_concurrent_requests`
- Enable duplicate detection
- Check for memory leaks in parsing

**Slow Crawling**
- Increase `max_concurrent_requests`
- Reduce `politeness_delay`
- Check network connectivity

**Storage Errors**
- Verify database connectivity
- Check disk space
- Review database configuration

### Performance Tuning

1. **Adjust concurrency** based on target site capacity
2. **Tune politeness delay** for respectful crawling
3. **Optimize duplicate detection** for your use case
4. **Configure appropriate timeouts**

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guidelines
- Add comprehensive logging
- Include error handling
- Write clear documentation
- Add appropriate tests

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by "System Design Interview" by Alex Xu
- Built with modern Python async/await patterns
- Uses industry-standard tools and practices

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/alexnthnz/web-crawler/issues)
- **Documentation**: This README and inline code documentation
- **Community**: Contributions and discussions welcome