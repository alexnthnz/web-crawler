"""
Monitoring and metrics collection for the web crawler system.
"""

import time
import logging
import asyncio
from typing import Dict, Optional, Any, List
from dataclasses import dataclass, field
from datetime import datetime
import json
from pathlib import Path

try:
    from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, generate_latest
    from prometheus_client import start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


@dataclass
class MetricPoint:
    """Individual metric data point."""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class Metric:
    """Metric container with history."""
    name: str
    description: str
    metric_type: str  # counter, gauge, histogram
    points: List[MetricPoint] = field(default_factory=list)
    current_value: float = 0.0


class MetricsCollector:
    """Collects and manages crawler metrics."""
    
    def __init__(self, enable_prometheus: bool = False, prometheus_port: int = 8000):
        self.logger = logging.getLogger(__name__)
        self.metrics: Dict[str, Metric] = {}
        self.enable_prometheus = enable_prometheus and PROMETHEUS_AVAILABLE
        self.prometheus_port = prometheus_port
        
        # Prometheus metrics
        self.prometheus_registry = None
        self.prometheus_metrics = {}
        
        if self.enable_prometheus:
            self._setup_prometheus()
    
    def _setup_prometheus(self):
        """Setup Prometheus metrics."""
        if not PROMETHEUS_AVAILABLE:
            self.logger.warning("Prometheus client not available, disabling Prometheus metrics")
            self.enable_prometheus = False
            return
        
        self.prometheus_registry = CollectorRegistry()
        
        # Define Prometheus metrics
        self.prometheus_metrics = {
            'urls_crawled_total': Counter(
                'crawler_urls_crawled_total',
                'Total number of URLs crawled',
                registry=self.prometheus_registry
            ),
            'pages_stored_total': Counter(
                'crawler_pages_stored_total',
                'Total number of pages stored',
                registry=self.prometheus_registry
            ),
            'errors_total': Counter(
                'crawler_errors_total',
                'Total number of crawl errors',
                ['error_type'],
                registry=self.prometheus_registry
            ),
            'duplicates_skipped_total': Counter(
                'crawler_duplicates_skipped_total',
                'Total number of duplicate pages skipped',
                registry=self.prometheus_registry
            ),
            'response_time_seconds': Histogram(
                'crawler_response_time_seconds',
                'Response time for HTTP requests',
                registry=self.prometheus_registry
            ),
            'queue_size': Gauge(
                'crawler_queue_size',
                'Number of URLs in queue',
                registry=self.prometheus_registry
            ),
            'active_workers': Gauge(
                'crawler_active_workers',
                'Number of active crawler workers',
                registry=self.prometheus_registry
            ),
            'bytes_downloaded_total': Counter(
                'crawler_bytes_downloaded_total',
                'Total bytes downloaded',
                registry=self.prometheus_registry
            )
        }
        
        self.logger.info("Prometheus metrics initialized")
    
    async def start_prometheus_server(self):
        """Start Prometheus metrics HTTP server."""
        if not self.enable_prometheus:
            return
        
        try:
            start_http_server(self.prometheus_port, registry=self.prometheus_registry)
            self.logger.info(f"Prometheus metrics server started on port {self.prometheus_port}")
        except Exception as e:
            self.logger.error(f"Failed to start Prometheus server: {e}")
    
    def record_metric(self, name: str, value: float, labels: Optional[Dict[str, str]] = None,
                     description: str = "", metric_type: str = "gauge"):
        """Record a metric value."""
        labels = labels or {}
        
        # Store in internal metrics
        if name not in self.metrics:
            self.metrics[name] = Metric(
                name=name,
                description=description,
                metric_type=metric_type
            )
        
        metric = self.metrics[name]
        point = MetricPoint(
            timestamp=time.time(),
            value=value,
            labels=labels
        )
        metric.points.append(point)
        metric.current_value = value
        
        # Keep only recent points (last 1000)
        if len(metric.points) > 1000:
            metric.points = metric.points[-1000:]
        
        # Update Prometheus metrics
        if self.enable_prometheus and name in self.prometheus_metrics:
            prom_metric = self.prometheus_metrics[name]
            
            if hasattr(prom_metric, 'inc'):  # Counter
                if labels:
                    prom_metric.labels(**labels).inc(value)
                else:
                    prom_metric.inc(value)
            elif hasattr(prom_metric, 'observe'):  # Histogram
                prom_metric.observe(value)
            elif hasattr(prom_metric, 'set'):  # Gauge
                prom_metric.set(value)
    
    def increment_counter(self, name: str, labels: Optional[Dict[str, str]] = None,
                         description: str = ""):
        """Increment a counter metric."""
        current_value = 0
        if name in self.metrics:
            current_value = self.metrics[name].current_value
        
        self.record_metric(name, current_value + 1, labels, description, "counter")
    
    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None,
                  description: str = ""):
        """Set a gauge metric value."""
        self.record_metric(name, value, labels, description, "gauge")
    
    def observe_histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None,
                         description: str = ""):
        """Record a histogram observation."""
        self.record_metric(name, value, labels, description, "histogram")
    
    def get_metric(self, name: str) -> Optional[Metric]:
        """Get a metric by name."""
        return self.metrics.get(name)
    
    def get_all_metrics(self) -> Dict[str, Metric]:
        """Get all metrics."""
        return self.metrics.copy()
    
    def get_current_values(self) -> Dict[str, float]:
        """Get current values of all metrics."""
        return {name: metric.current_value for name, metric in self.metrics.items()}
    
    def export_metrics_json(self, file_path: str):
        """Export metrics to JSON file."""
        try:
            export_data = {
                'export_time': datetime.utcnow().isoformat(),
                'metrics': {}
            }
            
            for name, metric in self.metrics.items():
                export_data['metrics'][name] = {
                    'description': metric.description,
                    'type': metric.metric_type,
                    'current_value': metric.current_value,
                    'points': [
                        {
                            'timestamp': point.timestamp,
                            'value': point.value,
                            'labels': point.labels
                        }
                        for point in metric.points[-100:]  # Last 100 points
                    ]
                }
            
            with open(file_path, 'w') as f:
                json.dump(export_data, f, indent=2)
            
            self.logger.info(f"Metrics exported to {file_path}")
            
        except Exception as e:
            self.logger.error(f"Error exporting metrics: {e}")


class CrawlerMonitor:
    """High-level monitoring interface for the crawler."""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector
        self.logger = logging.getLogger(__name__)
        self.start_time = time.time()
    
    def record_url_crawled(self, url: str, status_code: int, response_time: float):
        """Record a URL crawl event."""
        self.metrics.increment_counter('urls_crawled_total', description='URLs crawled')
        self.metrics.observe_histogram('response_time_seconds', response_time, 
                                     description='HTTP response time')
        
        # Record by status code
        status_label = {'status_code': str(status_code)}
        self.metrics.increment_counter('http_responses_total', status_label, 
                                     'HTTP responses by status code')
    
    def record_page_stored(self, url: str, content_size: int):
        """Record a page storage event."""
        self.metrics.increment_counter('pages_stored_total', description='Pages stored')
        self.metrics.increment_counter('bytes_downloaded_total', description='Bytes downloaded')
    
    def record_error(self, error_type: str, error_message: str = ""):
        """Record an error event."""
        labels = {'error_type': error_type}
        self.metrics.increment_counter('errors_total', labels, 'Crawl errors')
    
    def record_duplicate_skipped(self, url: str, duplicate_type: str):
        """Record a duplicate page skip event."""
        labels = {'duplicate_type': duplicate_type}
        self.metrics.increment_counter('duplicates_skipped_total', labels, 
                                     'Duplicate pages skipped')
    
    def update_queue_size(self, size: int):
        """Update the queue size metric."""
        self.metrics.set_gauge('queue_size', size, description='URLs in queue')
    
    def update_active_workers(self, count: int):
        """Update the active workers count."""
        self.metrics.set_gauge('active_workers', count, description='Active workers')
    
    def update_crawler_stats(self, stats: Dict[str, Any]):
        """Update multiple crawler statistics at once."""
        for key, value in stats.items():
            if isinstance(value, (int, float)):
                self.metrics.set_gauge(f"crawler_{key}", value, 
                                     description=f"Crawler stat: {key}")
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of all metrics."""
        current_values = self.metrics.get_current_values()
        runtime = time.time() - self.start_time
        
        return {
            'runtime_seconds': runtime,
            'metrics': current_values,
            'rates': {
                'urls_per_second': current_values.get('urls_crawled_total', 0) / runtime if runtime > 0 else 0,
                'pages_per_minute': current_values.get('pages_stored_total', 0) / (runtime / 60) if runtime > 0 else 0,
            }
        }


# Global monitoring instance
_global_monitor: Optional[CrawlerMonitor] = None


def initialize_monitoring(enable_prometheus: bool = False, prometheus_port: int = 8000) -> CrawlerMonitor:
    """Initialize global monitoring."""
    global _global_monitor
    
    metrics_collector = MetricsCollector(enable_prometheus, prometheus_port)
    _global_monitor = CrawlerMonitor(metrics_collector)
    
    return _global_monitor


def get_monitor() -> Optional[CrawlerMonitor]:
    """Get the global monitor instance."""
    return _global_monitor


async def start_monitoring_server(monitor: CrawlerMonitor):
    """Start monitoring server."""
    if monitor.metrics.enable_prometheus:
        await monitor.metrics.start_prometheus_server()