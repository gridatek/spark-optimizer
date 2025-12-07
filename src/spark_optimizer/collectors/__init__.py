"""Data collectors for Spark applications from various sources."""

from .base_collector import BaseCollector
from .history_server_collector import HistoryServerCollector
from .event_log_collector import EventLogCollector
from .metrics_collector import MetricsCollector

# Cloud provider collectors (optional dependencies)
collectors = [
    "BaseCollector",
    "HistoryServerCollector",
    "EventLogCollector",
    "MetricsCollector",
]

# Try to import EMR collector (requires boto3)
try:
    from .emr_collector import EMRCollector

    collectors.append("EMRCollector")
except ImportError:
    pass

# Try to import Databricks collector (requires requests)
try:
    from .databricks_collector import DatabricksCollector

    collectors.append("DatabricksCollector")
except ImportError:
    pass

__all__ = collectors
