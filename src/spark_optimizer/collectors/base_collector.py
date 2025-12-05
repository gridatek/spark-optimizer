"""Base collector interface for Spark job data collection."""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from datetime import datetime


class BaseCollector(ABC):
    """Abstract base class for all collectors."""

    def __init__(self, config: Optional[Dict] = None):
        """Initialize the collector with configuration.

        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}

    @abstractmethod
    def collect(self) -> List[Dict]:
        """Collect job data from the source.

        Returns:
            List of dictionaries containing job metrics
        """
        pass

    @abstractmethod
    def validate_config(self) -> bool:
        """Validate the collector configuration.

        Returns:
            True if configuration is valid, False otherwise
        """
        pass

    def _parse_timestamp(self, timestamp: int) -> datetime:
        """Parse timestamp from milliseconds to datetime.

        Args:
            timestamp: Timestamp in milliseconds

        Returns:
            datetime object
        """
        return datetime.fromtimestamp(timestamp / 1000.0)
