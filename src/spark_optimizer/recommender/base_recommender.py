"""Base recommender interface."""

from abc import ABC, abstractmethod
from typing import Dict, Optional


class BaseRecommender(ABC):
    """Abstract base class for resource recommenders."""

    def __init__(self, config: Optional[Dict] = None):
        """Initialize the recommender.

        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}

    @abstractmethod
    def recommend(self, job_requirements: Dict) -> Dict:
        """Generate resource recommendations for a job.

        Args:
            job_requirements: Dictionary containing job requirements
                (e.g., input size, job type, etc.)

        Returns:
            Dictionary containing recommended configuration
        """
        pass

    @abstractmethod
    def train(self, historical_jobs: list):
        """Train the recommender with historical job data.

        Args:
            historical_jobs: List of historical job data
        """
        pass

    def _create_recommendation_response(
        self,
        executor_cores: int,
        executor_memory_mb: int,
        num_executors: int,
        driver_memory_mb: int,
        confidence: float = 1.0,
        metadata: Optional[Dict] = None,
    ) -> Dict:
        """Create a standardized recommendation response.

        Args:
            executor_cores: Number of cores per executor
            executor_memory_mb: Memory per executor in MB
            num_executors: Number of executors
            driver_memory_mb: Driver memory in MB
            confidence: Confidence score (0.0 to 1.0)
            metadata: Additional metadata

        Returns:
            Standardized recommendation dictionary
        """
        return {
            "configuration": {
                "executor_cores": executor_cores,
                "executor_memory_mb": executor_memory_mb,
                "num_executors": num_executors,
                "driver_memory_mb": driver_memory_mb,
            },
            "confidence": confidence,
            "metadata": metadata or {},
        }
