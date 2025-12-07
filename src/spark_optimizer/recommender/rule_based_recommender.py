"""Rule-based recommender using expert knowledge and best practices."""

from typing import Dict, List, Optional
from .base_recommender import BaseRecommender
from ..analyzer.rule_engine import (
    RuleEngine,
    get_default_rules,
    Recommendation,
    Severity,
)


class RuleBasedRecommender(BaseRecommender):
    """Recommends resources based on rule-based optimization."""

    def __init__(self, config: Optional[Dict] = None):
        """Initialize rule-based recommender.

        Args:
            config: Optional configuration dictionary
        """
        super().__init__(config)
        self.engine = RuleEngine()

        # Register default rules
        self.engine.register_rules(get_default_rules())

    def recommend(
        self,
        input_size_bytes: int,
        job_type: Optional[str] = None,
        sla_minutes: Optional[int] = None,
        budget_dollars: Optional[float] = None,
        priority: str = "balanced",
    ) -> Dict:
        """Generate recommendations based on rules.

        This method provides baseline recommendations and applies
        optimization rules for fine-tuning.

        Args:
            input_size_bytes: Expected input data size in bytes
            job_type: Type of job (e.g., etl, ml, streaming)
            sla_minutes: Maximum acceptable duration in minutes
            budget_dollars: Maximum acceptable cost in dollars
            priority: Optimization priority (performance, cost, or balanced)

        Returns:
            Recommendation dictionary with configuration and optimization hints
        """
        # Create baseline configuration based on input size and job type
        baseline_config = self._create_baseline_config(
            input_size_bytes, job_type, priority
        )

        # Build job data for rule evaluation
        job_data = {
            "input_bytes": input_size_bytes,
            "job_type": job_type,
            "priority": priority,
            **baseline_config,
        }

        # Evaluate rules (though this is for new jobs, we use heuristics)
        recommendations = self.engine.evaluate(job_data)

        # Apply rule recommendations to baseline config
        optimized_config = self._apply_recommendations(baseline_config, recommendations)

        return self._create_recommendation_response(
            executor_cores=optimized_config["executor_cores"],
            executor_memory_mb=optimized_config["executor_memory_mb"],
            num_executors=optimized_config["num_executors"],
            driver_memory_mb=optimized_config["driver_memory_mb"],
            confidence=0.7,
            metadata={
                "method": "rule_based",
                "job_type": job_type or "unknown",
                "priority": priority,
                "rules_applied": len(recommendations),
                "optimization_hints": [
                    {
                        "rule_id": rec.rule_id,
                        "title": rec.title,
                        "severity": rec.severity.value,
                        "spark_configs": rec.spark_configs,
                    }
                    for rec in recommendations
                ],
            },
        )

    def analyze_job(self, job_data: Dict) -> Dict:
        """Analyze a completed job and provide optimization recommendations.

        This is the primary use case for rule-based optimization -
        analyzing historical jobs to find improvements.

        Args:
            job_data: Dictionary containing job metrics (from database)

        Returns:
            Analysis results with recommendations
        """
        # Evaluate all rules
        recommendations = self.engine.evaluate(job_data)

        # Group by severity
        recommendations_by_severity = self.engine.get_recommendations_by_severity(
            job_data
        )

        # Calculate potential improvements
        critical_count = len(recommendations_by_severity[Severity.CRITICAL])
        warning_count = len(recommendations_by_severity[Severity.WARNING])
        info_count = len(recommendations_by_severity[Severity.INFO])

        # Extract current configuration
        current_config = {
            "num_executors": job_data.get("num_executors", 0),
            "executor_cores": job_data.get("executor_cores", 0),
            "executor_memory_mb": job_data.get("executor_memory_mb", 0),
            "driver_memory_mb": job_data.get("driver_memory_mb", 0),
        }

        # Apply recommendations to get optimized config
        optimized_config = self._apply_recommendations(current_config, recommendations)

        # Build Spark config string
        spark_configs = {}
        for rec in recommendations:
            spark_configs.update(rec.spark_configs)

        return {
            "app_id": job_data.get("app_id", "unknown"),
            "app_name": job_data.get("app_name", "unknown"),
            "analysis": {
                "total_recommendations": len(recommendations),
                "critical": critical_count,
                "warnings": warning_count,
                "info": info_count,
                "health_score": self._calculate_health_score(
                    critical_count, warning_count, info_count
                ),
            },
            "current_configuration": current_config,
            "recommended_configuration": optimized_config,
            "spark_configs": spark_configs,
            "recommendations": [
                {
                    "rule_id": rec.rule_id,
                    "title": rec.title,
                    "description": rec.description,
                    "severity": rec.severity.value,
                    "current_value": rec.current_value,
                    "recommended_value": rec.recommended_value,
                    "expected_improvement": rec.expected_improvement,
                    "spark_configs": rec.spark_configs,
                }
                for rec in recommendations
            ],
        }

    def train(self, historical_jobs: List[Dict]):
        """Rule-based recommender doesn't require training.

        Args:
            historical_jobs: List of historical job data (unused)
        """
        # Rules are predefined, no training needed
        pass

    def _create_baseline_config(
        self, input_size_bytes: int, job_type: Optional[str], priority: str
    ) -> Dict:
        """Create baseline configuration based on input size and job type.

        Args:
            input_size_bytes: Input data size in bytes
            job_type: Type of job
            priority: Optimization priority

        Returns:
            Baseline configuration dictionary
        """
        input_gb = input_size_bytes / (1024**3)

        # Base configuration (for ~10GB input)
        base_executor_cores = 4
        base_executor_memory_mb = 8192
        base_num_executors = 5
        base_driver_memory_mb = 4096

        # Scale based on input size
        scale_factor = max(0.5, min(5.0, input_gb / 10))

        num_executors = max(2, int(base_num_executors * scale_factor))
        executor_memory_mb = int(
            base_executor_memory_mb * min(2.0, 1 + scale_factor / 10)
        )

        # Adjust based on job type
        if job_type == "ml":
            executor_memory_mb = int(executor_memory_mb * 1.5)
            base_driver_memory_mb = int(base_driver_memory_mb * 1.5)
        elif job_type == "streaming":
            num_executors = int(num_executors * 1.3)
            executor_memory_mb = int(executor_memory_mb * 0.8)

        # Adjust based on priority
        if priority == "performance":
            num_executors = int(num_executors * 1.2)
            executor_memory_mb = int(executor_memory_mb * 1.2)
            base_executor_cores = 6
        elif priority == "cost":
            num_executors = max(2, int(num_executors * 0.8))
            executor_memory_mb = int(executor_memory_mb * 0.8)

        # Round memory to nearest GB
        executor_memory_mb = max(2048, int(round(executor_memory_mb / 1024) * 1024))
        driver_memory_mb = max(2048, int(round(base_driver_memory_mb / 1024) * 1024))

        return {
            "executor_cores": base_executor_cores,
            "executor_memory_mb": executor_memory_mb,
            "num_executors": num_executors,
            "driver_memory_mb": driver_memory_mb,
        }

    def _apply_recommendations(
        self, baseline_config: Dict, recommendations: List[Recommendation]
    ) -> Dict:
        """Apply rule recommendations to baseline configuration.

        Args:
            baseline_config: Baseline configuration
            recommendations: List of recommendations

        Returns:
            Optimized configuration
        """
        config = baseline_config.copy()

        for rec in recommendations:
            # Apply memory recommendations
            if rec.rule_id in ["MEM001", "MEM002", "GC001"]:
                # Extract recommended memory value
                if "MB" in str(rec.recommended_value):
                    try:
                        memory_mb = int(str(rec.recommended_value).replace(" MB", ""))
                        config["executor_memory_mb"] = max(
                            config["executor_memory_mb"], memory_mb
                        )
                    except (ValueError, AttributeError):
                        pass

            # Apply partition recommendations
            elif rec.rule_id == "PARTITION001":
                # Partitioning is handled via spark configs, not resource allocation
                pass

            # Apply shuffle recommendations
            elif rec.rule_id == "SHUFFLE001":
                # May need more memory for broadcast
                config["driver_memory_mb"] = int(config["driver_memory_mb"] * 1.2)

        return config

    def _calculate_health_score(self, critical: int, warnings: int, info: int) -> float:
        """Calculate job health score (0-100).

        Args:
            critical: Number of critical issues
            warnings: Number of warnings
            info: Number of info recommendations

        Returns:
            Health score between 0 and 100
        """
        # Start with perfect score
        score = 100.0

        # Deduct points for issues
        score -= critical * 30  # Critical issues: -30 points each
        score -= warnings * 10  # Warnings: -10 points each
        score -= info * 2  # Info: -2 points each

        return max(0.0, min(100.0, score))
