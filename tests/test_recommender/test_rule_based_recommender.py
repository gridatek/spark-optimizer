"""Tests for rule-based recommender."""

import pytest
from spark_optimizer.recommender.rule_based_recommender import RuleBasedRecommender


class TestRuleBasedRecommender:
    """Test the RuleBasedRecommender class."""

    def test_init(self):
        """Test recommender initialization."""
        recommender = RuleBasedRecommender()

        assert recommender.engine is not None
        assert len(recommender.engine.rules) == 7  # Default rules

    def test_recommend_basic(self):
        """Test basic recommendation."""
        recommender = RuleBasedRecommender()

        rec = recommender.recommend(
            input_size_bytes=10 * 1024**3,  # 10GB
            job_type="etl",
            priority="balanced",
        )

        assert "configuration" in rec
        assert "confidence" in rec
        assert "metadata" in rec
        assert rec["metadata"]["method"] == "rule_based"

        config = rec["configuration"]
        assert config["num_executors"] > 0
        assert config["executor_cores"] > 0
        assert config["executor_memory_mb"] > 0
        assert config["driver_memory_mb"] > 0

    def test_recommend_ml_job(self):
        """Test recommendation for ML job."""
        recommender = RuleBasedRecommender()

        rec_etl = recommender.recommend(input_size_bytes=10 * 1024**3, job_type="etl")

        rec_ml = recommender.recommend(input_size_bytes=10 * 1024**3, job_type="ml")

        # ML jobs should get more memory
        assert (
            rec_ml["configuration"]["executor_memory_mb"]
            > rec_etl["configuration"]["executor_memory_mb"]
        )

    def test_recommend_streaming_job(self):
        """Test recommendation for streaming job."""
        recommender = RuleBasedRecommender()

        rec_etl = recommender.recommend(input_size_bytes=10 * 1024**3, job_type="etl")

        rec_streaming = recommender.recommend(
            input_size_bytes=10 * 1024**3, job_type="streaming"
        )

        # Streaming jobs should get more executors
        assert (
            rec_streaming["configuration"]["num_executors"]
            >= rec_etl["configuration"]["num_executors"]
        )

    def test_recommend_performance_priority(self):
        """Test recommendation with performance priority."""
        recommender = RuleBasedRecommender()

        rec_balanced = recommender.recommend(
            input_size_bytes=10 * 1024**3, priority="balanced"
        )

        rec_performance = recommender.recommend(
            input_size_bytes=10 * 1024**3, priority="performance"
        )

        # Performance priority should allocate more resources
        assert (
            rec_performance["configuration"]["num_executors"]
            >= rec_balanced["configuration"]["num_executors"]
        )
        assert (
            rec_performance["configuration"]["executor_memory_mb"]
            >= rec_balanced["configuration"]["executor_memory_mb"]
        )

    def test_recommend_cost_priority(self):
        """Test recommendation with cost priority."""
        recommender = RuleBasedRecommender()

        rec_balanced = recommender.recommend(
            input_size_bytes=10 * 1024**3, priority="balanced"
        )

        rec_cost = recommender.recommend(input_size_bytes=10 * 1024**3, priority="cost")

        # Cost priority should allocate fewer resources
        assert (
            rec_cost["configuration"]["num_executors"]
            <= rec_balanced["configuration"]["num_executors"]
        )
        assert (
            rec_cost["configuration"]["executor_memory_mb"]
            <= rec_balanced["configuration"]["executor_memory_mb"]
        )

    def test_recommend_scales_with_input_size(self):
        """Test that recommendations scale with input size."""
        recommender = RuleBasedRecommender()

        rec_small = recommender.recommend(input_size_bytes=5 * 1024**3)  # 5GB

        rec_large = recommender.recommend(input_size_bytes=50 * 1024**3)  # 50GB

        # Larger input should get more executors
        assert (
            rec_large["configuration"]["num_executors"]
            > rec_small["configuration"]["num_executors"]
        )

    def test_analyze_job_healthy(self):
        """Test analyzing a healthy job."""
        recommender = RuleBasedRecommender()

        job_data = {
            "app_id": "app-123",
            "app_name": "Healthy Job",
            "input_bytes": 10 * 1024**3,
            "disk_spilled_bytes": 0,
            "memory_spilled_bytes": 0,
            "num_executors": 10,
            "executor_cores": 4,
            "executor_memory_mb": 8192,
            "driver_memory_mb": 4096,
            "duration_ms": 300000,
            "jvm_gc_time": 10000,  # 3.3% GC time - healthy
            "total_tasks": 1000,
            "failed_tasks": 0,
        }

        analysis = recommender.analyze_job(job_data)

        assert analysis["app_id"] == "app-123"
        assert "analysis" in analysis
        assert "health_score" in analysis["analysis"]
        assert analysis["analysis"]["health_score"] >= 80  # Should be healthy
        assert analysis["analysis"]["critical"] == 0

    def test_analyze_job_with_issues(self):
        """Test analyzing a job with performance issues."""
        recommender = RuleBasedRecommender()

        job_data = {
            "app_id": "app-456",
            "app_name": "Problem Job",
            "input_bytes": 10 * 1024**3,
            "disk_spilled_bytes": 2 * 1024**3,  # 2GB spilled - critical
            "memory_spilled_bytes": 1 * 1024**3,  # 1GB spilled
            "num_executors": 5,
            "executor_cores": 4,
            "executor_memory_mb": 4096,
            "driver_memory_mb": 2048,
            "duration_ms": 300000,
            "jvm_gc_time": 50000,  # 16.7% GC time - critical
            "total_tasks": 1000,
            "failed_tasks": 100,  # 10% failure rate - warning
            "shuffle_write_bytes": 100 * 1024**3,  # 10x input - critical
        }

        analysis = recommender.analyze_job(job_data)

        assert analysis["app_id"] == "app-456"
        assert analysis["analysis"]["total_recommendations"] > 0
        assert analysis["analysis"]["critical"] > 0  # Should have critical issues
        assert analysis["analysis"]["health_score"] < 70  # Should be unhealthy
        assert len(analysis["recommendations"]) > 0

        # Check that recommendations have expected fields
        rec = analysis["recommendations"][0]
        assert "rule_id" in rec
        assert "title" in rec
        assert "description" in rec
        assert "severity" in rec
        assert "spark_configs" in rec

    def test_analyze_job_memory_spilling(self):
        """Test that memory spilling is detected."""
        recommender = RuleBasedRecommender()

        job_data = {
            "app_id": "app-789",
            "app_name": "Spilling Job",
            "disk_spilled_bytes": 5 * 1024**3,  # 5GB spilled
            "memory_spilled_bytes": 0,
            "executor_memory_mb": 8192,
            "input_bytes": 10 * 1024**3,
            "num_executors": 10,
            "executor_cores": 4,
            "driver_memory_mb": 4096,
        }

        analysis = recommender.analyze_job(job_data)

        # Should recommend increasing memory
        assert analysis["analysis"]["critical"] > 0
        assert (
            analysis["recommended_configuration"]["executor_memory_mb"]
            > job_data["executor_memory_mb"]
        )

        # Should have MEM001 rule triggered
        rule_ids = [rec["rule_id"] for rec in analysis["recommendations"]]
        assert "MEM001" in rule_ids

    def test_analyze_job_shuffle_explosion(self):
        """Test that excessive shuffle is detected."""
        recommender = RuleBasedRecommender()

        job_data = {
            "app_id": "app-999",
            "app_name": "Shuffle Heavy Job",
            "input_bytes": 10 * 1024**3,  # 10GB
            "shuffle_write_bytes": 100 * 1024**3,  # 100GB - 10x input!
            "num_executors": 10,
            "executor_cores": 4,
            "executor_memory_mb": 8192,
            "driver_memory_mb": 4096,
        }

        analysis = recommender.analyze_job(job_data)

        # Should detect shuffle explosion
        assert analysis["analysis"]["critical"] > 0

        # Should have SHUFFLE001 rule triggered
        rule_ids = [rec["rule_id"] for rec in analysis["recommendations"]]
        assert "SHUFFLE001" in rule_ids

        # Should recommend broadcast join
        shuffle_rec = next(
            rec for rec in analysis["recommendations"] if rec["rule_id"] == "SHUFFLE001"
        )
        assert "broadcast" in shuffle_rec["description"].lower()

    def test_health_score_calculation(self):
        """Test health score calculation."""
        recommender = RuleBasedRecommender()

        # Perfect score
        assert recommender._calculate_health_score(0, 0, 0) == 100.0

        # One critical issue
        assert recommender._calculate_health_score(1, 0, 0) == 70.0

        # Multiple issues
        score = recommender._calculate_health_score(1, 2, 3)
        assert score == 44.0  # 100 - (1*30 + 2*10 + 3*2)

        # Score can't go below 0
        assert recommender._calculate_health_score(10, 10, 10) == 0.0

    def test_baseline_config_creation(self):
        """Test baseline configuration creation."""
        recommender = RuleBasedRecommender()

        config = recommender._create_baseline_config(
            input_size_bytes=10 * 1024**3, job_type="etl", priority="balanced"
        )

        assert config["executor_cores"] > 0
        assert config["executor_memory_mb"] > 0
        assert config["num_executors"] > 0
        assert config["driver_memory_mb"] > 0

        # Memory should be in GB increments
        assert config["executor_memory_mb"] % 1024 == 0
        assert config["driver_memory_mb"] % 1024 == 0

    def test_train_method(self):
        """Test that train method exists but doesn't require data."""
        recommender = RuleBasedRecommender()

        # Should not raise exception
        recommender.train([])
        recommender.train([{"some": "data"}])

    def test_metadata_includes_optimization_hints(self):
        """Test that metadata includes optimization hints."""
        recommender = RuleBasedRecommender()

        rec = recommender.recommend(
            input_size_bytes=10 * 1024**3, job_type="etl", priority="balanced"
        )

        assert "metadata" in rec
        assert "optimization_hints" in rec["metadata"]
        # Even if no rules triggered, the field should exist
        assert isinstance(rec["metadata"]["optimization_hints"], list)
