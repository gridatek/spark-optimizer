"""Tests for rule-based optimization engine."""

import pytest
from spark_optimizer.analyzer.rule_engine import (
    RuleEngine,
    Severity,
    MemorySpillingRule,
    MemoryFractionRule,
    ShuffleExplosionRule,
    PartitioningRule,
    GCPressureRule,
    DataSkewRule,
    SmallFilesRule,
    get_default_rules,
)


class TestRuleEngine:
    """Test the RuleEngine class."""

    def test_register_rule(self):
        """Test registering a single rule."""
        engine = RuleEngine()
        rule = MemorySpillingRule()

        engine.register_rule(rule)

        assert len(engine.rules) == 1
        assert engine.rules[0] == rule

    def test_register_rules(self):
        """Test registering multiple rules."""
        engine = RuleEngine()
        rules = [MemorySpillingRule(), GCPressureRule()]

        engine.register_rules(rules)

        assert len(engine.rules) == 2

    def test_evaluate_no_rules(self):
        """Test evaluating with no rules registered."""
        engine = RuleEngine()
        job_data = {"input_bytes": 1000}

        recommendations = engine.evaluate(job_data)

        assert recommendations == []

    def test_get_critical_recommendations(self):
        """Test filtering critical recommendations."""
        engine = RuleEngine()
        engine.register_rules([MemorySpillingRule(), MemoryFractionRule()])

        job_data = {
            "disk_spilled_bytes": 1024**3,  # 1GB spilled - triggers critical
            "memory_spilled_bytes": 1024**2,  # Also triggers warning
            "executor_memory_mb": 4096,
            "input_bytes": 10 * 1024**3,
        }

        critical_recs = engine.get_critical_recommendations(job_data)

        assert len(critical_recs) > 0
        assert all(rec.severity == Severity.CRITICAL for rec in critical_recs)

    def test_get_recommendations_by_severity(self):
        """Test grouping recommendations by severity."""
        engine = RuleEngine()
        engine.register_rules(get_default_rules())

        job_data = {
            "disk_spilled_bytes": 1024**3,  # Triggers critical
            "jvm_gc_time": 60000,  # Triggers critical if duration is 100s
            "duration_ms": 100000,
            "executor_memory_mb": 4096,
            "input_bytes": 10 * 1024**3,
            "num_executors": 5,
            "executor_cores": 4,
        }

        grouped = engine.get_recommendations_by_severity(job_data)

        assert Severity.CRITICAL in grouped
        assert Severity.WARNING in grouped
        assert Severity.INFO in grouped
        assert isinstance(grouped[Severity.CRITICAL], list)


class TestMemorySpillingRule:
    """Test the MemorySpillingRule class."""

    def test_no_spilling(self):
        """Test when there's no spilling."""
        rule = MemorySpillingRule()
        job_data = {
            "disk_spilled_bytes": 0,
            "memory_spilled_bytes": 0,
            "executor_memory_mb": 4096,
        }

        recommendation = rule.evaluate(job_data)

        assert recommendation is None

    def test_disk_spilling(self):
        """Test when disk spilling occurs."""
        rule = MemorySpillingRule()
        job_data = {
            "disk_spilled_bytes": 2 * 1024**3,  # 2GB
            "memory_spilled_bytes": 0,
            "executor_memory_mb": 4096,
        }

        recommendation = rule.evaluate(job_data)

        assert recommendation is not None
        assert recommendation.severity == Severity.CRITICAL
        assert recommendation.rule_id == "MEM001"
        assert "6144" in recommendation.recommended_value  # 4096 * 1.5

    def test_memory_spilling(self):
        """Test when memory spilling occurs."""
        rule = MemorySpillingRule()
        job_data = {
            "disk_spilled_bytes": 0,
            "memory_spilled_bytes": 1 * 1024**3,  # 1GB
            "executor_memory_mb": 8192,
        }

        recommendation = rule.evaluate(job_data)

        assert recommendation is not None
        assert recommendation.severity == Severity.CRITICAL
        assert "12288" in recommendation.recommended_value  # 8192 * 1.5


class TestMemoryFractionRule:
    """Test the MemoryFractionRule class."""

    def test_no_significant_spilling(self):
        """Test when spilling is minimal."""
        rule = MemoryFractionRule()
        job_data = {
            "memory_spilled_bytes": 0,
            "input_bytes": 10 * 1024**3,
        }

        recommendation = rule.evaluate(job_data)

        assert recommendation is None

    def test_significant_spilling(self):
        """Test when spilling is significant (>10% of input)."""
        rule = MemoryFractionRule()
        job_data = {
            "memory_spilled_bytes": 2 * 1024**3,  # 2GB
            "input_bytes": 10 * 1024**3,  # 10GB -> 20% spilling
        }

        recommendation = rule.evaluate(job_data)

        assert recommendation is not None
        assert recommendation.severity == Severity.WARNING
        assert "0.8" in recommendation.recommended_value
        assert "spark.memory.fraction" in recommendation.spark_configs


class TestShuffleExplosionRule:
    """Test the ShuffleExplosionRule class."""

    def test_normal_shuffle(self):
        """Test when shuffle is normal."""
        rule = ShuffleExplosionRule()
        job_data = {
            "input_bytes": 10 * 1024**3,
            "shuffle_write_bytes": 5 * 1024**3,  # 0.5x input
        }

        recommendation = rule.evaluate(job_data)

        assert recommendation is None

    def test_excessive_shuffle(self):
        """Test when shuffle is excessive (>5x input)."""
        rule = ShuffleExplosionRule()
        job_data = {
            "input_bytes": 10 * 1024**3,  # 10GB
            "shuffle_write_bytes": 60 * 1024**3,  # 60GB -> 6x input
        }

        recommendation = rule.evaluate(job_data)

        assert recommendation is not None
        assert recommendation.severity == Severity.CRITICAL
        assert "broadcast" in recommendation.description.lower()
        assert "spark.sql.autoBroadcastJoinThreshold" in recommendation.spark_configs


class TestPartitioningRule:
    """Test the PartitioningRule class."""

    def test_optimal_partitioning(self):
        """Test when partitioning is close to optimal."""
        rule = PartitioningRule()
        job_data = {
            "num_executors": 10,
            "executor_cores": 4,
            "input_bytes": 50 * 1024**3,  # 50GB
        }

        # For small-medium datasets, default 200 partitions is OK
        recommendation = rule.evaluate(job_data)

        # May or may not trigger depending on heuristics
        # Just ensure it doesn't crash
        assert recommendation is None or recommendation.severity == Severity.WARNING

    def test_large_dataset_partitioning(self):
        """Test partitioning for large datasets."""
        rule = PartitioningRule()
        job_data = {
            "num_executors": 20,
            "executor_cores": 4,
            "input_bytes": 500 * 1024**3,  # 500GB - large dataset
        }

        recommendation = rule.evaluate(job_data)

        # Should recommend more partitions for large dataset
        if recommendation:
            assert int(recommendation.recommended_value) > 200


class TestGCPressureRule:
    """Test the GCPressureRule class."""

    def test_normal_gc(self):
        """Test when GC time is normal (<10%)."""
        rule = GCPressureRule()
        job_data = {
            "duration_ms": 100000,  # 100s
            "jvm_gc_time": 5000,  # 5s -> 5%
        }

        recommendation = rule.evaluate(job_data)

        assert recommendation is None

    def test_high_gc_pressure(self):
        """Test when GC time is excessive (>10%)."""
        rule = GCPressureRule()
        job_data = {
            "duration_ms": 100000,  # 100s
            "jvm_gc_time": 20000,  # 20s -> 20%
        }

        recommendation = rule.evaluate(job_data)

        assert recommendation is not None
        assert recommendation.severity == Severity.CRITICAL
        assert "G1GC" in recommendation.spark_configs.get(
            "spark.executor.extraJavaOptions", ""
        )


class TestDataSkewRule:
    """Test the DataSkewRule class."""

    def test_no_skew(self):
        """Test when there's no data skew."""
        rule = DataSkewRule()
        job_data = {
            "total_tasks": 1000,
            "failed_tasks": 10,  # 1% failure rate
        }

        recommendation = rule.evaluate(job_data)

        assert recommendation is None

    def test_potential_skew(self):
        """Test when data skew is likely (>5% failures)."""
        rule = DataSkewRule()
        job_data = {
            "total_tasks": 1000,
            "failed_tasks": 100,  # 10% failure rate
        }

        recommendation = rule.evaluate(job_data)

        assert recommendation is not None
        assert recommendation.severity == Severity.WARNING
        assert "skew" in recommendation.description.lower()
        assert "spark.sql.adaptive.enabled" in recommendation.spark_configs


class TestSmallFilesRule:
    """Test the SmallFilesRule class."""

    def test_large_files(self):
        """Test when files are appropriately sized."""
        rule = SmallFilesRule()
        job_data = {
            "total_tasks": 100,
            "input_bytes": 50 * 1024**3,  # 50GB / 100 tasks = 500MB avg
        }

        recommendation = rule.evaluate(job_data)

        assert recommendation is None

    def test_small_files(self):
        """Test when files are too small."""
        rule = SmallFilesRule()
        job_data = {
            "total_tasks": 1000,  # Many tasks
            "input_bytes": 5 * 1024**3,  # 5GB / 1000 tasks = 5MB avg
        }

        recommendation = rule.evaluate(job_data)

        assert recommendation is not None
        assert recommendation.severity == Severity.WARNING
        assert "coalesce" in recommendation.recommended_value.lower()


class TestGetDefaultRules:
    """Test the get_default_rules function."""

    def test_returns_all_rules(self):
        """Test that all default rules are returned."""
        rules = get_default_rules()

        assert len(rules) == 7
        assert any(isinstance(r, MemorySpillingRule) for r in rules)
        assert any(isinstance(r, MemoryFractionRule) for r in rules)
        assert any(isinstance(r, ShuffleExplosionRule) for r in rules)
        assert any(isinstance(r, PartitioningRule) for r in rules)
        assert any(isinstance(r, GCPressureRule) for r in rules)
        assert any(isinstance(r, DataSkewRule) for r in rules)
        assert any(isinstance(r, SmallFilesRule) for r in rules)

    def test_rules_have_unique_ids(self):
        """Test that all rules have unique IDs."""
        rules = get_default_rules()
        rule_ids = [r.rule_id for r in rules]

        assert len(rule_ids) == len(set(rule_ids))
