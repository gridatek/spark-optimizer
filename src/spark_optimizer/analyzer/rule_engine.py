"""Rule-based optimization engine for Spark jobs."""

from typing import Dict, List, Optional, Callable, Any
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum


class Severity(Enum):
    """Severity levels for optimization recommendations."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class Recommendation:
    """A single optimization recommendation."""

    rule_id: str
    title: str
    description: str
    severity: Severity
    current_value: Any
    recommended_value: Any
    expected_improvement: str
    spark_configs: Dict[str, str]


class Rule(ABC):
    """Base class for optimization rules."""

    def __init__(self, rule_id: str, title: str, severity: Severity):
        """Initialize rule.

        Args:
            rule_id: Unique identifier for the rule
            title: Human-readable title
            severity: Severity level of the issue
        """
        self.rule_id = rule_id
        self.title = title
        self.severity = severity

    @abstractmethod
    def evaluate(self, job_data: Dict) -> Optional[Recommendation]:
        """Evaluate rule against job data.

        Args:
            job_data: Dictionary containing job metrics

        Returns:
            Recommendation if rule is triggered, None otherwise
        """
        pass

    def _get_value(self, job_data: Dict, key: str, default: Any = 0) -> Any:
        """Safely get value from job data.

        Args:
            job_data: Job data dictionary
            key: Key to retrieve
            default: Default value if key not found

        Returns:
            Value from job data or default
        """
        return job_data.get(key, default)


class RuleEngine:
    """Engine for evaluating optimization rules."""

    def __init__(self):
        """Initialize rule engine."""
        self.rules: List[Rule] = []

    def register_rule(self, rule: Rule) -> None:
        """Register a new rule.

        Args:
            rule: Rule to register
        """
        self.rules.append(rule)

    def register_rules(self, rules: List[Rule]) -> None:
        """Register multiple rules.

        Args:
            rules: List of rules to register
        """
        self.rules.extend(rules)

    def evaluate(self, job_data: Dict) -> List[Recommendation]:
        """Evaluate all rules against job data.

        Args:
            job_data: Dictionary containing job metrics

        Returns:
            List of recommendations
        """
        recommendations = []

        for rule in self.rules:
            try:
                recommendation = rule.evaluate(job_data)
                if recommendation:
                    recommendations.append(recommendation)
            except Exception as e:
                # Log but don't fail if a single rule has issues
                print(f"Error evaluating rule {rule.rule_id}: {e}")
                continue

        return recommendations

    def get_critical_recommendations(self, job_data: Dict) -> List[Recommendation]:
        """Get only critical recommendations.

        Args:
            job_data: Dictionary containing job metrics

        Returns:
            List of critical recommendations
        """
        all_recommendations = self.evaluate(job_data)
        return [r for r in all_recommendations if r.severity == Severity.CRITICAL]

    def get_recommendations_by_severity(
        self, job_data: Dict
    ) -> Dict[Severity, List[Recommendation]]:
        """Get recommendations grouped by severity.

        Args:
            job_data: Dictionary containing job metrics

        Returns:
            Dictionary mapping severity to recommendations
        """
        recommendations = self.evaluate(job_data)
        grouped: Dict[Severity, List[Recommendation]] = {
            Severity.CRITICAL: [],
            Severity.WARNING: [],
            Severity.INFO: [],
        }

        for rec in recommendations:
            grouped[rec.severity].append(rec)

        return grouped


# Memory Optimization Rules


class MemorySpillingRule(Rule):
    """Detect memory spilling to disk."""

    def __init__(self):
        super().__init__(
            rule_id="MEM001",
            title="Memory Spilling Detected",
            severity=Severity.CRITICAL,
        )

    def evaluate(self, job_data: Dict) -> Optional[Recommendation]:
        disk_spilled = self._get_value(job_data, "disk_spilled_bytes", 0)
        memory_spilled = self._get_value(job_data, "memory_spilled_bytes", 0)
        executor_memory_mb = self._get_value(job_data, "executor_memory_mb", 4096)

        if disk_spilled > 0 or memory_spilled > 0:
            # Recommend increasing memory by 50%
            recommended_memory = int(executor_memory_mb * 1.5)

            spilled_gb = (disk_spilled + memory_spilled) / (1024**3)

            return Recommendation(
                rule_id=self.rule_id,
                title=self.title,
                description=f"Job spilled {spilled_gb:.2f} GB to disk/memory. "
                f"This indicates insufficient executor memory and will severely impact performance.",
                severity=self.severity,
                current_value=f"{executor_memory_mb} MB",
                recommended_value=f"{recommended_memory} MB",
                expected_improvement="20-50% faster execution, more stable performance",
                spark_configs={
                    "spark.executor.memory": f"{recommended_memory}m",
                },
            )

        return None


class MemoryFractionRule(Rule):
    """Optimize memory fraction for storage vs execution."""

    def __init__(self):
        super().__init__(
            rule_id="MEM002",
            title="Memory Fraction Optimization",
            severity=Severity.WARNING,
        )

    def evaluate(self, job_data: Dict) -> Optional[Recommendation]:
        memory_spilled = self._get_value(job_data, "memory_spilled_bytes", 0)
        input_bytes = self._get_value(job_data, "input_bytes", 0)

        # If spilling more than 10% of input data
        if input_bytes > 0 and memory_spilled > input_bytes * 0.1:
            return Recommendation(
                rule_id=self.rule_id,
                title=self.title,
                description="Significant memory spilling detected. "
                "Increasing memory fraction for execution can help.",
                severity=self.severity,
                current_value="Default (0.6)",
                recommended_value="0.8",
                expected_improvement="Reduced spilling, 10-20% performance gain",
                spark_configs={
                    "spark.memory.fraction": "0.8",
                    "spark.memory.storageFraction": "0.2",
                },
            )

        return None


# Shuffle Optimization Rules


class ShuffleExplosionRule(Rule):
    """Detect excessive shuffle operations."""

    def __init__(self):
        super().__init__(
            rule_id="SHUFFLE001",
            title="Excessive Shuffle Detected",
            severity=Severity.CRITICAL,
        )

    def evaluate(self, job_data: Dict) -> Optional[Recommendation]:
        input_bytes = self._get_value(job_data, "input_bytes", 0)
        shuffle_write = self._get_value(job_data, "shuffle_write_bytes", 0)

        # If shuffle is more than 5x input, consider broadcast
        if input_bytes > 0 and shuffle_write > input_bytes * 5:
            shuffle_gb = shuffle_write / (1024**3)
            input_gb = input_bytes / (1024**3)

            return Recommendation(
                rule_id=self.rule_id,
                title=self.title,
                description=f"Shuffle write ({shuffle_gb:.2f} GB) is {shuffle_write/input_bytes:.1f}x "
                f"the input size ({input_gb:.2f} GB). Consider using broadcast joins for smaller tables.",
                severity=self.severity,
                current_value=f"{shuffle_gb:.2f} GB shuffled",
                recommended_value="Use broadcast joins or repartition",
                expected_improvement="50-80% reduction in shuffle, significantly faster execution",
                spark_configs={
                    "spark.sql.autoBroadcastJoinThreshold": "104857600",  # 100MB
                },
            )

        return None


class PartitioningRule(Rule):
    """Optimize partition count."""

    def __init__(self):
        super().__init__(
            rule_id="PARTITION001",
            title="Suboptimal Partition Count",
            severity=Severity.WARNING,
        )

    def evaluate(self, job_data: Dict) -> Optional[Recommendation]:
        num_executors = self._get_value(job_data, "num_executors", 1)
        executor_cores = self._get_value(job_data, "executor_cores", 1)
        total_stages = self._get_value(job_data, "total_stages", 0)
        input_bytes = self._get_value(job_data, "input_bytes", 0)

        # Target: 2-3 partitions per core
        target_partitions = num_executors * executor_cores * 2

        # Get shuffle partitions (default is 200)
        # This would ideally come from spark configs, but we use heuristic
        input_gb = input_bytes / (1024**3)

        # For large datasets, use more partitions
        if input_gb > 100:
            recommended_partitions = min(target_partitions * 2, 2000)
        elif input_gb > 10:
            recommended_partitions = target_partitions
        else:
            recommended_partitions = max(target_partitions // 2, 20)

        # Only recommend if significantly different from default 200
        if abs(recommended_partitions - 200) > 50:
            return Recommendation(
                rule_id=self.rule_id,
                title=self.title,
                description=f"With {num_executors} executors and {executor_cores} cores each, "
                f"shuffle partitions should be optimized for parallelism.",
                severity=self.severity,
                current_value="200 (default)",
                recommended_value=str(recommended_partitions),
                expected_improvement="Better parallelism, 10-30% performance gain",
                spark_configs={
                    "spark.sql.shuffle.partitions": str(recommended_partitions),
                },
            )

        return None


# GC Optimization Rules


class GCPressureRule(Rule):
    """Detect excessive GC time."""

    def __init__(self):
        super().__init__(
            rule_id="GC001",
            title="High GC Pressure",
            severity=Severity.CRITICAL,
        )

    def evaluate(self, job_data: Dict) -> Optional[Recommendation]:
        duration_ms = self._get_value(job_data, "duration_ms", 0)
        jvm_gc_time = self._get_value(job_data, "jvm_gc_time", 0)

        # If GC time is more than 10% of total time
        if duration_ms > 0 and jvm_gc_time > duration_ms * 0.1:
            gc_percent = (jvm_gc_time / duration_ms) * 100

            return Recommendation(
                rule_id=self.rule_id,
                title=self.title,
                description=f"GC time is {gc_percent:.1f}% of total execution time. "
                f"This indicates memory pressure and frequent garbage collection.",
                severity=self.severity,
                current_value=f"{gc_percent:.1f}% GC time",
                recommended_value="< 10% GC time",
                expected_improvement="15-40% performance improvement",
                spark_configs={
                    "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35",
                },
            )

        return None


# Data Skew Rules


class DataSkewRule(Rule):
    """Detect potential data skew."""

    def __init__(self):
        super().__init__(
            rule_id="SKEW001",
            title="Potential Data Skew",
            severity=Severity.WARNING,
        )

    def evaluate(self, job_data: Dict) -> Optional[Recommendation]:
        total_tasks = self._get_value(job_data, "total_tasks", 0)
        failed_tasks = self._get_value(job_data, "failed_tasks", 0)
        duration_ms = self._get_value(job_data, "duration_ms", 0)

        # High task failure rate can indicate skew
        if total_tasks > 0 and failed_tasks > total_tasks * 0.05:
            failure_rate = (failed_tasks / total_tasks) * 100

            return Recommendation(
                rule_id=self.rule_id,
                title=self.title,
                description=f"{failure_rate:.1f}% task failure rate detected. "
                f"This often indicates data skew where some partitions are much larger than others.",
                severity=self.severity,
                current_value=f"{failed_tasks}/{total_tasks} tasks failed",
                recommended_value="Add salting or use adaptive execution",
                expected_improvement="More balanced execution, fewer failures",
                spark_configs={
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.skewJoin.enabled": "true",
                },
            )

        return None


# I/O Optimization Rules


class SmallFilesRule(Rule):
    """Detect too many small input files."""

    def __init__(self):
        super().__init__(
            rule_id="IO001",
            title="Too Many Small Files",
            severity=Severity.WARNING,
        )

    def evaluate(self, job_data: Dict) -> Optional[Recommendation]:
        total_tasks = self._get_value(job_data, "total_tasks", 0)
        input_bytes = self._get_value(job_data, "input_bytes", 0)

        # If average file size is less than 10MB
        if total_tasks > 100 and input_bytes > 0:
            avg_file_size = input_bytes / total_tasks
            avg_file_mb = avg_file_size / (1024**2)

            if avg_file_mb < 10:
                return Recommendation(
                    rule_id=self.rule_id,
                    title=self.title,
                    description=f"Job has {total_tasks} tasks with average input size of {avg_file_mb:.2f} MB. "
                    f"Small files create scheduling overhead and reduce parallelism.",
                    severity=self.severity,
                    current_value=f"{total_tasks} files, {avg_file_mb:.2f} MB avg",
                    recommended_value="Coalesce files to 128-256 MB each",
                    expected_improvement="Reduced scheduling overhead, 20-40% faster",
                    spark_configs={
                        "spark.sql.files.maxPartitionBytes": str(
                            256 * 1024 * 1024
                        ),  # 256MB
                    },
                )

        return None


def get_default_rules() -> List[Rule]:
    """Get list of all default optimization rules.

    Returns:
        List of rule instances
    """
    return [
        # Memory rules
        MemorySpillingRule(),
        MemoryFractionRule(),
        # Shuffle rules
        ShuffleExplosionRule(),
        PartitioningRule(),
        # GC rules
        GCPressureRule(),
        # Skew rules
        DataSkewRule(),
        # I/O rules
        SmallFilesRule(),
    ]
