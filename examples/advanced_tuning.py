"""Advanced usage example with multiple recommenders and analysis.

This example demonstrates advanced features of the optimizer:
1. Comparing different recommendation methods (similarity, rule-based, ML)
2. Analyzing historical jobs for patterns
3. Cost-optimized recommendations

To run this example:
    python examples/advanced_tuning.py
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from spark_optimizer.recommender.similarity_recommender import SimilarityRecommender
from spark_optimizer.recommender.rule_based_recommender import RuleBasedRecommender
from spark_optimizer.recommender.ml_recommender import MLRecommender
from spark_optimizer.analyzer.job_analyzer import JobAnalyzer
from spark_optimizer.storage.database import Database
from spark_optimizer.storage.repository import SparkApplicationRepository
from spark_optimizer.storage.models import SparkApplication


def compare_recommenders(job_requirements, db):
    """Compare recommendations from different methods.

    Args:
        job_requirements: Job requirements dictionary
        db: Database instance
    """
    print("\n" + "=" * 60)
    print("Comparing Recommendations from Different Methods")
    print("=" * 60)

    input_size_bytes = int(job_requirements["input_size_gb"] * 1024**3)

    # 1. Similarity-based recommendation
    print("\n1. Similarity-based Recommender:")
    print("-" * 40)
    similarity_rec = SimilarityRecommender(db=db)
    sim_result = similarity_rec.recommend(
        input_size_bytes=input_size_bytes,
        job_type=job_requirements.get("job_type"),
        priority="balanced",
    )
    print_recommendation(sim_result)

    # 2. Rule-based recommendation
    print("\n2. Rule-based Recommender:")
    print("-" * 40)
    rule_rec = RuleBasedRecommender()
    rule_result = rule_rec.recommend(
        input_size_bytes=input_size_bytes,
        job_type=job_requirements.get("job_type"),
        priority="balanced",
    )
    print_recommendation(rule_result)

    # 3. ML-based recommendation
    print("\n3. ML-based Recommender:")
    print("-" * 40)
    ml_rec = MLRecommender(db=db)

    # Check if we have enough data for ML
    with db.get_session() as session:
        job_count = session.query(SparkApplication).count()

    if job_count >= 10:
        # Train the model first
        print("  Training ML model on historical data...")
        ml_rec.train()
        ml_result = ml_rec.recommend(
            input_size_bytes=input_size_bytes,
            job_type=job_requirements.get("job_type"),
            priority="balanced",
        )
        print_recommendation(ml_result)
    else:
        print(f"  Note: ML recommender requires at least 10 historical jobs")
        print(f"  (Currently have {job_count} jobs)")
        print("  Using fallback heuristics...")
        ml_result = ml_rec.recommend(
            input_size_bytes=input_size_bytes,
            job_type=job_requirements.get("job_type"),
            priority="balanced",
        )
        print_recommendation(ml_result)

    return {
        "similarity": sim_result,
        "rule_based": rule_result,
        "ml": ml_result,
    }


def analyze_historical_jobs(db):
    """Analyze historical jobs for patterns and insights.

    Args:
        db: Database instance
    """
    print("\n" + "=" * 60)
    print("Analyzing Historical Jobs")
    print("=" * 60)

    analyzer = JobAnalyzer()

    with db.get_session() as session:
        repo = SparkApplicationRepository(session)
        recent_jobs = repo.get_recent(days=30, limit=10)

        if not recent_jobs:
            print("\nNo recent jobs found in database.")
            print("To analyze real jobs, first collect data from your Spark cluster.")
            return

        print(f"\nFound {len(recent_jobs)} recent jobs to analyze")
        print("-" * 60)

        for i, job in enumerate(recent_jobs[:5], 1):
            # Build job data dictionary for analyzer
            job_data = {
                "app_id": job.app_id,
                "app_name": job.app_name or "Unknown",
                "input_bytes": job.input_bytes or 0,
                "output_bytes": job.output_bytes or 0,
                "shuffle_read_bytes": job.shuffle_read_bytes or 0,
                "shuffle_write_bytes": job.shuffle_write_bytes or 0,
                "disk_spilled_bytes": job.disk_spilled_bytes or 0,
                "memory_spilled_bytes": job.memory_spilled_bytes or 0,
                "num_executors": job.num_executors or 0,
                "executor_cores": job.executor_cores or 0,
                "executor_memory_mb": job.executor_memory_mb or 0,
                "driver_memory_mb": job.driver_memory_mb or 0,
                "duration_ms": job.duration_ms or 0,
                "jvm_gc_time_ms": job.jvm_gc_time_ms or 0,
                "executor_run_time_ms": job.executor_run_time_ms or 0,
                "executor_cpu_time_ms": job.executor_cpu_time_ms or 0,
                "total_tasks": job.total_tasks or 0,
                "failed_tasks": job.failed_tasks or 0,
                "total_stages": job.total_stages or 0,
                "failed_stages": job.failed_stages or 0,
                "peak_memory_usage": job.peak_memory_usage or 0,
            }

            analysis = analyzer.analyze_job(job_data)

            print(f"\nJob {i}: {job.app_name or job.app_id}")
            print(f"  Duration: {(job.duration_ms or 0) / 1000:.1f}s")
            print(f"  Input Size: {(job.input_bytes or 0) / (1024**3):.2f} GB")
            print(f"  Executors: {job.num_executors or 'N/A'}")

            # Show efficiency metrics
            efficiency = analysis.get("resource_efficiency", {})
            print(f"  CPU Efficiency: {efficiency.get('cpu_efficiency', 0):.1%}")
            print(f"  Memory Efficiency: {efficiency.get('memory_efficiency', 0):.1%}")

            # Show health score
            health_score = analysis.get("health_score", 0)
            print(f"  Health Score: {health_score:.0f}/100")

            # Show any bottlenecks
            bottlenecks = analysis.get("bottlenecks", [])
            if bottlenecks:
                print(f"  Bottlenecks: {', '.join(bottlenecks)}")

            # Show issues
            issues = analysis.get("issues", [])
            if issues:
                print(f"  Issues Found: {len(issues)}")
                for issue in issues[:2]:  # Show first 2 issues
                    print(f"    - [{issue.get('severity', 'INFO')}] {issue.get('type', 'Unknown')}")


def cost_optimized_recommendation(job_requirements, max_cost_usd, db):
    """Get cost-optimized recommendations.

    Args:
        job_requirements: Job requirements dictionary
        max_cost_usd: Maximum acceptable cost in USD
        db: Database instance
    """
    print("\n" + "=" * 60)
    print(f"Cost-Optimized Recommendation (Max: ${max_cost_usd:.2f})")
    print("=" * 60)

    input_size_bytes = int(job_requirements["input_size_gb"] * 1024**3)

    # Get recommendations with different priorities
    recommender = SimilarityRecommender(db=db)

    # Get performance-focused recommendation
    performance_rec = recommender.recommend(
        input_size_bytes=input_size_bytes,
        job_type=job_requirements.get("job_type"),
        priority="performance",
    )

    # Get cost-focused recommendation
    cost_rec = recommender.recommend(
        input_size_bytes=input_size_bytes,
        job_type=job_requirements.get("job_type"),
        priority="cost",
        budget_dollars=max_cost_usd,
    )

    # Get balanced recommendation
    balanced_rec = recommender.recommend(
        input_size_bytes=input_size_bytes,
        job_type=job_requirements.get("job_type"),
        priority="balanced",
    )

    # Calculate estimated costs (rough estimate based on executor-hours)
    def estimate_cost(config):
        """Rough cost estimate based on resources and estimated duration."""
        num_executors = config.get("num_executors", 4)
        memory_gb = config.get("executor_memory_mb", 4096) / 1024
        predicted_duration_hours = config.get("predicted_duration_ms", 3600000) / (1000 * 3600)

        # Rough cost: $0.10 per executor-hour per 4GB memory
        cost_per_executor_hour = 0.10 * (memory_gb / 4)
        total_cost = num_executors * predicted_duration_hours * cost_per_executor_hour
        return total_cost

    print("\nComparison of Priorities:")
    print("-" * 60)

    for name, rec in [
        ("Performance", performance_rec),
        ("Balanced", balanced_rec),
        ("Cost-Optimized", cost_rec),
    ]:
        config = rec.get("configuration", rec)
        est_cost = estimate_cost(config)
        print(f"\n{name} Priority:")
        print(f"  Executors: {config.get('num_executors', 'N/A')}")
        print(f"  Memory per Executor: {config.get('executor_memory_mb', 'N/A')} MB")
        print(f"  Cores per Executor: {config.get('executor_cores', 'N/A')}")
        print(f"  Estimated Cost: ${est_cost:.2f}")

        if est_cost <= max_cost_usd:
            print(f"  Status: Within budget")
        else:
            print(f"  Status: Over budget by ${est_cost - max_cost_usd:.2f}")

    # Recommendations for cost optimization
    print("\nCost Optimization Tips:")
    print("-" * 60)
    print("1. Consider using spot/preemptible instances for fault-tolerant jobs")
    print("2. Enable dynamic allocation to scale resources based on demand")
    print("3. Use smaller executor memory with more executors for shuffle-heavy jobs")
    print("4. Schedule jobs during off-peak hours for lower spot prices")
    print("5. Consider using instance fleets with mixed instance types")

    return {
        "performance": performance_rec,
        "balanced": balanced_rec,
        "cost_optimized": cost_rec,
    }


def print_recommendation(recommendation):
    """Print recommendation in a formatted way.

    Args:
        recommendation: Recommendation dictionary
    """
    config = recommendation.get("configuration", recommendation)
    print(f"  Executor Cores: {config.get('executor_cores', 'N/A')}")
    print(f"  Executor Memory: {config.get('executor_memory_mb', 'N/A')} MB")
    print(f"  Number of Executors: {config.get('num_executors', 'N/A')}")
    print(f"  Driver Memory: {config.get('driver_memory_mb', 'N/A')} MB")

    confidence = recommendation.get("confidence", recommendation.get("confidence_score", 0))
    print(f"  Confidence: {confidence:.2%}")

    metadata = recommendation.get("metadata", {})
    method = metadata.get("method", recommendation.get("recommendation_method", "fallback"))
    print(f"  Method: {method}")


def create_sample_jobs(db, num_jobs=15):
    """Create sample historical jobs for demonstration.

    Args:
        db: Database instance
        num_jobs: Number of sample jobs to create
    """
    print("\nCreating sample historical jobs for demonstration...")

    with db.get_session() as session:
        repo = SparkApplicationRepository(session)

        for i in range(num_jobs):
            # Vary job characteristics
            input_gb = 5 + (i * 3)  # 5GB to 50GB
            is_ml_job = i % 3 == 0

            job_data = {
                "app_id": f"app-sample-{i:04d}",
                "app_name": f"Sample {'ML' if is_ml_job else 'ETL'} Job {i}",
                "user": "demo_user",
                "status": "COMPLETED",
                "start_time": datetime.utcnow() - timedelta(days=i, hours=i % 24),
                "end_time": datetime.utcnow() - timedelta(days=i, hours=i % 24 - 1),
                "submit_time": datetime.utcnow() - timedelta(days=i, hours=i % 24),
                "duration_ms": (30 + input_gb * 2) * 60 * 1000,  # 30min + 2min per GB
                "num_executors": 5 + input_gb // 10,
                "executor_cores": 4 if not is_ml_job else 8,
                "executor_memory_mb": 4096 if not is_ml_job else 8192,
                "driver_memory_mb": 2048,
                "total_tasks": input_gb * 10,
                "failed_tasks": 0,
                "total_stages": 5 if not is_ml_job else 15,
                "failed_stages": 0,
                "input_bytes": input_gb * 1024**3,
                "output_bytes": int(input_gb * 0.5 * 1024**3),
                "shuffle_read_bytes": int(input_gb * 0.2 * 1024**3),
                "shuffle_write_bytes": int(input_gb * 0.2 * 1024**3),
                "disk_spilled_bytes": 0,
                "memory_spilled_bytes": 0,
                "executor_run_time_ms": (20 + input_gb) * 60 * 1000,
                "executor_cpu_time_ms": (15 + input_gb) * 60 * 1000,
                "jvm_gc_time_ms": int((input_gb * 0.05) * 60 * 1000),
            }

            # Check if job already exists
            if not repo.get_by_app_id(job_data["app_id"]):
                repo.create(job_data)

        session.commit()

    print(f"  Created {num_jobs} sample jobs")


def main():
    """Run advanced tuning examples."""
    print("=" * 60)
    print("Spark Resource Optimizer - Advanced Tuning Examples")
    print("=" * 60)

    # Initialize database
    db_url = "sqlite:///spark_optimizer_advanced.db"
    db = Database(db_url)
    db.create_tables()

    # Create sample data for demonstration
    with db.get_session() as session:
        job_count = session.query(SparkApplication).count()

    if job_count < 10:
        create_sample_jobs(db, num_jobs=15)

    # Define job requirements for testing
    job_requirements = {
        "input_size_gb": 25.0,
        "job_type": "etl",
        "app_name": "daily_data_pipeline",
    }

    print(f"\nJob Requirements:")
    print(f"  Input Size: {job_requirements['input_size_gb']} GB")
    print(f"  Job Type: {job_requirements['job_type']}")
    print(f"  App Name: {job_requirements['app_name']}")

    # 1. Compare different recommender methods
    comparison = compare_recommenders(job_requirements, db)

    # 2. Analyze historical jobs
    analyze_historical_jobs(db)

    # 3. Get cost-optimized recommendation
    cost_results = cost_optimized_recommendation(job_requirements, max_cost_usd=10.0, db=db)

    # Cleanup
    print("\n" + "=" * 60)
    print("Cleanup...")
    db_file = Path("spark_optimizer_advanced.db")
    if db_file.exists():
        db_file.unlink()
        print("  Removed example database")

    print("\n" + "=" * 60)
    print("Advanced tuning examples completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
