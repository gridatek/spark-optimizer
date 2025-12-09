"""Basic usage example for Spark Resource Optimizer.

This example demonstrates the core functionality of the optimizer:
1. Collecting data from Spark event logs
2. Storing data in a database
3. Getting recommendations for new jobs

To run this example:
    python examples/basic_usage.py

Note: This example uses a sample event log directory. To test with real data,
point the event_log_path to your Spark event log directory.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from spark_optimizer.collectors.event_log_collector import EventLogCollector
from spark_optimizer.storage.database import Database
from spark_optimizer.storage.repository import SparkApplicationRepository
from spark_optimizer.recommender.similarity_recommender import SimilarityRecommender


def main():
    """Demonstrate basic usage of the optimizer."""
    print("=" * 60)
    print("Spark Resource Optimizer - Basic Usage Example")
    print("=" * 60)

    # Use an in-memory database for this example
    db_url = "sqlite:///spark_optimizer_example.db"

    # 1. Initialize the database
    print("\nStep 1: Initializing database...")
    db = Database(db_url)
    db.create_tables()
    print("  Database initialized successfully")

    # 2. Check for event logs (optional - if you have them)
    sample_log_dir = Path("./sample_event_logs")
    if sample_log_dir.exists() and any(sample_log_dir.iterdir()):
        print(f"\nStep 2: Collecting data from event logs in {sample_log_dir}...")
        collector = EventLogCollector(str(sample_log_dir))

        try:
            jobs_collected = 0
            with db.get_session() as session:
                repo = SparkApplicationRepository(session)

                for job_metrics in collector.collect_all():
                    # Convert dataclass to dict for storage
                    from dataclasses import asdict
                    job_data = asdict(job_metrics)

                    # Convert memory strings to MB for storage
                    from spark_optimizer.collectors.event_log_collector import (
                        memory_string_to_bytes,
                    )
                    job_data["executor_memory_mb"] = memory_string_to_bytes(
                        job_data.pop("executor_memory")
                    ) // (1024 * 1024)
                    job_data["driver_memory_mb"] = memory_string_to_bytes(
                        job_data.pop("driver_memory")
                    ) // (1024 * 1024)

                    # Rename fields to match model
                    job_data["disk_spilled_bytes"] = job_data.pop("spill_disk_bytes")
                    job_data["memory_spilled_bytes"] = job_data.pop("spill_memory_bytes")
                    job_data["executor_run_time_ms"] = job_data.pop("executor_run_time")
                    job_data["executor_cpu_time_ms"] = job_data.pop("executor_cpu_time")
                    job_data["jvm_gc_time_ms"] = job_data.pop("jvm_gc_time")
                    job_data.pop("jvm_heap_memory", None)
                    job_data.pop("driver_cores", None)

                    # Check if job already exists
                    if not repo.get_by_app_id(job_data["app_id"]):
                        repo.create(job_data)
                        jobs_collected += 1

                session.commit()

            print(f"  Collected {jobs_collected} jobs from event logs")
        except Exception as e:
            print(f"  Warning: Could not collect from event logs: {e}")
    else:
        print("\nStep 2: No event logs found (skipping collection)")
        print(f"  To test with real data, create a '{sample_log_dir}' directory")
        print("  with Spark event logs from your cluster.")

    # 3. Get recommendations for a new job
    print("\nStep 3: Getting recommendations for a new job...")

    # Initialize the recommender
    recommender = SimilarityRecommender(db=db)

    # Define job requirements
    job_requirements = {
        "input_size_gb": 50.0,
        "job_type": "etl",
        "app_name": "my_etl_pipeline",
    }

    print(f"\n  Job Requirements:")
    print(f"    Input Size: {job_requirements['input_size_gb']} GB")
    print(f"    Job Type: {job_requirements['job_type']}")
    print(f"    App Name: {job_requirements['app_name']}")

    # Get recommendation
    recommendation = recommender.recommend(
        input_size_bytes=int(job_requirements["input_size_gb"] * 1024**3),
        job_type=job_requirements.get("job_type"),
        priority="balanced",
    )

    # Display recommendation
    print("\n  Recommended Configuration:")
    config = recommendation.get("configuration", recommendation)
    print(f"    Executor Cores: {config.get('executor_cores', 'N/A')}")
    print(f"    Executor Memory: {config.get('executor_memory_mb', 'N/A')} MB")
    print(f"    Number of Executors: {config.get('num_executors', 'N/A')}")
    print(f"    Driver Memory: {config.get('driver_memory_mb', 'N/A')} MB")

    confidence = recommendation.get("confidence", recommendation.get("confidence_score", 0))
    print(f"\n  Confidence: {confidence:.2%}")

    metadata = recommendation.get("metadata", {})
    print(f"  Method: {metadata.get('method', 'fallback')}")

    # 4. Show example Spark submit command
    print("\nStep 4: Example spark-submit command:")
    print("-" * 60)
    spark_submit = f"""spark-submit \\
  --master yarn \\
  --deploy-mode cluster \\
  --num-executors {config.get('num_executors', 4)} \\
  --executor-cores {config.get('executor_cores', 4)} \\
  --executor-memory {config.get('executor_memory_mb', 4096)}m \\
  --driver-memory {config.get('driver_memory_mb', 2048)}m \\
  --conf spark.sql.shuffle.partitions={recommendation.get('shuffle_partitions', 200)} \\
  your_spark_job.py"""
    print(spark_submit)
    print("-" * 60)

    # 5. Clean up (remove example database)
    print("\nStep 5: Cleanup...")
    db_file = Path("spark_optimizer_example.db")
    if db_file.exists():
        db_file.unlink()
        print("  Removed example database")

    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)

    return recommendation


if __name__ == "__main__":
    main()
