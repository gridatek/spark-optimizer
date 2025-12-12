#!/usr/bin/env python3
"""
Example: Collecting Spark metrics from Databricks workspace

This example demonstrates how to:
1. Connect to a Databricks workspace
2. Discover and monitor clusters
3. Collect Spark application metrics
4. Track DBU-based costs
5. Get cluster type recommendations
6. Store metrics in a database

Prerequisites:
- pip install spark-optimizer[databricks]
- Databricks workspace URL
- Databricks access token (PAT)
"""

import os
import sys
from typing import Dict, List
from datetime import datetime

try:
    from spark_optimizer.collectors import DatabricksCollector
    from spark_optimizer.storage import Database
except ImportError as e:
    print(f"Error: {e}")
    print("\nPlease install the required packages:")
    print("pip install spark-optimizer")
    sys.exit(1)


def print_section(title: str):
    """Print a formatted section header."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70 + "\n")


def get_databricks_credentials() -> tuple:
    """Get Databricks credentials from environment or user input."""
    workspace_url = os.environ.get("DATABRICKS_WORKSPACE_URL")
    token = os.environ.get("DATABRICKS_TOKEN")

    if not workspace_url:
        print("Enter your Databricks workspace URL")
        print("(e.g., https://dbc-xxx.cloud.databricks.com):")
        workspace_url = input("> ").strip()

    if not token:
        print("\nEnter your Databricks access token:")
        print("(Generate from User Settings > Access Tokens)")
        token = input("> ").strip()

    return workspace_url, token


def validate_connection(collector: DatabricksCollector) -> bool:
    """Validate Databricks connection."""
    print_section("Validating Databricks Connection")

    print(f"Workspace URL: {collector.workspace_url}")
    print("Validating credentials...")

    if collector.validate_config():
        print("✓ Connection successful!")
        return True
    else:
        print("✗ Connection failed. Please check your credentials.")
        return False


def collect_metrics(
    collector: DatabricksCollector, config: Dict
) -> List[Dict]:
    """Collect metrics from Databricks clusters."""
    print_section("Collecting Metrics from Databricks")

    print("Configuration:")
    print(f"  - Cluster filter: {config.get('cluster_ids', 'All clusters')}")
    print(f"  - Max clusters: {config.get('max_clusters', 10)}")
    print(f"  - Days back: {config.get('days_back', 7)}")
    print(f"  - Collect costs: {config.get('collect_costs', True)}")
    print(f"  - DBU price: ${config.get('dbu_price', 0.40)}")

    print("\nCollecting data...")
    jobs = collector.collect()

    print(f"\n✓ Collected {len(jobs)} Spark applications")
    return jobs


def display_metrics(jobs: List[Dict]):
    """Display collected metrics summary."""
    print_section("Metrics Summary")

    if not jobs:
        print("No metrics collected.")
        return

    total_duration = sum(job.get("duration_ms", 0) for job in jobs)
    total_tasks = sum(job.get("total_tasks", 0) for job in jobs)
    total_cost = sum(job.get("estimated_cost", 0) for job in jobs)

    print(f"Total Applications: {len(jobs)}")
    print(f"Total Duration: {total_duration / 1000 / 60:.2f} minutes")
    print(f"Total Tasks: {total_tasks}")
    print(f"Estimated Cost: ${total_cost:.2f}")

    print("\nTop 5 Applications by Duration:")
    sorted_jobs = sorted(
        jobs, key=lambda x: x.get("duration_ms", 0), reverse=True
    )
    for i, job in enumerate(sorted_jobs[:5], 1):
        duration_min = job.get("duration_ms", 0) / 1000 / 60
        cost = job.get("estimated_cost", 0)
        print(f"  {i}. {job['app_name']}")
        print(f"     Duration: {duration_min:.2f} min | Cost: ${cost:.2f}")


def get_recommendations(collector: DatabricksCollector):
    """Get cluster type recommendations."""
    print_section("Cluster Recommendations")

    print("Select workload type:")
    print("  1. Memory-intensive (ML, Graph Processing)")
    print("  2. I/O-intensive (Streaming, ETL)")
    print("  3. SQL Analytics")
    print("  4. General ETL")

    choice = input("\nEnter choice (1-4): ").strip()

    workload_profiles = {
        "1": {"memory_intensive": True, "job_type": "ml"},
        "2": {"io_intensive": True, "job_type": "streaming"},
        "3": {"job_type": "sql"},
        "4": {"job_type": "etl"},
    }

    workload = workload_profiles.get(choice, {"job_type": "etl"})

    print("\nEnter current cluster type")
    print("(e.g., Standard_DS4_v2 for Azure or i3.2xlarge for AWS):")
    current_type = input("> ").strip()

    try:
        recommendation = collector.get_cluster_recommendations(
            current_type, workload
        )

        print("\n" + "-" * 70)
        print(f"Current Cluster: {recommendation['current_cluster_type']}")
        print(f"Recommended: {recommendation['recommended_cluster_type']}")
        print(f"Reason: {recommendation['reason']}")
        print(
            f"Cost Impact: {recommendation['estimated_cost_change_percent']:+.1f}%"
        )
        print(
            f"Autoscaling: {'Yes' if recommendation['autoscaling_recommended'] else 'No'}"
        )
        print("-" * 70)
    except KeyError as e:
        print(f"\n✗ Unknown cluster type: {current_type}")
        print("Please use a supported cluster type.")


def store_in_database(jobs: List[Dict]):
    """Store collected metrics in database."""
    print_section("Storing Metrics in Database")

    db_url = os.environ.get("DATABASE_URL")

    if not db_url:
        print("Database URL not found in environment.")
        print("Set DATABASE_URL to enable storage.")
        print(
            "Example: export DATABASE_URL='postgresql://user:pass@localhost/spark_metrics'"
        )
        return

    try:
        db = Database(db_url)
        stored = 0

        for job in jobs:
            db.save_job(job)
            stored += 1

        print(f"✓ Stored {stored} jobs in database")
    except Exception as e:
        print(f"✗ Database error: {e}")


def main():
    """Main example workflow."""
    print("\n" + "=" * 70)
    print("  Databricks Metrics Collection Example")
    print("=" * 70)

    # Get credentials
    workspace_url, token = get_databricks_credentials()

    # Configuration
    config = {
        "cluster_ids": [],  # Empty = all clusters
        "max_clusters": 10,
        "days_back": 7,
        "collect_costs": True,
        "dbu_price": 0.40,  # Adjust based on your pricing
    }

    # Initialize collector
    collector = DatabricksCollector(
        workspace_url=workspace_url, token=token, config=config
    )

    # Validate connection
    if not validate_connection(collector):
        return

    # Main menu
    while True:
        print("\n" + "-" * 70)
        print("What would you like to do?")
        print("  1. Collect metrics from clusters")
        print("  2. Get cluster recommendations")
        print("  3. Collect and store in database")
        print("  4. Exit")
        print("-" * 70)

        choice = input("Enter choice (1-4): ").strip()

        if choice == "1":
            jobs = collect_metrics(collector, config)
            display_metrics(jobs)

        elif choice == "2":
            get_recommendations(collector)

        elif choice == "3":
            jobs = collect_metrics(collector, config)
            display_metrics(jobs)
            if jobs:
                store_in_database(jobs)

        elif choice == "4":
            print("\nGoodbye!")
            break

        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)
