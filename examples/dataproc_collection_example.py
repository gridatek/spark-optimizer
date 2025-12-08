#!/usr/bin/env python3
"""
Example: Collecting Spark metrics from GCP Dataproc clusters

This example demonstrates how to:
1. Connect to GCP Dataproc
2. Discover and monitor clusters
3. Collect Spark application metrics
4. Track costs based on machine types
5. Get machine type recommendations
6. Store metrics in a database

Prerequisites:
- pip install spark-resource-optimizer[gcp]
- GCP credentials configured (gcloud auth or service account)
- GCP project with Dataproc clusters
"""

import os
import sys
from typing import Dict, List

try:
    from spark_optimizer.collectors import DataprocCollector
    from spark_optimizer.storage import Database
except ImportError as e:
    print(f"Error: {e}")
    print("\nPlease install the required packages:")
    print("pip install spark-resource-optimizer[gcp]")
    sys.exit(1)


def print_section(title: str):
    """Print a formatted section header."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70 + "\n")


def get_gcp_credentials() -> tuple:
    """Get GCP project and region from environment or user input."""
    project_id = os.environ.get("GCP_PROJECT_ID")
    region = os.environ.get("GCP_REGION", "us-central1")

    if not project_id:
        print("Enter your GCP project ID:")
        project_id = input("> ").strip()

    print(f"\nUsing region: {region}")
    change = input("Change region? (y/n): ").strip().lower()
    if change == "y":
        print("Enter GCP region (e.g., us-central1, us-west1, europe-west1):")
        region = input("> ").strip()

    return project_id, region


def validate_connection(collector: DataprocCollector) -> bool:
    """Validate GCP Dataproc connection."""
    print_section("Validating GCP Dataproc Connection")

    print(f"Project ID: {collector.project_id}")
    print(f"Region: {collector.region}")
    print("Validating credentials...")

    if collector.validate_config():
        print("✓ Connection successful!")
        print("✓ Credentials are valid")
        print("✓ Dataproc API is accessible")
        return True
    else:
        print("✗ Connection failed.")
        print("\nTroubleshooting:")
        print("  1. Check that gcloud is authenticated:")
        print("     gcloud auth application-default login")
        print("  2. Verify GOOGLE_APPLICATION_CREDENTIALS is set")
        print("  3. Ensure service account has dataproc.viewer role")
        return False


def collect_metrics(
    collector: DataprocCollector, config: Dict
) -> List[Dict]:
    """Collect metrics from Dataproc clusters."""
    print_section("Collecting Metrics from Dataproc")

    print("Configuration:")
    print(f"  - Cluster filter: {config.get('cluster_names', 'All clusters')}")
    print(f"  - Label filter: {config.get('cluster_labels', 'None')}")
    print(f"  - Max clusters: {config.get('max_clusters', 10)}")
    print(f"  - Days back: {config.get('days_back', 7)}")
    print(f"  - Collect costs: {config.get('collect_costs', True)}")
    print(f"  - Include preemptible: {config.get('include_preemptible', True)}")

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

    # Group by cluster
    clusters = {}
    for job in jobs:
        cluster_name = job["tags"].get("cluster_name", "unknown")
        if cluster_name not in clusters:
            clusters[cluster_name] = []
        clusters[cluster_name].append(job)

    print(f"\nClusters: {len(clusters)}")
    for cluster_name, cluster_jobs in clusters.items():
        cluster_cost = sum(j.get("estimated_cost", 0) for j in cluster_jobs)
        print(f"  - {cluster_name}: {len(cluster_jobs)} jobs, ${cluster_cost:.2f}")

    print("\nTop 5 Applications by Duration:")
    sorted_jobs = sorted(
        jobs, key=lambda x: x.get("duration_ms", 0), reverse=True
    )
    for i, job in enumerate(sorted_jobs[:5], 1):
        duration_min = job.get("duration_ms", 0) / 1000 / 60
        cost = job.get("estimated_cost", 0)
        machine_type = job["tags"].get("machine_type", "unknown")
        print(f"  {i}. {job['app_name']}")
        print(
            f"     Duration: {duration_min:.2f} min | "
            f"Cost: ${cost:.2f} | "
            f"Machine: {machine_type}"
        )


def get_recommendations(collector: DataprocCollector):
    """Get machine type recommendations."""
    print_section("Machine Type Recommendations")

    print("Select workload type:")
    print("  1. Memory-intensive (ML, Graph Processing)")
    print("  2. Compute-intensive (Streaming, Real-time)")
    print("  3. Cost-optimized (Batch Processing)")
    print("  4. Balanced ETL")

    choice = input("\nEnter choice (1-4): ").strip()

    workload_profiles = {
        "1": {"memory_intensive": True, "job_type": "ml"},
        "2": {"compute_intensive": True, "job_type": "streaming"},
        "3": {"cost_optimized": True, "job_type": "batch"},
        "4": {"job_type": "etl"},
    }

    workload = workload_profiles.get(choice, {"job_type": "etl"})

    print("\nEnter current machine type")
    print("(e.g., n1-standard-8, n2-highmem-16, e2-standard-4):")
    current_type = input("> ").strip()

    try:
        recommendation = collector.get_machine_type_recommendations(
            current_type, workload
        )

        print("\n" + "-" * 70)
        print(f"Current Machine: {recommendation['current_machine_type']}")
        print(f"  Cores: {recommendation['current_specs']['cores']}")
        print(f"  Memory: {recommendation['current_specs']['memory_gb']} GB")
        print(f"  Price: ${recommendation['current_specs']['price']}/hr")

        print(f"\nRecommended: {recommendation['recommended_machine_type']}")
        print(f"  Cores: {recommendation['recommended_specs']['cores']}")
        print(f"  Memory: {recommendation['recommended_specs']['memory_gb']} GB")
        print(f"  Price: ${recommendation['recommended_specs']['price']}/hr")

        print(f"\nReason: {recommendation['reason']}")
        print(
            f"Cost Impact: {recommendation['estimated_cost_change_percent']:+.1f}%"
        )
        print(
            f"Use Preemptible: {'Yes (save ~80%)' if recommendation['preemptible_recommended'] else 'No'}"
        )
        print("-" * 70)
    except KeyError as e:
        print(f"\n✗ Unknown machine type: {current_type}")
        print("Please use a supported machine type (n1, n2, e2, c2 families).")


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
    print("  GCP Dataproc Metrics Collection Example")
    print("=" * 70)

    # Get credentials
    project_id, region = get_gcp_credentials()

    # Configuration
    config = {
        "cluster_names": [],  # Empty = all clusters
        "cluster_labels": {},  # Filter by labels
        "max_clusters": 10,
        "days_back": 7,
        "collect_costs": True,
        "include_preemptible": True,
        "preemptible_discount": 0.8,
    }

    # Initialize collector
    try:
        collector = DataprocCollector(
            project_id=project_id, region=region, config=config
        )
    except Exception as e:
        print(f"\n✗ Failed to initialize collector: {e}")
        print("\nMake sure you have authenticated with GCP:")
        print("  gcloud auth application-default login")
        return

    # Validate connection
    if not validate_connection(collector):
        return

    # Main menu
    while True:
        print("\n" + "-" * 70)
        print("What would you like to do?")
        print("  1. Collect metrics from clusters")
        print("  2. Get machine type recommendations")
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
        import traceback

        traceback.print_exc()
        sys.exit(1)
