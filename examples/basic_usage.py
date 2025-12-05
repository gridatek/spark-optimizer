"""Basic usage example for Spark Resource Optimizer."""

from spark_optimizer.collectors.event_log_collector import EventLogCollector
from spark_optimizer.storage.database import Database
from spark_optimizer.recommender.similarity_recommender import SimilarityRecommender


def main():
    """Demonstrate basic usage of the optimizer."""

    # 1. Collect data from Spark event logs
    print("Step 1: Collecting data from event logs...")
    collector = EventLogCollector(event_log_path="./sample_event_logs/")

    # TODO: Uncomment when implementation is complete
    # jobs_data = collector.collect()
    # print(f"Collected {len(jobs_data)} jobs")

    # 2. Store data in database
    print("\nStep 2: Storing data in database...")
    db = Database("sqlite:///spark_optimizer.db")
    db.create_tables()

    # TODO: Uncomment when implementation is complete
    # with db.get_session() as session:
    #     repo = SparkApplicationRepository(session)
    #     for job in jobs_data:
    #         repo.create(job)

    # 3. Get recommendations for a new job
    print("\nStep 3: Getting recommendations...")
    recommender = SimilarityRecommender()

    # TODO: Uncomment when implementation is complete
    # recommender.train(jobs_data)

    job_requirements = {
        "input_size_gb": 50.0,
        "job_type": "etl",
        "app_name": "my_etl_job",
    }

    # TODO: Uncomment when implementation is complete
    # recommendation = recommender.recommend(job_requirements)
    # print("\nRecommended Configuration:")
    # print(f"  Executor Cores: {recommendation['configuration']['executor_cores']}")
    # print(f"  Executor Memory: {recommendation['configuration']['executor_memory_mb']} MB")
    # print(f"  Number of Executors: {recommendation['configuration']['num_executors']}")
    # print(f"  Driver Memory: {recommendation['configuration']['driver_memory_mb']} MB")
    # print(f"  Confidence: {recommendation['confidence']:.2f}")

    print("\nNote: Implementation is not yet complete. Uncomment code blocks when ready.")


if __name__ == "__main__":
    main()
