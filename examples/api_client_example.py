"""Example of using the REST API client."""

import requests
import json


class SparkOptimizerClient:
    """Simple client for the Spark Optimizer API."""

    def __init__(self, base_url="http://localhost:8080"):
        """Initialize API client.

        Args:
            base_url: Base URL of the API server
        """
        self.base_url = base_url.rstrip("/")

    def health_check(self):
        """Check API health.

        Returns:
            Health check response
        """
        response = requests.get(f"{self.base_url}/health")
        return response.json()

    def get_recommendation(self, input_size_gb, job_type=None, app_name=None):
        """Get resource recommendation.

        Args:
            input_size_gb: Input data size in GB
            job_type: Optional job type (etl, ml, sql, streaming)
            app_name: Optional application name

        Returns:
            Recommendation response
        """
        payload = {"input_size_gb": input_size_gb}
        if job_type:
            payload["job_type"] = job_type
        if app_name:
            payload["app_name"] = app_name

        response = requests.post(f"{self.base_url}/recommend", json=payload)
        return response.json()

    def list_jobs(self, limit=50, offset=0, app_name=None):
        """List stored jobs.

        Args:
            limit: Maximum number of results
            offset: Offset for pagination
            app_name: Optional filter by app name

        Returns:
            List of jobs
        """
        params = {"limit": limit, "offset": offset}
        if app_name:
            params["app_name"] = app_name

        response = requests.get(f"{self.base_url}/jobs", params=params)
        return response.json()

    def get_job_details(self, app_id):
        """Get details for a specific job.

        Args:
            app_id: Spark application ID

        Returns:
            Job details
        """
        response = requests.get(f"{self.base_url}/jobs/{app_id}")
        return response.json()

    def analyze_job(self, app_id):
        """Analyze a specific job.

        Args:
            app_id: Spark application ID

        Returns:
            Job analysis
        """
        response = requests.get(f"{self.base_url}/analyze/{app_id}")
        return response.json()

    def submit_feedback(self, recommendation_id, satisfaction_score, actual_performance=None):
        """Submit feedback on a recommendation.

        Args:
            recommendation_id: ID of the recommendation
            satisfaction_score: Score from 0.0 to 1.0
            actual_performance: Optional actual performance metrics

        Returns:
            Feedback submission response
        """
        payload = {
            "recommendation_id": recommendation_id,
            "satisfaction_score": satisfaction_score,
        }
        if actual_performance:
            payload["actual_performance"] = actual_performance

        response = requests.post(f"{self.base_url}/feedback", json=payload)
        return response.json()


def main():
    """Demonstrate API client usage."""
    print("Spark Resource Optimizer API Client Example\n")

    # Initialize client
    client = SparkOptimizerClient("http://localhost:8080")

    # Health check
    print("1. Health Check:")
    try:
        health = client.health_check()
        print(f"   Status: {health.get('status')}")
    except requests.exceptions.ConnectionError:
        print("   Error: Could not connect to API server")
        print("   Make sure the server is running: spark-optimizer serve")
        return

    # Get recommendation
    print("\n2. Get Recommendation:")
    recommendation = client.get_recommendation(
        input_size_gb=50.0,
        job_type="etl",
        app_name="example_job"
    )
    print(f"   Recommended Executors: {recommendation['configuration']['num_executors']}")
    print(f"   Executor Memory: {recommendation['configuration']['executor_memory_mb']} MB")
    print(f"   Confidence: {recommendation['confidence']}")

    # List jobs
    print("\n3. List Recent Jobs:")
    jobs = client.list_jobs(limit=10)
    print(f"   Total Jobs: {jobs.get('total', 0)}")

    # Analyze a job (example)
    print("\n4. Analyze Job:")
    print("   (Requires existing job ID)")
    # analysis = client.analyze_job("app-20240101-000001")

    # Submit feedback
    print("\n5. Submit Feedback:")
    feedback = client.submit_feedback(
        recommendation_id=1,
        satisfaction_score=0.9
    )
    print(f"   Feedback: {feedback.get('message')}")


if __name__ == "__main__":
    main()
