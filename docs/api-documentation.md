# Spark Resource Optimizer API Documentation

## Overview

The Spark Resource Optimizer REST API provides endpoints for optimizing Apache Spark resource allocation and analyzing job performance.

## Base URL

- Development: `http://localhost:5000`
- Production: `http://localhost:8080`

## Authentication

Currently, the API does not require authentication. This may be added in future versions.

## API Endpoints

### Health Check

**GET** `/health`

Check if the API server is running and healthy.

**Response:**
```json
{
  "status": "healthy",
  "version": "1.0.0"
}
```

---

### Get Resource Recommendations

**POST** `/api/v1/recommend`

Get recommended Spark resource configuration for a new job.

**Request Body:**
```json
{
  "input_size_bytes": 10737418240,
  "job_type": "etl",
  "priority": "balanced",
  "sla_minutes": 60,
  "budget_dollars": 100.0
}
```

**Parameters:**
- `input_size_bytes` (required): Expected input data size in bytes
- `job_type` (optional): Type of job (`etl`, `ml`, `streaming`, `batch`, `interactive`)
- `priority` (optional): Optimization priority (`performance`, `cost`, `balanced`)
- `sla_minutes` (optional): Maximum acceptable duration in minutes
- `budget_dollars` (optional): Maximum acceptable cost in dollars

**Response:**
```json
{
  "configuration": {
    "executor_cores": 4,
    "executor_memory_mb": 8192,
    "num_executors": 10,
    "driver_memory_mb": 4096
  },
  "confidence": 0.85,
  "metadata": {
    "method": "rule_based",
    "job_type": "etl",
    "priority": "balanced",
    "rules_applied": 3
  }
}
```

---

### List Jobs

**GET** `/api/v1/jobs`

Retrieve a list of Spark jobs with optional filtering.

**Query Parameters:**
- `limit` (optional): Maximum number of jobs to return (default: 20, max: 100)
- `job_type` (optional): Filter by job type
- `min_duration` (optional): Minimum duration in milliseconds
- `max_duration` (optional): Maximum duration in milliseconds

**Response:**
```json
{
  "jobs": [
    {
      "app_id": "app-20241207-123456-0000",
      "app_name": "ETL Pipeline",
      "duration_ms": 300000,
      "input_bytes": 10737418240,
      "num_executors": 10
    }
  ],
  "total": 42,
  "limit": 20
}
```

---

### Get Job Details

**GET** `/api/v1/jobs/{app_id}`

Retrieve detailed information about a specific Spark job.

**Path Parameters:**
- `app_id`: Application ID (e.g., `app-20241207-123456-0000`)

**Response:**
```json
{
  "app_id": "app-20241207-123456-0000",
  "app_name": "ETL Pipeline",
  "duration_ms": 300000,
  "executor_cores": 4,
  "executor_memory_mb": 8192,
  "num_executors": 10,
  "total_tasks": 1000,
  "failed_tasks": 5,
  "disk_spilled_bytes": 1073741824
}
```

---

### Analyze Job Performance

**GET** `/api/v1/jobs/{app_id}/analyze`

Analyze a completed Spark job to identify performance issues and generate recommendations.

**Path Parameters:**
- `app_id`: Application ID

**Response:**
```json
{
  "app_id": "app-20241207-123456-0000",
  "app_name": "ETL Pipeline",
  "analysis": {
    "total_recommendations": 3,
    "critical": 1,
    "warnings": 2,
    "info": 0,
    "health_score": 70.0
  },
  "current_configuration": {
    "executor_cores": 4,
    "executor_memory_mb": 4096,
    "num_executors": 10,
    "driver_memory_mb": 2048
  },
  "recommended_configuration": {
    "executor_cores": 4,
    "executor_memory_mb": 6144,
    "num_executors": 10,
    "driver_memory_mb": 2048
  },
  "spark_configs": {
    "spark.executor.memory": "6144m",
    "spark.sql.adaptive.enabled": "true"
  },
  "recommendations": [
    {
      "rule_id": "MEM001",
      "title": "Memory Spilling Detected",
      "description": "Job spilled 2.00 GB to disk/memory",
      "severity": "critical",
      "current_value": "4096 MB",
      "recommended_value": "6144 MB",
      "expected_improvement": "20-50% faster execution",
      "spark_configs": {
        "spark.executor.memory": "6144m"
      }
    }
  ]
}
```

**Health Score:** 0-100 score indicating job health:
- 80-100: Healthy (no major issues)
- 60-79: Minor issues (warnings present)
- 0-59: Significant issues (critical problems detected)

**Optimization Rules:**
- `MEM001`: Memory spilling detection
- `MEM002`: Memory fraction optimization
- `SHUFFLE001`: Shuffle explosion detection
- `PARTITION001`: Partition count optimization
- `GC001`: GC pressure detection
- `SKEW001`: Data skew detection
- `IO001`: Small files detection

---

### Collect Jobs from History Server

**POST** `/api/v1/collect`

Fetch job data from Spark History Server and store in the database.

**Request Body:**
```json
{
  "history_server_url": "http://localhost:18080",
  "max_apps": 100,
  "status": "completed",
  "min_date": "2024-01-01T00:00:00"
}
```

**Parameters:**
- `history_server_url` (required): URL of the Spark History Server
- `max_apps` (optional): Maximum number of applications to fetch (default: 100, max: 1000)
- `status` (optional): Application status filter (`completed`, `running`, `failed`)
- `min_date` (optional): Minimum date for applications (ISO format)

**Response:**
```json
{
  "success": true,
  "collected": 42,
  "failed": 2,
  "skipped": 5,
  "message": "Successfully collected 42 jobs from History Server"
}
```

---

### Compare Multiple Jobs

**POST** `/api/v1/compare`

Compare 2-10 Spark jobs side-by-side to identify patterns and performance differences.

**Request Body:**
```json
{
  "app_ids": [
    "app-20241207-123456-0000",
    "app-20241207-234567-0001",
    "app-20241207-345678-0002"
  ]
}
```

**Parameters:**
- `app_ids` (required): List of 2-10 application IDs to compare
- `metrics` (optional): Specific metrics to compare

**Response:**
```json
{
  "jobs": [
    {
      "app_id": "app-20241207-123456-0000",
      "app_name": "ETL Pipeline v1",
      "duration_ms": 300000,
      "throughput_gbps": 0.035,
      "spill_ratio": 0.0
    }
  ],
  "summary": {
    "fastest": {
      "app_id": "app-20241207-123456-0000",
      "app_name": "ETL Pipeline v1",
      "duration_ms": 300000
    },
    "most_efficient": {
      "app_id": "app-20241207-123456-0000",
      "app_name": "ETL Pipeline v1",
      "throughput_gbps": 0.035
    }
  },
  "recommendations": [
    {
      "type": "spilling",
      "observation": "2 job(s) experienced spilling while 1 did not",
      "recommendation": "Jobs without spilling had sufficient memory allocation"
    }
  ],
  "compared_count": 3
}
```

**Efficiency Metrics:**
- `throughput_gbps`: GB processed per second
- `data_per_executor_hour_gb`: GB processed per executor-hour
- `spill_ratio`: Spilled data / input data ratio

---

### Get Aggregate Statistics

**GET** `/api/v1/stats`

Retrieve aggregate statistics across all jobs in the database.

**Response:**
```json
{
  "total_jobs": 150,
  "avg_duration_ms": 450000,
  "total_input_gb": 5120.5
}
```

---

## Error Responses

All endpoints return consistent error responses:

```json
{
  "error": "Error message",
  "message": "Additional details (optional)"
}
```

**HTTP Status Codes:**
- `200`: Success
- `400`: Bad Request (validation error)
- `404`: Not Found
- `500`: Internal Server Error
- `503`: Service Unavailable (e.g., History Server unreachable)

---

## OpenAPI Specification

The full OpenAPI 3.0 specification is available at:
- YAML format: `/api/v1/openapi.yaml`
- Interactive UI: Use tools like Swagger UI or Redoc to visualize the spec

---

## Example Usage

### Python

```python
import requests

# Get recommendations
response = requests.post(
    "http://localhost:5000/api/v1/recommend",
    json={
        "input_size_bytes": 10 * 1024**3,
        "job_type": "etl",
        "priority": "balanced"
    }
)
config = response.json()["configuration"]
print(f"Recommended executors: {config['num_executors']}")

# Analyze a job
response = requests.get(
    "http://localhost:5000/api/v1/jobs/app-123/analyze"
)
analysis = response.json()
print(f"Health score: {analysis['analysis']['health_score']}")

# Collect from History Server
response = requests.post(
    "http://localhost:5000/api/v1/collect",
    json={
        "history_server_url": "http://localhost:18080",
        "max_apps": 50
    }
)
print(f"Collected: {response.json()['collected']} jobs")
```

### cURL

```bash
# Get recommendations
curl -X POST http://localhost:5000/api/v1/recommend \
  -H "Content-Type: application/json" \
  -d '{
    "input_size_bytes": 10737418240,
    "job_type": "etl",
    "priority": "balanced"
  }'

# Analyze a job
curl http://localhost:5000/api/v1/jobs/app-123/analyze

# Compare jobs
curl -X POST http://localhost:5000/api/v1/compare \
  -H "Content-Type: application/json" \
  -d '{
    "app_ids": ["app-1", "app-2", "app-3"]
  }'
```

---

## Rate Limiting

Currently, there are no rate limits. This may be added in future versions.

## Versioning

The API uses URL versioning (`/api/v1/`). Breaking changes will result in a new version.

## Support

For issues and questions, please open an issue on GitHub.
