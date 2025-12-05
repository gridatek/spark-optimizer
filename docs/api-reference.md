# API Reference

## Overview

The Spark Resource Optimizer provides both a REST API and a CLI for accessing its functionality.

## REST API

Base URL: `http://localhost:8080` (default)

### Authentication

Currently, the API does not require authentication. This will be added in future versions.

### Response Format

All API responses follow this structure:

**Success Response**:
```json
{
    "status": "success",
    "data": { ... }
}
```

**Error Response**:
```json
{
    "status": "error",
    "error": "Error message",
    "code": "ERROR_CODE"
}
```

### Endpoints

---

#### Health Check

Check API server health status.

**Endpoint**: `GET /health`

**Response**:
```json
{
    "status": "healthy",
    "service": "spark-resource-optimizer",
    "version": "0.1.0"
}
```

**Example**:
```bash
curl http://localhost:8080/health
```

---

#### Get Recommendation

Get resource recommendations for a Spark job.

**Endpoint**: `POST /recommend`

**Request Body**:
```json
{
    "input_size_gb": 50.0,
    "job_type": "etl",
    "app_name": "daily_etl_job",
    "additional_params": {
        "output_format": "parquet",
        "num_partitions": 100
    }
}
```

**Parameters**:
- `input_size_gb` (required): Input data size in GB
- `job_type` (optional): Job type - `etl`, `ml`, `sql`, `streaming`
- `app_name` (optional): Application name for similarity matching
- `method` (optional): Recommendation method - `similarity`, `ml`, `rule_based`, `hybrid`
- `additional_params` (optional): Additional job-specific parameters

**Response**:
```json
{
    "configuration": {
        "executor_cores": 4,
        "executor_memory_mb": 8192,
        "num_executors": 10,
        "driver_memory_mb": 4096
    },
    "predicted_metrics": {
        "duration_minutes": 30,
        "cost_usd": 12.5
    },
    "confidence": 0.85,
    "method": "similarity",
    "metadata": {
        "similar_jobs": [
            {
                "app_id": "app-20240101-000001",
                "similarity": 0.92
            }
        ]
    }
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/recommend \
  -H "Content-Type: application/json" \
  -d '{
    "input_size_gb": 50.0,
    "job_type": "etl",
    "app_name": "daily_etl_job"
  }'
```

**Python Example**:
```python
import requests

response = requests.post(
    "http://localhost:8080/recommend",
    json={
        "input_size_gb": 50.0,
        "job_type": "etl",
        "app_name": "daily_etl_job"
    }
)

recommendation = response.json()
print(f"Recommended executors: {recommendation['configuration']['num_executors']}")
```

---

#### Collect Job Data

Collect and store Spark job data from various sources.

**Endpoint**: `POST /collect`

**Request Body**:
```json
{
    "source_type": "event_log",
    "source_path": "/path/to/spark/event-logs",
    "config": {
        "recursive": true,
        "batch_size": 100
    }
}
```

**Parameters**:
- `source_type` (required): Source type - `event_log`, `history_server`, `metrics`
- `source_path` (required): Path or URL to data source
- `config` (optional): Source-specific configuration

**Response**:
```json
{
    "status": "success",
    "jobs_collected": 150,
    "jobs_stored": 145,
    "errors": 5,
    "duration_seconds": 42.5
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/collect \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "event_log",
    "source_path": "/path/to/logs"
  }'
```

---

#### List Jobs

List stored Spark jobs with optional filtering and pagination.

**Endpoint**: `GET /jobs`

**Query Parameters**:
- `limit` (optional, default: 50): Maximum number of results
- `offset` (optional, default: 0): Pagination offset
- `app_name` (optional): Filter by application name (partial match)
- `user` (optional): Filter by user
- `date_from` (optional): Start date (ISO format)
- `date_to` (optional): End date (ISO format)
- `status` (optional): Filter by status - `completed`, `failed`, `running`

**Response**:
```json
{
    "jobs": [
        {
            "app_id": "app-20240101-000001",
            "app_name": "daily_etl_job",
            "user": "data_engineer",
            "start_time": "2024-01-01T10:00:00Z",
            "end_time": "2024-01-01T10:30:00Z",
            "duration_ms": 1800000,
            "status": "completed",
            "configuration": {
                "executor_cores": 4,
                "executor_memory_mb": 8192,
                "num_executors": 10
            }
        }
    ],
    "total": 150,
    "limit": 50,
    "offset": 0
}
```

**Example**:
```bash
# List recent jobs
curl http://localhost:8080/jobs?limit=10

# Filter by app name
curl http://localhost:8080/jobs?app_name=etl

# Date range query
curl "http://localhost:8080/jobs?date_from=2024-01-01&date_to=2024-01-31"
```

---

#### Get Job Details

Get detailed information about a specific Spark job.

**Endpoint**: `GET /jobs/{app_id}`

**Path Parameters**:
- `app_id`: Spark application ID

**Response**:
```json
{
    "app_id": "app-20240101-000001",
    "app_name": "daily_etl_job",
    "user": "data_engineer",
    "submit_time": "2024-01-01T09:59:50Z",
    "start_time": "2024-01-01T10:00:00Z",
    "end_time": "2024-01-01T10:30:00Z",
    "duration_ms": 1800000,
    "spark_version": "3.4.0",
    "status": "completed",

    "configuration": {
        "executor_cores": 4,
        "executor_memory_mb": 8192,
        "num_executors": 10,
        "driver_memory_mb": 4096
    },

    "metrics": {
        "total_tasks": 500,
        "failed_tasks": 0,
        "total_stages": 10,
        "failed_stages": 0,
        "input_bytes": 53687091200,
        "output_bytes": 26843545600,
        "shuffle_read_bytes": 10737418240,
        "shuffle_write_bytes": 10737418240
    },

    "stages": [
        {
            "stage_id": 0,
            "stage_name": "collect at <console>:25",
            "num_tasks": 50,
            "duration_ms": 120000,
            "input_bytes": 5368709120
        }
    ]
}
```

**Example**:
```bash
curl http://localhost:8080/jobs/app-20240101-000001
```

---

#### Analyze Job

Analyze a specific job and return insights, bottlenecks, and suggestions.

**Endpoint**: `GET /analyze/{app_id}`

**Path Parameters**:
- `app_id`: Spark application ID

**Response**:
```json
{
    "app_id": "app-20240101-000001",
    "app_name": "daily_etl_job",

    "analysis": {
        "resource_efficiency": {
            "cpu_efficiency": 0.75,
            "memory_efficiency": 0.82,
            "io_efficiency": 0.68
        },

        "bottlenecks": [
            {
                "type": "memory",
                "severity": "medium",
                "description": "High memory spill detected in stage 3",
                "affected_stages": [3, 5]
            }
        ],

        "issues": [
            {
                "type": "data_skew",
                "severity": "high",
                "description": "Task duration variance > 3x in stage 2",
                "recommendation": "Repartition data or use salting technique"
            }
        ]
    },

    "suggestions": [
        {
            "category": "memory",
            "suggestion": "Increase executor memory to 12GB",
            "expected_improvement": "Eliminate disk spill, ~20% faster"
        },
        {
            "category": "parallelism",
            "suggestion": "Increase number of executors to 15",
            "expected_improvement": "Better resource utilization"
        }
    ],

    "comparison": {
        "similar_jobs": [
            {
                "app_id": "app-20240102-000001",
                "performance_difference": "+15%",
                "cost_difference": "+$5"
            }
        ]
    }
}
```

**Example**:
```bash
curl http://localhost:8080/analyze/app-20240101-000001
```

---

#### Submit Feedback

Submit feedback on a recommendation to improve future predictions.

**Endpoint**: `POST /feedback`

**Request Body**:
```json
{
    "recommendation_id": 123,
    "actual_performance": {
        "duration_ms": 1750000,
        "cost_usd": 11.5,
        "success": true
    },
    "satisfaction_score": 0.9,
    "comments": "Recommendation worked well, job completed faster than expected"
}
```

**Parameters**:
- `recommendation_id` (required): ID of the recommendation
- `actual_performance` (optional): Actual job performance metrics
- `satisfaction_score` (optional): User satisfaction (0.0 to 1.0)
- `comments` (optional): Additional feedback text

**Response**:
```json
{
    "status": "success",
    "message": "Feedback recorded successfully",
    "feedback_id": 456
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/feedback \
  -H "Content-Type: application/json" \
  -d '{
    "recommendation_id": 123,
    "satisfaction_score": 0.9
  }'
```

---

## CLI Reference

### Installation

```bash
pip install spark-resource-optimizer
```

### Global Options

```bash
spark-optimizer [OPTIONS] COMMAND [ARGS]

Options:
  --config PATH          Configuration file path
  --log-level LEVEL      Logging level (DEBUG, INFO, WARNING, ERROR)
  --help                 Show help message
  --version              Show version
```

### Commands

---

#### collect

Collect Spark job data from various sources.

**Usage**:
```bash
spark-optimizer collect [OPTIONS]
```

**Options**:
- `--source TEXT`: Source type (event-logs, history-server, metrics) [required]
- `--path TEXT`: Path or URL to data source [required]
- `--batch-size INT`: Batch size for processing (default: 100)
- `--recursive`: Recursively process directories
- `--since TEXT`: Collect jobs since date (ISO format)
- `--user TEXT`: Filter by user
- `--dry-run`: Validate without storing

**Examples**:
```bash
# Collect from event logs
spark-optimizer collect --source event-logs --path /path/to/logs

# Collect with date filter
spark-optimizer collect --source event-logs --path /path/to/logs --since 2024-01-01

# Collect from History Server
spark-optimizer collect --source history-server --path http://localhost:18080

# Dry run
spark-optimizer collect --source event-logs --path /path/to/logs --dry-run
```

---

#### recommend

Get resource recommendations for a Spark job.

**Usage**:
```bash
spark-optimizer recommend [OPTIONS]
```

**Options**:
- `--input-size FLOAT`: Input data size in GB [required]
- `--job-type TEXT`: Job type (etl, ml, sql, streaming)
- `--app-name TEXT`: Application name for similarity matching
- `--method TEXT`: Recommendation method (similarity, ml, rule-based, hybrid)
- `--max-cost FLOAT`: Maximum acceptable cost in USD
- `--verbose`: Show detailed output
- `--output FORMAT`: Output format (text, json, yaml)

**Examples**:
```bash
# Basic recommendation
spark-optimizer recommend --input-size 50 --job-type etl

# With cost constraint
spark-optimizer recommend --input-size 100 --max-cost 50

# JSON output
spark-optimizer recommend --input-size 50 --output json

# Verbose output
spark-optimizer recommend --input-size 50 --verbose
```

**Output Example**:
```
Recommended Configuration:
  Executor Cores:    4
  Executor Memory:   8192 MB
  Number of Executors: 10
  Driver Memory:     4096 MB

Predicted Performance:
  Duration:          ~30 minutes
  Estimated Cost:    $12.50

Confidence:          85%
Method:             similarity
```

---

#### analyze

Analyze a Spark job and get insights.

**Usage**:
```bash
spark-optimizer analyze [OPTIONS] APP_ID
```

**Arguments**:
- `APP_ID`: Spark application ID [required]

**Options**:
- `--output FORMAT`: Output format (text, json, yaml)
- `--detailed`: Show detailed stage-level analysis

**Examples**:
```bash
# Analyze job
spark-optimizer analyze app-20240101-000001

# Detailed analysis
spark-optimizer analyze app-20240101-000001 --detailed

# JSON output
spark-optimizer analyze app-20240101-000001 --output json
```

---

#### list-jobs

List stored Spark jobs.

**Usage**:
```bash
spark-optimizer list-jobs [OPTIONS]
```

**Options**:
- `--limit INT`: Maximum number of results (default: 50)
- `--app-name TEXT`: Filter by application name
- `--user TEXT`: Filter by user
- `--since TEXT`: Start date (ISO format)
- `--until TEXT`: End date (ISO format)
- `--status TEXT`: Filter by status (completed, failed, running)
- `--output FORMAT`: Output format (table, json, csv)

**Examples**:
```bash
# List recent jobs
spark-optimizer list-jobs --limit 10

# Filter by app name
spark-optimizer list-jobs --app-name etl

# Date range
spark-optimizer list-jobs --since 2024-01-01 --until 2024-01-31

# CSV output
spark-optimizer list-jobs --output csv > jobs.csv
```

---

#### serve

Start the REST API server.

**Usage**:
```bash
spark-optimizer serve [OPTIONS]
```

**Options**:
- `--host TEXT`: Host address (default: 0.0.0.0)
- `--port INT`: Port number (default: 8080)
- `--debug`: Enable debug mode
- `--workers INT`: Number of worker processes (default: 4)

**Examples**:
```bash
# Start server
spark-optimizer serve

# Custom port
spark-optimizer serve --port 9090

# Debug mode
spark-optimizer serve --debug
```

---

#### train

Train ML models with historical job data.

**Usage**:
```bash
spark-optimizer train [OPTIONS]
```

**Options**:
- `--method TEXT`: Model type (ml, all)
- `--test-split FLOAT`: Test set fraction (default: 0.2)
- `--evaluate`: Evaluate model after training

**Examples**:
```bash
# Train ML models
spark-optimizer train --method ml

# Train and evaluate
spark-optimizer train --method ml --evaluate
```

---

## Error Codes

| Code | Description |
|------|-------------|
| `INVALID_REQUEST` | Malformed request body or parameters |
| `MISSING_PARAMETER` | Required parameter not provided |
| `JOB_NOT_FOUND` | Specified job ID does not exist |
| `INSUFFICIENT_DATA` | Not enough historical data for recommendation |
| `MODEL_NOT_TRAINED` | ML model has not been trained |
| `COLLECTION_FAILED` | Failed to collect data from source |
| `DATABASE_ERROR` | Database operation failed |
| `INTERNAL_ERROR` | Internal server error |

## Rate Limiting

Currently, no rate limiting is enforced. This will be added in future versions.

## Versioning

API version is included in responses and can be queried via the health endpoint. Breaking changes will result in a new major version.

## SDKs

### Python SDK

```python
from spark_optimizer import SparkOptimizer

# Initialize client
client = SparkOptimizer("http://localhost:8080")

# Get recommendation
rec = client.recommend(input_size_gb=50.0, job_type="etl")
print(rec.configuration)

# List jobs
jobs = client.list_jobs(limit=10, app_name="etl")

# Analyze job
analysis = client.analyze("app-20240101-000001")
print(analysis.suggestions)
```

## Support

For issues, questions, or feature requests:
- GitHub Issues: https://github.com/yourusername/spark-resource-optimizer/issues
- Documentation: https://spark-optimizer.readthedocs.io
