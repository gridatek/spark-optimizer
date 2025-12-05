# Recommendation Engine

## Overview

The recommendation engine analyzes historical Spark job data to suggest optimal resource configurations for new jobs. It supports multiple recommendation strategies, each with different strengths and use cases.

## Recommendation Strategies

### 1. Similarity-Based Recommender

**How It Works**:
1. Extract features from the target job requirements
2. Calculate similarity scores with historical jobs
3. Find top-k most similar jobs
4. Average their resource configurations
5. Adjust for scale differences

**Similarity Calculation**:
```python
similarity_score = weighted_sum([
    size_similarity(job1.input_bytes, job2.input_bytes) * 0.3,
    text_similarity(job1.app_name, job2.app_name) * 0.2,
    stage_similarity(job1.num_stages, job2.num_stages) * 0.15,
    config_similarity(job1.config, job2.config) * 0.15,
    shuffle_similarity(job1.shuffle_bytes, job2.shuffle_bytes) * 0.1,
    output_similarity(job1.output_bytes, job2.output_bytes) * 0.1
])
```

**Advantages**:
- No training required
- Interpretable results
- Works with small datasets
- Captures domain-specific patterns

**Limitations**:
- Requires similar historical jobs
- May not generalize well
- Sensitive to feature weights
- Limited extrapolation capability

**Best For**:
- Recurring jobs with variations
- Jobs with similar names/patterns
- When historical data is limited
- Quick cold-start recommendations

**Example**:
```python
from spark_optimizer.recommender.similarity_recommender import SimilarityRecommender

recommender = SimilarityRecommender(config={"min_similarity": 0.7})
recommender.train(historical_jobs)

recommendation = recommender.recommend({
    "input_size_gb": 50.0,
    "job_type": "etl",
    "app_name": "daily_sales_aggregation"
})

print(f"Recommended Executors: {recommendation['configuration']['num_executors']}")
print(f"Confidence: {recommendation['confidence']}")
print(f"Similar Jobs: {len(recommendation['metadata']['similar_jobs'])}")
```

### 2. ML-Based Recommender

**How It Works**:
1. Extract features from job characteristics
2. Train regression models for each resource type
3. Predict optimal values for new jobs
4. Apply constraints and validation
5. Return predictions with confidence scores

**Models Used**:
- **Random Forest Regressor**: Handles non-linear relationships
- **Gradient Boosting**: Captures complex patterns
- **Neural Networks**: For large datasets (future)

**Features**:
```python
features = [
    # Size features
    "input_size_gb",
    "log_input_size",
    "output_size_gb",
    "shuffle_size_gb",

    # Complexity features
    "num_stages",
    "num_tasks",
    "avg_tasks_per_stage",

    # I/O features
    "io_ratio",  # output/input
    "shuffle_to_input_ratio",

    # Historical features
    "avg_stage_duration_ms",
    "task_parallelism"
]
```

**Training Process**:
```python
from spark_optimizer.recommender.ml_recommender import MLRecommender
from spark_optimizer.analyzer.feature_extractor import FeatureExtractor

# Extract features
extractor = FeatureExtractor()
X = extractor.create_feature_matrix(historical_jobs)

# Train models
recommender = MLRecommender()
recommender.train(historical_jobs)

# Evaluate
metrics = recommender.evaluate(test_jobs)
print(f"MAE for executors: {metrics['mae_num_executors']:.2f}")
```

**Advantages**:
- Learns complex patterns
- Good generalization
- Handles large datasets
- Can predict multiple metrics

**Limitations**:
- Requires substantial training data
- Black-box predictions
- Needs periodic retraining
- Computational overhead

**Best For**:
- Large historical datasets
- Diverse job types
- When accuracy is critical
- Production environments

### 3. Rule-Based Recommender

**How It Works**:
1. Classify job based on input size and type
2. Apply predefined rules and heuristics
3. Adjust for job-specific characteristics
4. Combine multiple rule results
5. Return conservative recommendations

**Rule Categories**:

**Size-Based Rules**:
```python
if input_size_gb <= 10:
    category = "small"
    executors = 5
    executor_cores = 2
    executor_memory_mb = 4096

elif input_size_gb <= 100:
    category = "medium"
    executors = 10
    executor_cores = 4
    executor_memory_mb = 8192

elif input_size_gb <= 1000:
    category = "large"
    executors = 20
    executor_cores = 4
    executor_memory_mb = 16384

else:
    category = "xlarge"
    executors = 50
    executor_cores = 4
    executor_memory_mb = 16384
```

**Job-Type Rules**:
```python
type_adjustments = {
    "etl": {
        "memory_multiplier": 1.0,
        "executor_multiplier": 1.0
    },
    "ml": {
        "memory_multiplier": 1.5,  # More memory for ML
        "executor_multiplier": 0.8  # Fewer, larger executors
    },
    "sql": {
        "memory_multiplier": 1.2,
        "executor_multiplier": 1.0
    },
    "streaming": {
        "memory_multiplier": 1.3,
        "executor_multiplier": 1.2  # More executors for parallelism
    }
}
```

**Anti-Pattern Rules**:
- Avoid single large executor (poor parallelism)
- Prevent memory-to-core imbalance
- Ensure minimum parallelism
- Set reasonable driver memory

**Advantages**:
- No training required
- Fast recommendations
- Interpretable rules
- Captures best practices

**Limitations**:
- Fixed heuristics
- May be suboptimal
- Requires domain expertise
- Less adaptable

**Best For**:
- Quick estimates
- Fallback recommendations
- Unknown job types
- Development environments

**Example**:
```python
from spark_optimizer.recommender.rule_based_recommender import RuleBasedRecommender

recommender = RuleBasedRecommender()

recommendation = recommender.recommend({
    "input_size_gb": 200.0,
    "job_type": "ml"
})

print(f"Rules Applied: {recommendation['metadata']['rules_applied']}")
```

## Hybrid Approach

Combine multiple recommenders for robust predictions:

```python
def hybrid_recommend(job_requirements, historical_jobs):
    # Get recommendations from all methods
    similarity_rec = SimilarityRecommender()
    similarity_rec.train(historical_jobs)
    sim_result = similarity_rec.recommend(job_requirements)

    ml_rec = MLRecommender()
    if ml_rec.is_trained:
        ml_result = ml_rec.recommend(job_requirements)
    else:
        ml_result = None

    rule_result = RuleBasedRecommender().recommend(job_requirements)

    # Weight by confidence
    weights = []
    configs = []

    if sim_result['confidence'] > 0.7:
        weights.append(sim_result['confidence'])
        configs.append(sim_result['configuration'])

    if ml_result and ml_result['confidence'] > 0.8:
        weights.append(ml_result['confidence'])
        configs.append(ml_result['configuration'])

    weights.append(0.5)  # Lower weight for rules
    configs.append(rule_result['configuration'])

    # Weighted average
    final_config = weighted_average(configs, weights)
    return final_config
```

## Feature Engineering

### Extracted Features

```python
class FeatureExtractor:
    def extract_features(self, job_data):
        features = {}

        # Size features
        features['input_size_gb'] = job_data['input_bytes'] / (1024**3)
        features['log_input_size'] = np.log1p(features['input_size_gb'])
        features['output_size_gb'] = job_data['output_bytes'] / (1024**3)
        features['shuffle_size_gb'] = job_data['shuffle_read_bytes'] / (1024**3)

        # Resource features
        features['total_cores'] = job_data['num_executors'] * job_data['executor_cores']
        features['total_memory_gb'] = (
            job_data['num_executors'] * job_data['executor_memory_mb'] / 1024
        )

        # Complexity features
        features['num_stages'] = job_data['total_stages']
        features['num_tasks'] = job_data['total_tasks']
        features['avg_tasks_per_stage'] = features['num_tasks'] / max(features['num_stages'], 1)

        # I/O features
        features['io_ratio'] = features['output_size_gb'] / max(features['input_size_gb'], 0.001)
        features['shuffle_to_input_ratio'] = features['shuffle_size_gb'] / max(features['input_size_gb'], 0.001)

        # Performance features
        features['duration_minutes'] = job_data['duration_ms'] / 60000
        features['throughput_gb_per_min'] = features['input_size_gb'] / max(features['duration_minutes'], 0.001)

        return features
```

### Feature Normalization

```python
from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
normalized_features = scaler.fit_transform(feature_matrix)
```

## Recommendation Response Format

```python
{
    "configuration": {
        "executor_cores": 4,
        "executor_memory_mb": 8192,
        "num_executors": 10,
        "driver_memory_mb": 4096
    },
    "predicted_metrics": {
        "duration_minutes": 25,
        "cost_usd": 12.5
    },
    "confidence": 0.85,
    "method": "similarity",
    "metadata": {
        "similar_jobs": [
            {"app_id": "app-001", "similarity": 0.92},
            {"app_id": "app-045", "similarity": 0.89}
        ],
        "feature_importance": {
            "input_size": 0.35,
            "num_stages": 0.22
        }
    }
}
```

## Confidence Scoring

Confidence scores indicate recommendation reliability:

```python
def calculate_confidence(recommendation_context):
    confidence = 1.0

    # Reduce if limited historical data
    if len(historical_jobs) < 10:
        confidence *= 0.6

    # Reduce if low similarity
    if max_similarity < 0.7:
        confidence *= 0.7

    # Reduce if extrapolating beyond training range
    if input_size > max_training_size:
        confidence *= 0.8

    # Reduce if prediction variance is high
    if prediction_std > threshold:
        confidence *= 0.9

    return confidence
```

Confidence Levels:
- **> 0.9**: Very High - Direct match with historical jobs
- **0.8 - 0.9**: High - Strong similarity or ML prediction
- **0.7 - 0.8**: Medium - Moderate similarity or rules
- **0.5 - 0.7**: Low - Fallback or limited data
- **< 0.5**: Very Low - Uncertain recommendation

## Cost Optimization

Factor in cost considerations:

```python
def cost_optimized_recommendation(job_requirements, max_cost):
    # Get baseline recommendation
    baseline = recommender.recommend(job_requirements)

    # Estimate cost
    estimated_cost = calculate_cost(baseline['configuration'])

    if estimated_cost <= max_cost:
        return baseline

    # Reduce resources to meet budget
    while estimated_cost > max_cost:
        # Try reducing executors first
        baseline['configuration']['num_executors'] -= 1

        # Recalculate cost
        estimated_cost = calculate_cost(baseline['configuration'])

        # Ensure minimum resources
        if baseline['configuration']['num_executors'] < 2:
            break

    return baseline
```

## Feedback Loop

Improve recommendations with user feedback:

```python
def record_feedback(recommendation_id, actual_performance, satisfaction):
    # Store feedback
    feedback = {
        "recommendation_id": recommendation_id,
        "actual_duration_ms": actual_performance['duration_ms'],
        "actual_cost": actual_performance['cost'],
        "satisfaction_score": satisfaction,  # 0.0 to 1.0
        "timestamp": datetime.now()
    }

    # Update repository
    repo.add_feedback(feedback)

    # Retrain models periodically
    if should_retrain():
        ml_recommender.train(get_all_jobs_with_feedback())
```

## Performance Tuning

### Executor Count

```python
# Rule of thumb: 2-3x input data size in GB
optimal_executors = max(5, int(input_size_gb / 10))

# Adjust for cluster size
optimal_executors = min(optimal_executors, max_cluster_executors)
```

### Executor Memory

```python
# Rule of thumb: 5-10x input size per executor
memory_per_executor_gb = (input_size_gb * 7) / num_executors

# Add overhead (10%)
memory_with_overhead = memory_per_executor_gb * 1.1

# Convert to MB and round
executor_memory_mb = int(memory_with_overhead * 1024)
```

### Executor Cores

```python
# Rule of thumb: 4-5 cores per executor
# More cores can lead to I/O bottlenecks
recommended_cores = 4

# Adjust for job type
if job_type == "cpu_intensive":
    recommended_cores = 6
elif job_type == "io_intensive":
    recommended_cores = 2
```

## Validation

Validate recommendations before returning:

```python
def validate_recommendation(config):
    errors = []

    # Check minimum resources
    if config['num_executors'] < 1:
        errors.append("Must have at least 1 executor")

    if config['executor_memory_mb'] < 1024:
        errors.append("Executor memory must be at least 1GB")

    # Check ratios
    memory_per_core = config['executor_memory_mb'] / config['executor_cores']
    if memory_per_core < 1024:
        errors.append("Memory per core should be at least 1GB")

    # Check cluster limits
    total_cores = config['num_executors'] * config['executor_cores']
    if total_cores > max_cluster_cores:
        errors.append(f"Total cores ({total_cores}) exceeds cluster capacity")

    return len(errors) == 0, errors
```

## Best Practices

1. **Start Conservative**: Overestimate resources initially
2. **Iterate**: Use feedback to refine recommendations
3. **Consider Cost**: Balance performance and cost
4. **Monitor**: Track recommendation accuracy
5. **Combine Methods**: Use hybrid approach for robustness
6. **Update Models**: Retrain periodically with new data
7. **Validate**: Check recommendations against constraints
8. **Document**: Record reasoning for recommendations

## CLI Usage

```bash
# Get recommendation
spark-optimizer recommend --input-size 50 --job-type etl

# Specify recommender method
spark-optimizer recommend --input-size 50 --method similarity

# Cost-constrained recommendation
spark-optimizer recommend --input-size 100 --max-cost 50

# With detailed output
spark-optimizer recommend --input-size 50 --verbose
```
