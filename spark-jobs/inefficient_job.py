#!/usr/bin/env python3
"""
Inefficient Spark Job
A Spark job with intentionally poor configuration to demonstrate optimization opportunities.
This job has:
- Undersized executor memory (will cause spilling)
- Too many shuffle partitions
- Inefficient operations
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, array, lit, rand
import sys


def main():
    # Create Spark session with intentionally poor configuration
    spark = SparkSession.builder \
        .appName("Inefficient Job - Needs Optimization") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "file:///spark-events") \
        .config("spark.executor.memory", "512m") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .getOrCreate()

    try:
        print("Starting Inefficient Job (intentionally poor configuration)...")

        # Create a DataFrame with lots of data to cause memory pressure
        print("Generating large dataset...")

        # Start with a small dataset and explode it to create millions of rows
        df = spark.range(0, 1000)

        # Add multiple columns with random data
        for i in range(10):
            df = df.withColumn(f"col_{i}", rand() * 1000)

        # Explode to create more rows (this will cause shuffle and memory pressure)
        df = df.withColumn("multiplier", explode(array([lit(x) for x in range(100)])))

        print(f"Created dataset with {df.count():,} rows")

        # Perform expensive operations
        print("Performing expensive shuffle operations...")

        # Multiple shuffles that will cause spilling due to low memory
        df1 = df.groupBy("multiplier").count()
        df2 = df.groupBy((col("id") % 100).alias("partition")).agg({"col_0": "sum", "col_1": "avg"})
        df3 = df.groupBy((col("id") % 50).alias("group")).agg({"col_2": "max", "col_3": "min"})

        # Join operations that will cause more shuffling
        result = df1.join(df2, df1.multiplier == df2.partition, "inner")
        result = result.join(df3, result.multiplier == df3.group, "inner")

        # Force computation
        row_count = result.count()
        print(f"Result has {row_count:,} rows")

        # Show sample
        print("\nSample results:")
        result.show(5)

        spark.stop()
        print("\nJob completed (but inefficiently)!")
        print("This job would benefit from:")
        print("  - More executor memory (recommend 2-4GB)")
        print("  - Fewer shuffle partitions (recommend 8-16)")
        print("  - Better partitioning strategy")
        sys.exit(0)

    except Exception as e:
        print(f"Job failed with error: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
