#!/usr/bin/env python3
"""
ETL Data Processing Spark Job
Simulates a typical ETL workflow with transformations and aggregations.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as _sum, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import sys
import random
from datetime import datetime, timedelta


def generate_sample_data(spark, num_records=10000):
    """Generate sample transaction data."""

    # Define schema
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("region", StringType(), True),
        StructField("date", StringType(), True)
    ])

    categories = ["Electronics", "Clothing", "Food", "Books", "Toys"]
    regions = ["North", "South", "East", "West", "Central"]

    # Generate data
    data = []
    base_date = datetime.now() - timedelta(days=30)

    for i in range(num_records):
        date = base_date + timedelta(days=random.randint(0, 30))
        data.append((
            f"TXN-{i:06d}",
            f"CUST-{random.randint(1, 1000):04d}",
            random.choice(categories),
            round(random.uniform(10.0, 1000.0), 2),
            random.randint(1, 10),
            random.choice(regions),
            date.strftime("%Y-%m-%d")
        ))

    return spark.createDataFrame(data, schema)


def main():
    # Create Spark session with more resources
    spark = SparkSession.builder \
        .appName("ETL Data Processing Job") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "file:///spark-events") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    try:
        print("Starting ETL Data Processing Job...")

        # Generate sample data
        print("Generating sample transaction data...")
        df = generate_sample_data(spark, num_records=50000)

        # Cache the dataframe as we'll use it multiple times
        df.cache()

        print(f"Generated {df.count()} transactions")

        # Transformation 1: Add revenue column
        df = df.withColumn("revenue", col("amount") * col("quantity"))

        # Transformation 2: Categorize transactions
        df = df.withColumn(
            "transaction_size",
            when(col("revenue") < 100, "Small")
            .when((col("revenue") >= 100) & (col("revenue") < 500), "Medium")
            .otherwise("Large")
        )

        # Analysis 1: Revenue by region
        print("\nRevenue by Region:")
        region_revenue = df.groupBy("region") \
            .agg(
                _sum("revenue").alias("total_revenue"),
                count("*").alias("transaction_count"),
                avg("revenue").alias("avg_revenue")
            ) \
            .orderBy(col("total_revenue").desc())

        region_revenue.show()

        # Analysis 2: Top product categories
        print("\nTop Product Categories:")
        category_stats = df.groupBy("product_category") \
            .agg(
                _sum("revenue").alias("total_revenue"),
                count("*").alias("transaction_count"),
                avg("amount").alias("avg_amount")
            ) \
            .orderBy(col("total_revenue").desc())

        category_stats.show()

        # Analysis 3: Transaction size distribution
        print("\nTransaction Size Distribution:")
        size_dist = df.groupBy("transaction_size") \
            .agg(
                count("*").alias("count"),
                _sum("revenue").alias("total_revenue")
            ) \
            .orderBy("transaction_size")

        size_dist.show()

        # Analysis 4: Top customers by revenue
        print("\nTop 10 Customers by Revenue:")
        top_customers = df.groupBy("customer_id") \
            .agg(
                _sum("revenue").alias("total_spent"),
                count("*").alias("num_transactions")
            ) \
            .orderBy(col("total_spent").desc()) \
            .limit(10)

        top_customers.show()

        # Write summary results
        print("\nJob Summary:")
        print(f"Total Transactions: {df.count()}")
        print(f"Total Revenue: ${df.agg(_sum('revenue')).collect()[0][0]:,.2f}")
        print(f"Average Transaction: ${df.agg(avg('revenue')).collect()[0][0]:,.2f}")

        df.unpersist()
        spark.stop()

        print("\nJob completed successfully!")
        sys.exit(0)

    except Exception as e:
        print(f"Job failed with error: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
