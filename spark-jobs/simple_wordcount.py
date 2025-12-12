#!/usr/bin/env python3
"""
Simple Word Count Spark Job
A basic Spark application that counts words in generated text data.
"""
from pyspark.sql import SparkSession
import sys


def main():
    # Create Spark session with event logging enabled
    spark = SparkSession.builder \
        .appName("Simple WordCount Job") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "file:///spark-events") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .getOrCreate()

    try:
        # Generate some sample text data
        text_data = [
            "Apache Spark is a unified analytics engine",
            "Spark provides high-level APIs in Java, Scala, Python and R",
            "Spark is used for big data processing and machine learning",
            "Apache Spark can process data in batch and streaming modes",
            "Spark SQL is used for structured data processing"
        ] * 1000  # Repeat to create more data

        # Create RDD and perform word count
        rdd = spark.sparkContext.parallelize(text_data, numSlices=4)

        words = rdd.flatMap(lambda line: line.split())
        word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

        # Collect results
        results = word_counts.take(10)

        print("\n" + "="*50)
        print("Top 10 Words:")
        print("="*50)
        for word, count in results:
            print(f"{word}: {count}")
        print("="*50 + "\n")

        total_words = word_counts.count()
        print(f"Total unique words: {total_words}")

        spark.stop()
        print("Job completed successfully!")
        sys.exit(0)

    except Exception as e:
        print(f"Job failed with error: {e}")
        spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
