#!/usr/bin/env python3

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType



POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_DB = os.environ.get("POSTGRES_DB")
POSTGRES_HOST = "postgres"

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "git_logs"

spark = SparkSession.builder \
    .appName("GitHubEventsPipeline") \
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            "org.apache.kafka:kafka-clients:3.5.1",
            "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.0",
            "org.apache.commons:commons-pool2:2.11.1",
            "org.postgresql:postgresql:42.6.0"
        ])
    ) \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("id", StringType()),
    StructField("type", StringType()),
    StructField("repo", StructType([
        StructField("name", StringType())
    ])),
    StructField("actor", StructType([
        StructField("login", StringType())
    ])),
    StructField("created_at", StringType())
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).withColumn("created_at", to_timestamp("data.created_at")).select(
    col("data.id").alias("id"),
    col("data.type").alias("type"),
    col("data.repo.name").alias("repo_name"),
    col("data.actor.login").alias("actor_login"),
    col("created_at")
)

def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}") \
        .option("dbtable", "github_events") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query = parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
