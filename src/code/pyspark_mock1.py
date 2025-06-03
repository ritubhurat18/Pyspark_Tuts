# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F

# Start SparkSession
spark = SparkSession.builder.appName("MockSession1").getOrCreate()

# Sample data (as JSON-like dictionaries)
json_data = [
    {"user_id": "u1", "email": "john.doe@gmail.com", "signup_ts": "2024-12-01T14:32:45Z", "referrer": "https://facebook.com/ad1"},
    {"user_id": "u2", "email": "invalid_email", "signup_ts": "2024-12-01T15:00:00Z", "referrer": "https://twitter.com/ad5"},
    {"user_id": "u3", "email": "alice@example.com", "signup_ts": "invalid-date", "referrer": "https://linkedin.com/profile123"},
    {"user_id": "u4", "email": None, "signup_ts": "2024-12-01T10:45:00Z", "referrer": "https://instagram.com/ad2"},
    {"user_id": "u5", "email": "raj.kumar@yahoo.com", "signup_ts": "2024-12-01T23:59:59Z", "referrer": ""},
    {"user_id": "u6", "email": "test.user@protonmail.com", "signup_ts": "2024-12-02T00:01:00Z", "referrer": "https://reddit.com/r/promotion"},
]

# Define schema
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("email", StringType(), True),
    StructField("signup_ts", StringType(), True),
    StructField("referrer", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(json_data, schema)
valid_mail_df = df\
    .filter(df.email.isNotNull())\
    .filter(df.email != "")\
    .filter(df.email.rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+\.[A-Za-z]{2,}$"))
valid_mail_df.show(truncate=False)
valid_df = valid_mail_df.withColumn("referrer_domain",F.regexp_extract("referrer", "https?://([^/]+)",1))

valid_df.show(truncate=False)


