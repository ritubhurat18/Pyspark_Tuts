# Databricks notebook source

# Pyspark Word Count Program 
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

def get_spark_session():
    return SparkSession.builder.appName("Pyspark Word Count").getOrCreate()


def clean_text(df):
    clean_df = df.\
        select(F.lower(F.regexp_replace(df.value,"[^A-Za-z0-9\\s\'$]",""))\
        .alias("cleaned_value"))
    return clean_df

def word_count(df):
    split_df = df.\
        select(F.split(F.col("cleaned_value")," ").alias("splitted_value")).\
        select(F.explode(F.col("splitted_value")).alias("word"))
    word_count = split_df.\
        groupBy(split_df.word).agg(F.count(split_df.word).alias("count"))
    return word_count

def main():

    spark = get_spark_session()
    txt = "Hello hello Reetu, Here's your First@ Word Count Program in Spark for count word!"
    df = spark.createDataFrame([txt], StringType())
    clean_df = clean_text(df)
    clean_df.show(truncate=False)
    word_count_df = word_count(clean_df)
    word_count_df.show(truncate=False)

if __name__ == "__main__":
    main()



