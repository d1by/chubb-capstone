# Databricks notebook source
from pyspark.sql import functions as F

base = "/Volumes/workspace/default/chubb/capstone"

bronze_customers = (
    spark.read.option("header", True)
        .csv(f"{base}/customers.csv")
        .withColumn("ingestion_ts", F.current_timestamp())
)

bronze_products = (
    spark.read.option("header", True)
        .csv(f"{base}/products.csv")
        .withColumn("ingestion_ts", F.current_timestamp())
)

bronze_orders = (
    spark.read.option("header", True)
        .csv(f"{base}/orders.csv")
        .withColumn("ingestion_ts", F.current_timestamp())
)

bronze_regions = (
    spark.read.option("header", True)
        .csv(f"{base}/regions.csv")
        .withColumn("ingestion_ts", F.current_timestamp())
)

bronze_customers.write.mode("overwrite").format("delta").saveAsTable("capstone_bronze_customers")
bronze_products.write.mode("overwrite").format("delta").saveAsTable("capstone_bronze_products")
bronze_orders.write.mode("overwrite").format("delta").saveAsTable("capstone_bronze_orders")
bronze_regions.write.mode("overwrite").format("delta").saveAsTable("capstone_bronze_regions")