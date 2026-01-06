# Databricks notebook source
from pyspark.sql import functions as F

orders = spark.table("capstone_silver_orders")
products = spark.table("capstone_silver_products")
regions = spark.table("capstone_silver_regions")

gold_orders = orders.select(
    "order_id",
    "order_date",
    "product_id",
    "region_id",
    "quantity",
    F.round(F.col("unit_price"), 2).alias("unit_price"),
    F.round(F.col("cost"), 2).alias("cost")
).withColumn(
    "revenue", F.round(F.col("quantity") * F.col("unit_price"), 2)
).withColumn(
    "total_cost", F.round(F.col("quantity") * F.col("cost"), 2)
).withColumn(
    "profit", F.round(F.col("revenue") - F.col("total_cost"), 2)
)

products_renamed = products.select(
    "product_id",
    F.col("category").alias("product_category")
)

regions_renamed = regions.select(
    "region_id",
    F.col("region_name"),
    F.col("country").alias("region_country")
)

gold_sales_fact = gold_orders.join(
    products_renamed, on="product_id", how="left"
).join(
    regions_renamed, on="region_id", how="left"
)

gold_sales_fact.write.mode("overwrite").format("delta").saveAsTable(
    "capstone_gold_sales_fact"
)

display(gold_sales_fact.limit(10))