# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

customers = spark.table("capstone_bronze_customers")
products = spark.table("capstone_bronze_products")
regions = spark.table("capstone_bronze_regions")
orders = spark.table("capstone_bronze_orders")

cust_w = Window.partitionBy("customer_id").orderBy(F.col("ingestion_ts").desc())
prod_w = Window.partitionBy("product_id").orderBy(F.col("ingestion_ts").desc())
reg_w  = Window.partitionBy("region_id").orderBy(F.col("ingestion_ts").desc())
ord_w  = Window.partitionBy("order_id").orderBy(F.col("ingestion_ts").desc())

silver_customers = (
    customers
        .withColumn("rn", F.row_number().over(cust_w))
        .filter("rn = 1")
        .drop("rn", "ingestion_ts")
)

silver_products = (
    products
        .withColumn("rn", F.row_number().over(prod_w))
        .filter("rn = 1")
        .drop("rn", "ingestion_ts")
)

silver_regions = (
    regions
        .withColumn("rn", F.row_number().over(reg_w))
        .filter("rn = 1")
        .drop("rn", "ingestion_ts")
)

silver_orders = (
    orders
        .dropna(subset=["order_id", "product_id", "quantity"])
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("cost", F.col("cost").cast("double"))
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") >= 0)
        .withColumn("order_date", F.to_timestamp("order_date"))
        .withColumn("rn", F.row_number().over(ord_w))
        .filter("rn = 1")
        .drop("rn", "ingestion_ts")
)

silver_customers.write.mode("overwrite").format("delta").saveAsTable("capstone_silver_customers")
silver_products.write.mode("overwrite").format("delta").saveAsTable("capstone_silver_products")
silver_regions.write.mode("overwrite").format("delta").saveAsTable("capstone_silver_regions")
silver_orders.write.mode("overwrite").format("delta").saveAsTable("capstone_silver_orders")