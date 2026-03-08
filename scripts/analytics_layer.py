# =====================================
# ANALYTICS + SERVING LAYER
# =====================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, avg, desc
import os
import time

start_time = time.time()

print("========================================")
print("        ANALYTICS LAYER STARTED        ")
print("========================================")

spark = SparkSession.builder \
    .appName("AnalyticsLayer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

if not os.path.exists("data/serving"):
    os.makedirs("data/serving")

print("Loading Clean Parquet Data...")
df_clean = spark.read.parquet("data/clean/parquet/")

total_records = df_clean.count()
print(f"Total Records: {total_records}")

# ============================
# KPI 1 TOTAL REVENUE
# ============================

total_revenue = df_clean.agg(
    _sum("total_amount").alias("total_revenue")
)

total_revenue.show()

total_revenue.write.mode("overwrite") \
    .option("header", True) \
    .csv("data/serving/total_revenue")

# ============================
# KPI 2 TOP PRODUCT
# ============================

top_products = df_clean.groupBy("product") \
    .agg(_sum("quantity").alias("total_quantity")) \
    .orderBy(desc("total_quantity")) \
    .limit(10)

top_products.show()

top_products.write.mode("overwrite") \
    .option("header", True) \
    .csv("data/serving/top_products")

# ============================
# KPI 3 REVENUE CATEGORY
# ============================

category_revenue = df_clean.groupBy("category") \
    .agg(_sum("total_amount").alias("category_revenue")) \
    .orderBy(desc("category_revenue"))

category_revenue.show()

category_revenue.write.mode("overwrite") \
    .option("header", True) \
    .csv("data/serving/category_revenue")

# ============================
# KPI 4 AVG TRANSACTION
# ============================

avg_transaction = df_clean.groupBy("customer_id") \
    .agg(avg("total_amount").alias("avg_transaction_value"))

avg_transaction.show()

avg_transaction.write.mode("overwrite") \
    .option("header", True) \
    .csv("data/serving/avg_transaction")

spark.stop()

end_time = time.time()

print("Execution time:", round(end_time-start_time,2),"seconds")