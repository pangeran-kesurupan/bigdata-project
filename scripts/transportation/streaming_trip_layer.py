from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("TransportationStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema data
schema = StructType([
    StructField("trip_id", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("location", StringType()),
    StructField("distance", DoubleType()),
    StructField("fare", DoubleType()),
    StructField("timestamp", StringType())
])

# Read streaming JSON
df = spark.readStream \
    .schema(schema) \
    .json("stream_data/transportation")

# Write ke parquet
query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data/serving/transportation") \
    .option("checkpointLocation", "data/checkpoints/transportation") \
    .start()

query.awaitTermination()