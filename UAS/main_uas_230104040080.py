# ==========================================================
# UAS Teknologi Big Data - NIM Genap
# Nama Project  : uas-tbg-230104040080
# NIM           : 230104040080
# Studi Kasus   : Smart Hospital Monitoring System
# File          : main_uas_230104040080.py
# Pipeline      : Patient Sensor Data -> Spark Processing
#                 -> Parquet Storage -> AI Dataset
# ==========================================================

import os
import shutil
import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    sum as spark_sum,
    avg as spark_avg,
    max as spark_max,
    min as spark_min,
    hour,
    round as spark_round
)


# ----------------------------------------------------------
# 1. ABSOLUTE PATH CONFIG
# ----------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

PATIENT_TOTAL_DIR = os.path.join(OUTPUT_DIR, "patient_total")
PATIENT_TIME_DIR = os.path.join(OUTPUT_DIR, "patient_time")
ML_DATA_DIR = os.path.join(OUTPUT_DIR, "ml_data")


# ----------------------------------------------------------
# 2. INIT SPARK
# ----------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("UAS_Smart_Hospital_230104040080")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("🚀 Spark berhasil dijalankan")
print(f"📁 BASE_DIR   : {BASE_DIR}")
print(f"📁 OUTPUT_DIR : {OUTPUT_DIR}")


# ----------------------------------------------------------
# 3. PREPARE OUTPUT FOLDER
# ----------------------------------------------------------
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)

os.makedirs(OUTPUT_DIR, exist_ok=True)
print("🧹 Folder output lama dibersihkan")


# ----------------------------------------------------------
# 4. GENERATE DUMMY PATIENT SENSOR DATA
#    Requirement:
#    - timestamp
#    - room
#    - patient_count
#    - minimal 3 room: ICU, Emergency, Pharmacy
#    - data selama 120 menit
#    - patient_count random 5-80
# ----------------------------------------------------------
random.seed(80)  # agar hasil konsisten untuk NIM akhir 0

rooms = ["ICU", "Emergency", "Pharmacy"]
start_time = datetime(2026, 6, 11, 7, 0, 0)

sensor_data = []

for minute in range(120):
    current_time = start_time + timedelta(minutes=minute)

    for room in rooms:
        # Pola dibuat sedikit realistis, tetapi tetap berada di rentang 5-80
        if room == "Emergency":
            base = random.randint(25, 70)
            # Emergency dibuat cenderung lebih padat pada menit ke-45 s.d. 90
            if 45 <= minute <= 90:
                base += random.randint(5, 10)
        elif room == "ICU":
            base = random.randint(10, 45)
        else:  # Pharmacy
            base = random.randint(15, 60)
            # Pharmacy cenderung naik setelah pasien dari layanan lain
            if 60 <= minute <= 110:
                base += random.randint(3, 8)

        patient_count = max(5, min(base, 80))

        sensor_data.append((current_time, room, patient_count))

patient_df = spark.createDataFrame(
    sensor_data,
    ["timestamp", "room", "patient_count"]
)

print("✅ Dummy data berhasil dibuat")
print(f"📌 Total record: {patient_df.count()} baris")


# ----------------------------------------------------------
# 5. SPARK TRANSFORMATION
# ----------------------------------------------------------

# 5.1 Total pasien per ruangan
patient_total_df = (
    patient_df
    .groupBy("room")
    .agg(
        spark_sum("patient_count").alias("total_patient"),
        spark_round(spark_avg("patient_count"), 2).alias("avg_patient"),
        spark_max("patient_count").alias("max_patient"),
        spark_min("patient_count").alias("min_patient")
    )
    .orderBy("room")
)

# 5.2 Tren pasien per 15 menit
patient_time_df = (
    patient_df
    .groupBy(
        window(col("timestamp"), "15 minutes"),
        col("room")
    )
    .agg(
        spark_sum("patient_count").alias("total_patient"),
        spark_round(spark_avg("patient_count"), 2).alias("avg_patient")
    )
    .withColumn("time_window_start", col("window.start"))
    .withColumn("time_window_end", col("window.end"))
    .drop("window")
    .orderBy("time_window_start", "room")
)

# 5.3 Dataset AI berbasis jam
ml_data_df = (
    patient_df
    .withColumn("hour", hour(col("timestamp")))
    .select("timestamp", "room", "hour", "patient_count")
    .orderBy("timestamp", "room")
)

print("✅ Spark transformation berhasil")


# ----------------------------------------------------------
# 6. SAVE TO PARQUET
#    Requirement:
#    - output/patient_total
#    - output/patient_time
#    - output/ml_data
#    - mode overwrite
#    - absolute path
# ----------------------------------------------------------
patient_total_df.write.mode("overwrite").parquet(PATIENT_TOTAL_DIR)
patient_time_df.write.mode("overwrite").parquet(PATIENT_TIME_DIR)
ml_data_df.write.mode("overwrite").parquet(ML_DATA_DIR)

print("✅ SEMUA DATA PASIEN BERHASIL DISIMPAN KE FORMAT PARQUET")
print(f"   1. {PATIENT_TOTAL_DIR}")
print(f"   2. {PATIENT_TIME_DIR}")
print(f"   3. {ML_DATA_DIR}")


# ----------------------------------------------------------
# 7. SHOW SAMPLE RESULT FOR TERMINAL SCREENSHOT
# ----------------------------------------------------------
print("\n===== TOTAL PASIEN PER RUANGAN =====")
patient_total_df.show(truncate=False)

print("\n===== TREN PASIEN PER 15 MENIT =====")
patient_time_df.show(10, truncate=False)

print("\n===== DATASET AI =====")
ml_data_df.show(10, truncate=False)

# Analisis jam tertinggi berdasarkan total patient_count
peak_hour_df = (
    ml_data_df
    .groupBy("hour")
    .agg(spark_sum("patient_count").alias("total_patient"))
    .orderBy(col("total_patient").desc())
)

print("\n===== ANALISIS JAM PASIEN TERTINGGI =====")
peak_hour_df.show(truncate=False)


# ----------------------------------------------------------
# 8. STOP SPARK
# ----------------------------------------------------------
spark.stop()
print("🛑 Spark session ditutup")
