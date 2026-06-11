# ==========================================================
# UAS Teknologi Big Data - NIM Genap
# Nama Project  : uas-tbg-230104040080
# NIM           : 230104040080
# Studi Kasus   : Smart Hospital Monitoring System
# File          : dashboard_230104040080.py
# Pipeline      : Parquet Storage -> AI Prediction
#                 -> Streamlit Dashboard
# ==========================================================

import os

import pandas as pd
import plotly.express as px
import streamlit as st

from pyspark.sql import SparkSession
from sklearn.linear_model import LinearRegression


# ----------------------------------------------------------
# 1. PAGE CONFIG
# ----------------------------------------------------------
st.set_page_config(
    page_title="Smart Hospital Monitoring System",
    page_icon="🏥",
    layout="wide"
)

st.title("🏥 Smart Hospital Monitoring System")
st.caption("UAS Teknologi Big Data | PySpark + Parquet + Linear Regression + Streamlit")


# ----------------------------------------------------------
# 2. ABSOLUTE PATH CONFIG
# ----------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

PATIENT_TOTAL_DIR = os.path.join(OUTPUT_DIR, "patient_total")
PATIENT_TIME_DIR = os.path.join(OUTPUT_DIR, "patient_time")
ML_DATA_DIR = os.path.join(OUTPUT_DIR, "ml_data")


# ----------------------------------------------------------
# 3. INIT SPARK
# ----------------------------------------------------------
@st.cache_resource
def get_spark():
    spark_session = (
        SparkSession.builder
        .appName("Dashboard_Smart_Hospital_230104040080")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("ERROR")
    return spark_session


spark = get_spark()


# ----------------------------------------------------------
# 4. LOAD PARQUET DATA
# ----------------------------------------------------------
@st.cache_data
def load_parquet(parquet_path: str) -> pd.DataFrame:
    if not os.path.exists(parquet_path):
        st.error(
            f"Folder data tidak ditemukan:\n\n{parquet_path}\n\n"
            "Jalankan dulu: python3 main_uas_230104040080.py"
        )
        st.stop()

    return spark.read.parquet(parquet_path).toPandas()


patient_total = load_parquet(PATIENT_TOTAL_DIR)
patient_time = load_parquet(PATIENT_TIME_DIR)
ml_data = load_parquet(ML_DATA_DIR)

# Pastikan format datetime terbaca baik oleh Plotly
patient_time["time_window_start"] = pd.to_datetime(patient_time["time_window_start"])
patient_time["time_window_end"] = pd.to_datetime(patient_time["time_window_end"])
ml_data["timestamp"] = pd.to_datetime(ml_data["timestamp"])


# ----------------------------------------------------------
# 5. SIDEBAR FILTER
# ----------------------------------------------------------
st.sidebar.header("🔎 Filter Ruangan")
rooms = sorted(ml_data["room"].unique().tolist())
selected_room = st.sidebar.selectbox("Pilih ruangan layanan:", rooms)

filtered_time = patient_time[patient_time["room"] == selected_room].copy()
filtered_ml = ml_data[ml_data["room"] == selected_room].copy()
filtered_total = patient_total[patient_total["room"] == selected_room].copy()

st.sidebar.markdown("---")
st.sidebar.write("**Absolute Path Output:**")
st.sidebar.code(OUTPUT_DIR, language="bash")


# ----------------------------------------------------------
# 6. KPI METRICS
# ----------------------------------------------------------
total_all = int(patient_total["total_patient"].sum())
total_selected = int(filtered_total["total_patient"].iloc[0])
avg_selected = float(filtered_total["avg_patient"].iloc[0])
max_selected = int(filtered_total["max_patient"].iloc[0])

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Pasien Semua Ruangan", f"{total_all:,}")

with col2:
    st.metric(f"Total Pasien {selected_room}", f"{total_selected:,}")

with col3:
    st.metric(f"Rata-Rata Pasien {selected_room}", f"{avg_selected:.2f}")

with col4:
    st.metric(f"Maksimum Pasien {selected_room}", f"{max_selected:,}")


# ----------------------------------------------------------
# 7. VISUALIZATION: TREND LINE PLOTLY
# ----------------------------------------------------------
st.subheader("📈 Grafik Tren Pasien per 15 Menit")

fig_trend = px.line(
    filtered_time,
    x="time_window_start",
    y="total_patient",
    markers=True,
    title=f"Tren Total Pasien per 15 Menit - {selected_room}",
    labels={
        "time_window_start": "Waktu",
        "total_patient": "Total Pasien",
        "room": "Ruangan"
    }
)

st.plotly_chart(fig_trend, use_container_width=True)


# ----------------------------------------------------------
# 8. MACHINE LEARNING: LINEAR REGRESSION
# ----------------------------------------------------------
st.subheader("🤖 Prediksi Jumlah Pasien Berbasis Linear Regression")

X = filtered_ml[["hour"]]
y = filtered_ml["patient_count"]

model = LinearRegression()
model.fit(X, y)

min_hour = int(ml_data["hour"].min())
max_hour = int(ml_data["hour"].max())

hour_input = st.slider(
    "Pilih jam untuk prediksi jumlah pasien:",
    min_value=0,
    max_value=23,
    value=min_hour,
    step=1
)

prediction = model.predict(pd.DataFrame({"hour": [hour_input]}))[0]
prediction = max(0, prediction)

col_pred1, col_pred2 = st.columns(2)

with col_pred1:
    st.metric(
        label=f"Prediksi Pasien {selected_room} pada Jam {hour_input}:00",
        value=f"{int(round(prediction))} pasien"
    )

with col_pred2:
    r2_score = model.score(X, y)
    st.metric(
        label="R² Model Linear Regression",
        value=f"{r2_score:.3f}"
    )

# Visualisasi tren prediksi untuk jam 0-23
prediction_df = pd.DataFrame({"hour": list(range(24))})
prediction_df["predicted_patient"] = model.predict(prediction_df[["hour"]])
prediction_df["predicted_patient"] = prediction_df["predicted_patient"].clip(lower=0)

fig_pred = px.line(
    prediction_df,
    x="hour",
    y="predicted_patient",
    markers=True,
    title=f"Visualisasi Prediksi Jumlah Pasien per Jam - {selected_room}",
    labels={
        "hour": "Jam",
        "predicted_patient": "Prediksi Jumlah Pasien"
    }
)

st.plotly_chart(fig_pred, use_container_width=True)


# ----------------------------------------------------------
# 9. ANALISIS JAM PASIEN TERTINGGI
# ----------------------------------------------------------
st.subheader("📝 Analisis Jam Pasien Tertinggi")

hourly_room = (
    filtered_ml
    .groupby("hour", as_index=False)["patient_count"]
    .sum()
    .rename(columns={"patient_count": "total_patient"})
    .sort_values("total_patient", ascending=False)
)

peak_hour = int(hourly_room.iloc[0]["hour"])
peak_total = int(hourly_room.iloc[0]["total_patient"])

st.success(
    f"Berdasarkan data simulasi pada ruangan **{selected_room}**, "
    f"jam dengan jumlah pasien tertinggi adalah **jam {peak_hour}:00** "
    f"dengan total **{peak_total} pasien**."
)

fig_hourly = px.bar(
    hourly_room.sort_values("hour"),
    x="hour",
    y="total_patient",
    title=f"Total Pasien Berdasarkan Jam - {selected_room}",
    labels={
        "hour": "Jam",
        "total_patient": "Total Pasien"
    }
)

st.plotly_chart(fig_hourly, use_container_width=True)


# ----------------------------------------------------------
# 10. DATA TABLE
# ----------------------------------------------------------
with st.expander("Lihat Data Total Pasien per Ruangan"):
    st.dataframe(patient_total, use_container_width=True)

with st.expander("Lihat Data Tren 15 Menit"):
    st.dataframe(filtered_time, use_container_width=True)

with st.expander("Lihat Dataset AI"):
    st.dataframe(filtered_ml, use_container_width=True)
