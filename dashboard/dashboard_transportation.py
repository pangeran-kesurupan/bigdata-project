import streamlit as st
import pandas as pd
import glob
import sys
import os

# 🔥 FIX IMPORT MODULE
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from analytics.transportation_analytics import *
from alerts.transportation_alert import *

st.set_page_config(layout="wide")
st.title("🚗 Smart Transportation Dashboard")

# ================= LOAD DATA =================
files = glob.glob("data/serving/transportation/*.parquet")

if not files:
    st.warning("No data available yet...")
    st.stop()

try:
    df = pd.concat([pd.read_parquet(f) for f in files])
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# ================= DEBUG (AUTO DETECT) =================
st.sidebar.subheader("🧠 Debug Info")
st.sidebar.write("Columns:", list(df.columns))

# ================= KPI =================
try:
    kpis = compute_kpis(df)
except:
    kpis = {}

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Trips", kpis.get("total_trips", 0))
col2.metric("Total Fare", kpis.get("total_fare", 0))
col3.metric("Top Location", kpis.get("top_location", "-"))
col4.metric("Peak Hour", kpis.get("peak_hour", "-"))

# ================= ALERT =================
st.subheader("🚨 Alerts")
try:
    alerts = generate_alerts(df)
except:
    alerts = []

if alerts:
    for alert in alerts:
        st.error(alert)
else:
    st.success("No alerts")

# ================= CHART =================
st.subheader("📊 Fare per Location")
try:
    fare_loc = fare_per_location(df)
    if not fare_loc.empty and "location" in fare_loc.columns:
        st.bar_chart(fare_loc.set_index("location"))
    else:
        st.warning("Location data not available")
except:
    st.warning("Error in fare per location")

# ================= VEHICLE DISTRIBUTION (FIX UTAMA) =================
st.subheader("🚘 Vehicle Distribution")

try:
    veh_dist = vehicle_distribution(df)

    # 🔥 AUTO DETECT COLUMN
    if not veh_dist.empty:
        if "vehicle_type" in veh_dist.columns:
            st.bar_chart(veh_dist.set_index("vehicle_type"))
        elif "vehicle" in veh_dist.columns:
            st.bar_chart(veh_dist.set_index("vehicle"))
        else:
            st.warning("Vehicle column not found in processed data")
    else:
        st.warning("No vehicle distribution data")
except Exception as e:
    st.warning(f"Error in vehicle distribution: {e}")

# ================= MOBILITY =================
st.subheader("📈 Mobility Trend")

try:
    trend = mobility_trend(df)
    if not trend.empty and "timestamp" in trend.columns:
        st.line_chart(trend.set_index("timestamp"))
    else:
        st.warning("Timestamp data not available")
except:
    st.warning("Error in mobility trend")

# ================= ABNORMAL =================
st.subheader("⚠️ Abnormal Trips")

try:
    abnormal = detect_abnormal_trips(df)
    if not abnormal.empty:
        st.dataframe(abnormal)
    else:
        st.info("No abnormal trips detected")
except:
    st.warning("Error detecting abnormal trips")

# ================= LIVE DATA =================
st.subheader("📡 Live Trip Data")
st.dataframe(df.tail(20))