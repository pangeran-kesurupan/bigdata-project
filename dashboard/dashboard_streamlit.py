import streamlit as st
import pandas as pd
import os
import time

st.set_page_config(
    page_title="Real-Time E-Commerce Dashboard",
    layout="wide"
)

st.title("Real-Time E-Commerce Analytics Dashboard")

DATA_PATH = "data/serving/stream"

placeholder = st.empty()

def load_stream_data():

    if not os.path.exists(DATA_PATH):
        return pd.DataFrame()

    files = [f for f in os.listdir(DATA_PATH) if f.endswith(".parquet")]

    if len(files) == 0:
        return pd.DataFrame()

    df = pd.concat(
        [pd.read_parquet(os.path.join(DATA_PATH, f)) for f in files],
        ignore_index=True
    )

    return df

while True:

    with placeholder.container():

        df = load_stream_data()

        if df.empty:
            st.info("Waiting for streaming data...")
            time.sleep(5)
            continue

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        st.subheader("Key Metrics")

        col1,col2,col3,col4 = st.columns(4)

        total_transactions = len(df)
        total_revenue = df["price"].sum()
        avg_transaction = df["price"].mean()
        unique_cities = df["city"].nunique()

        col1.metric("Total Transactions", total_transactions)
        col2.metric("Total Revenue", int(total_revenue))
        col3.metric("Avg Transaction", int(avg_transaction))
        col4.metric("Cities", unique_cities)

        st.divider()

        colA,colB = st.columns(2)

        with colA:
            st.subheader("Revenue by City")
            city_sales = df.groupby("city")["price"].sum().sort_values(ascending=False)
            st.bar_chart(city_sales)

        with colB:
            st.subheader("Top Products")
            product_sales = df.groupby("product")["price"].sum().sort_values(ascending=False)
            st.bar_chart(product_sales)

        st.divider()

        st.subheader("Revenue Trend")

        revenue_trend = (
            df.set_index("timestamp")
            .resample("10s")["price"]
            .sum()
        )

        st.line_chart(revenue_trend)

        st.divider()

        st.subheader("Live Transactions")

        st.dataframe(
            df.sort_values("timestamp",ascending=False).head(30),
            use_container_width=True
        )

    time.sleep(5)