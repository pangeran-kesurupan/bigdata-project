import pandas as pd

def compute_kpis(df):
    if df.empty:
        return {}

    total_trips = len(df)
    total_fare = df['fare'].sum()

    top_location = df['location'].value_counts().idxmax()

    df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
    peak_hour = df['hour'].value_counts().idxmax()

    return {
        "total_trips": total_trips,
        "total_fare": total_fare,
        "top_location": top_location,
        "peak_hour": peak_hour
    }


def fare_per_location(df):
    return df.groupby('location')['fare'].sum().reset_index()


def vehicle_distribution(df):
    if "vehicle_type" not in df.columns:
        return pd.DataFrame()  # biar gak crash
    
    return df["vehicle_type"].value_counts().reset_index().rename(
        columns={"index": "vehicle_type", "vehicle_type": "count"}
    )


def mobility_trend(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df.set_index('timestamp').resample('1Min').size().reset_index(name='trip_count')


def detect_abnormal_trips(df):
    return df[(df['distance'] > 50) | (df['fare'] > 500000)]