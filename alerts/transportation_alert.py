def generate_alerts(df):
    alerts = []

    if len(df) > 100:
        alerts.append("🚨 High traffic volume detected")

    if df['fare'].mean() > 100000:
        alerts.append("💰 High average fare detected")

    if (df['distance'] > 50).any():
        alerts.append("⚠️ Abnormal long-distance trip detected")

    if df['location'].value_counts().max() > 50:
        alerts.append("🚗 Possible congestion in hotspot area")

    return alerts