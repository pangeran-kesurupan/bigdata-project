import json
import time
import uuid
import random
from datetime import datetime
import os

# Folder tujuan stream
OUTPUT_DIR = "stream_data/transportation"

# Pastikan folder ada
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Data dummy
vehicle_types = ["car", "motor", "taxi"]
locations = ["Jakarta", "Bandung", "Surabaya", "Medan", "Bali"]

print("🚀 Trip Generator Started... (CTRL+C untuk stop)")

while True:
    trip = {
        "trip_id": str(uuid.uuid4()),
        "vehicle_type": random.choice(vehicle_types),
        "location": random.choice(locations),
        "distance": round(random.uniform(1, 60), 2),  # km
        "fare": round(random.uniform(10000, 500000), 2),  # rupiah
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    # nama file unik
    filename = f"{OUTPUT_DIR}/trip_{int(time.time()*1000)}.json"

    # simpan JSON
    with open(filename, "w") as f:
        json.dump(trip, f)

    print(f"Generated: {trip}")

    # delay (biar streaming terasa)
    time.sleep(2)