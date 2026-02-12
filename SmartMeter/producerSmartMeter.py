# Smart Meter Producer — sends simulated smart meter readings to Pub/Sub
# SOFE4630U Milestone 3 — Design Part
#
# Usage:
#   1. Place your GCP service account JSON key in this folder.
#   2. Set your project_id below.
#   3. Run: python producerSmartMeter.py

import os
import glob
import json
import time
import random

from google.cloud import pubsub_v1  # pip install google-cloud-pubsub

# Auto-detect service account key in current directory
files = glob.glob("*.json")
if not files:
    raise Exception("No Service Account JSON key found in this directory.")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# TODO: fill in your GCP project ID
project_id = "cloudcomputing-milestone1"
topic_id = "smartmeter_input"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Simulate smart meter readings
# Each reading has: ID, Pressure(kPa), Temperature(C), Humidity(%), Wind(m/s)
num_readings = 50

for i in range(num_readings):
    # Randomly decide whether to include a None value (for testing the filter)
    has_none = random.random() < 0.2  # 20% chance of a missing value

    reading = {
        "ID": i,
        "Pressure(kPa)": round(random.uniform(95.0, 105.0), 2) if not has_none else None,
        "Temperature(C)": round(random.uniform(-10.0, 40.0), 2),
        "Humidity(%)": round(random.uniform(20.0, 90.0), 2),
        "Wind(m/s)": round(random.uniform(0.0, 30.0), 2),
    }

    future = publisher.publish(topic_path, json.dumps(reading).encode("utf-8"))
    status = "MISSING VALUE" if has_none else "OK"
    print(f"Sent reading ID={i}  [{status}]  → {reading}")
    time.sleep(0.5)

print(f"\nDone! Sent {num_readings} readings to {topic_path}")
