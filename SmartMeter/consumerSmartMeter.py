# Smart Meter Consumer — receives processed smart meter readings from Pub/Sub
# SOFE4630U Milestone 3 — Design Part
#
# Usage:
#   1. Place your GCP service account JSON key in this folder.
#   2. Set your project_id below.
#   3. Create a subscription "smartmeter_output-sub" on the "smartmeter_output" topic.
#   4. Run: python consumerSmartMeter.py

import os
import glob
import json

from google.cloud import pubsub_v1  # pip install google-cloud-pubsub

# Auto-detect service account key in current directory
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# TODO: fill in your GCP project ID
project_id = ""
subscription_id = "smartmeter_output-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    data = json.loads(message.data)
    print(f"Received processed reading: {data}")
    message.ack()


streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path} ...\n")

with subscriber:
    streaming_pull_future.result()
