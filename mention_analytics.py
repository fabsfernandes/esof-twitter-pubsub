from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import json

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= # add your GCP credentials
project_id = # add your GCP project id
subscription_id = # add your GCP topic subscription

# Number of seconds the subscriber should listen for messages
timeout = 600.0

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    tweet = json.loads(message.data)
    for mention in tweet['entities']['user_mentions']:
        print(mention['screen_name'])
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print('====================================')
print('=== SISTEMA ANALYTICS DE MENÇÕES ===')
print('====================================')
with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()
