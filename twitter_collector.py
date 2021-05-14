from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import time
import json

# setup security
access_token = # add your Twitter token
access_token_secret = # add your Twitter token
consumer_key = # add your Twitter token
consumer_secret = # add your Twitter token

from google.cloud import pubsub_v1
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= # add your GCP credentials
project_id = # add your GCP project id
topic_id = # add your GCP topic id


class MyListener(StreamListener):

    def __init__(self):
        global publisher
        publisher = pubsub_v1.PublisherClient()
        print('init')

    def on_data(self, data):
        topic_path = publisher.topic_path(project_id, topic_id)
        tweet = json.loads(data)
        tweet_encoded = data.encode("utf-8")
        future = publisher.publish(topic_path, tweet_encoded)
        print(tweet)
        return True
      
    def on_error(self, status):
        print(status)

    def file_close(self):
        print('close')

        
listener = MyListener()
oauth = OAuthHandler(consumer_key, consumer_secret)
oauth.set_access_token(access_token, access_token_secret)

max_time = 10  # in seconds
start_time = time.time()

## collecting...
stream = Stream(oauth, listener)
stream.filter(track=['juliette', 'bbb', 'bbb21', 'gil'], is_async=True) # tweets about Big Brother Brasil 2021 TV Show 

elapsed_time = (time.time() - start_time)
while elapsed_time < max_time:
    elapsed_time = (time.time() - start_time)

print('**Finish**')
stream.disconnect()
