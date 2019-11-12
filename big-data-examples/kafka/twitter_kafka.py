# coding : utf-8

from kafka import KafkaProducer
import tweepy
import json
import time


# Config Kafka Kafka Producer
producer = KafkaProducer(bootstrap_servers='X:9092')

# Config access API twitter
auth = tweepy.OAuthHandler('X', 'X')
auth.set_access_token('X', 'X')
api = tweepy.API(auth)

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        tweet = json.dumps(status._json).encode('utf-8') + '\n'
        producer.send('twitter', tweet)

    def on_error(self, status_code):
        print "Error", status_code

streamListenerM = MyStreamListener()
streamM = tweepy.Stream(auth = api.auth, listener=streamListenerM)
streamM.filter(track=['macaron'], async=True)

print time.strftime("%Y-%m-%d %H:%M:%S") + " [INFO] init KAFKA producer and twitter API connection ok"
