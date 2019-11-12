# encoding : utf8
from kafka import KafkaConsumer
from hdfs import InsecureClient
import bson
import time

# Init HDFS
client = InsecureClient('http://X:50070', user='X')
hdfs_file  = 'tweets.json'
# Create file if not exist
hdfs_files_list = client.list('')
if hdfs_file not in hdfs_files_list:
    with client.write(hdfs_file) as writer:
        writer.write('')

# Init kafka
consumer = KafkaConsumer('X', group_id='X_GRP',
                         bootstrap_servers='X:9092')

with client.write(hdfs_file, append=True) as writer:
    # New kafka message
    for msg in consumer:
        print time.strftime("%Y-%m-%d %H:%M:%S") + " [DEBUG] new tweet (consumer)"
        tweet = msg.value
        # Write message in HDFS
        writer.write(tweet)
