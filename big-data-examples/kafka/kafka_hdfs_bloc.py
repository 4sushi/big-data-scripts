# encoding : utf8
from kafka import KafkaConsumer
from hdfs import InsecureClient
import bson
import time

MAX_SIZE_FILE = 1024 * 1024 * 128 # 128mb (in bytes)

# Init HDFS
client = InsecureClient('http://X:50070', user='X')
hdfs_dir = 'tweets/'
hdfs_file  = 'tweets.json'
hdfs_files_list = client.list(hdfs_dir)
if len(hdfs_files_list) > 0:
    # Get last file
    hdfs_file = sorted(hdfs_files_list, reverse=True)[0]
    hdfs_file_num = int(hdfs_file.split('.')[0])
    hdfs_file_size = client.status(hdfs_dir + hdfs_file)['length'] # in bytes
else:
    # Create file
    hdfs_file_num = 1
    hdfs_file = str(hdfs_file_num) + '.json'
    hdfs_file_size = 0 # 0 bytes
    client.write(hdfs_dir + hdfs_file, '')

# Init kafka
consumer = KafkaConsumer('X', group_id='X',
                         bootstrap_servers='X:9092')

print time.strftime("%Y-%m-%d %H:%M:%S") + ' [INFO] init KAFKA consumer and HDFS connection ok'

# New kafka message
for msg in consumer:
    tweet = msg.value
    json_size = len(tweet)
    if hdfs_file_size + json_size > MAX_SIZE_FILE:
        # Create new file
        hdfs_file_num += 1
        hdfs_file = str(hdfs_file_num) + '.json'
        hdfs_file_size = json_size
        client.write(hdfs_dir + hdfs_file, '')
    else:
        # Use current file
        hdfs_file_size += json_size

    # print time.strftime("%Y-%m-%d %H:%M:%S") + ' [DEBUG] new tweet (consumer) size=' + str(json_size)
    # Write message in HDFS
    client.write(hdfs_dir + hdfs_file, data=tweet, append=True)
