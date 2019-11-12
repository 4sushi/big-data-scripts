"""
Run script with pypy
"""

from cassandra.cluster import Cluster
import random 
from itertools import count
from threading import Event

sentinel = object()
num_queries = 1000000
num_started = count()
num_finished = count()
finished_event = Event()
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('test')


def random_str(size):
    """
    Return random string (only [a-z])
    """
    # size = random.randint(1, max_size)
    text = ''
    for i in range(0, size):
        i_char = random.randint(1,26)
        c = chr(96 + i_char)
        text += c
    return text


def random_date():
    """
    Return random date (str, format YYYY-MM-DD)
    """
    year = str(random.randint(2000, 2018))
    month = str(random.randint(1, 12)).rjust(2, '0')
    day = str(random.randint(1, 28)).rjust(2, '0')
    d = '%s-%s-%s' % (year, month, day)
    return d
    



def insert_next(previous_result=sentinel):
    num = next(num_started)
    uniq_id = str(num)
    author = random_str(10)
    date_tweet = random_date()
    tweet = random_str(200)
    rate = random.randint(0, 5)
    query = "INSERT INTO test_tweet4(id, author, date, tweet, rate) VALUES('%s', '%s', '%s', '%s', %s)" % (uniq_id, author, date_tweet, tweet, rate)


    if previous_result is not sentinel:
        if isinstance(previous_result, BaseException):
            print("Error on insert: %r", previous_result)
        if next(num_finished) >= num_queries:
            finished_event.set()

    
    if num <= num_queries:
        future = session.execute_async(query)
        # NOTE: this callback also handles errors
        future.add_callbacks(insert_next, handle_error)

def handle_error(exception):
    print("Failed to fetch user info: %s", exception)






for i in range(min(120, num_queries)):
    insert_next()

finished_event.wait()