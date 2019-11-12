from cassandra.cluster import Cluster
import random

"""
Script to insert fake data in SQL table
"""

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
    

if __name__ == '__main__':

    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('test')

    for i in range(0, 1000000):
        uniq_id = str(i)
        author = random_str(10)
        date_tweet = random_date()
        tweet = random_str(200)
        rate = random.randint(0, 5)
        session.execute("INSERT INTO test_tweet(id, author, date, tweet, rate) VALUES(%s, %s, %s, %s, %s)", (uniq_id, author, date_tweet, tweet, rate))
