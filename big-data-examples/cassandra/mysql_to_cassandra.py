"""
Run script with pypy
"""

from cassandra.cluster import Cluster
import random 
from itertools import count
from threading import Event
import pymysql
import time

sentinel = object()
num_queries = 81834096
num_started = count()
num_finished = count()
finished_event = Event()
cluster = Cluster(['x'])
session = cluster.connect('test')
cursor = None
nb_insert= 0


def insert_next(previous_result=sentinel):
    global cursor
    global nb_insert

    num = next(num_started)
    nextrows = cursor.fetchone()

    query = """
    INSERT INTO x
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    
    if previous_result is not sentinel:
        if isinstance(previous_result, BaseException):
            print("Error on insert: %r", previous_result)
        if next(num_finished) >= num_queries:
            finished_event.set()

    
    if num <= num_queries:
        future = session.execute_async(query, nextrows)
        # NOTE: this callback also handles errors
        future.add_callbacks(insert_next, insert_next)
        nb_insert += 1
        if nb_insert % 10000 == 9999:
            print(nb_insert)
        


def handle_error(exception):
    print("Failed to fetch user info: %s", exception)


if __name__ == '__main__':

    t = time.time()

    connection = pymysql.connect(host="x", user='x', passwd='x',db='x', charset='utf8')
    cursor = connection.cursor(cursor=pymysql.cursors.SSCursor)
    cursor.execute("select * from x")

    for i in range(120):
        insert_next()

    finished_event.wait()
    cursor.close()
    connection.close()

    print('%s ms' % (time.time() - t))
