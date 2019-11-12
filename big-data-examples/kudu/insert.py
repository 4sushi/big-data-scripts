import kudu
from kudu.client import Partitioning
import random

"""
Script to insert fake data in Kudu table
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

    client = kudu.connect(host=['x', 'x','x'], port=7051)
    # Open a table
    table = client.table('test_tweet')
    # Create a new session so that we can apply write operations
    session = client.new_session()

    for i in range(0, 1000000):
        uniq_id = str(i)
        author = random_str(10)
        date_tweet = random_date()
        tweet = random_str(200)
        rate = random.randint(0, 5)
        # Insert a row
        op = table.new_insert({'id': uniq_id, 'author': author, 'date_tweet': date_tweet, 'tweet': tweet, 'rate': rate})
        session.apply(op)

        if i % 10000 == 9999:
            # Flush write operations, if failures occur, capture print them.
            try:
                session.flush()
            except kudu.KuduBadStatus as e:
                print(session.get_pending_errors())
