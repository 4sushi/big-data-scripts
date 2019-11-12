from cassandra.cluster import Cluster
from cassandra.cluster import ExecutionProfile
from cassandra.cluster import SimpleStatement

import time

execution_profil = ExecutionProfile(request_timeout=600)
profiles = {'node1': execution_profil}
cluster = Cluster(['x'], execution_profiles=profiles)
session = cluster.connect('test')


def test1():
    query = "SELECT * FROM test_tweet_select" 
    fetch_size=10000
    statement = SimpleStatement(query, fetch_size=fetch_size)
    for row in session.execute(statement, execution_profile='node1'):
        pass

def test2():
    result = session.execute("SELECT * FROM test_tweet_select WHERE date >= '1999-01-01' and rate > 0 and author is not null and id = '100000'", execution_profile='node1')
    for row in result:
        pass


def test3():
    result = session.execute("SELECT * FROM test_tweet_select WHERE tweet = 'abmlvofjtgaaesxnzibuoancwhqepxtvsizuzttituvmwixzatvlhnoveuduwnsozsiheoqscetzubpcwgmwbpgtazqqfavxdejwqjmhiewryoymyoidgsbereqqundrndeebxghapeinipxqyrfiiaagdxpsshvvdkekmdbfsrenitapaenagtstdomgkcsdkhdnpgj' ALLOW FILTERING", execution_profile='node1')
    for row in result:
        pass

def test4():
    result =  session.execute("SELECT * FROM test_tweet_select WHERE date >= '2017-01-01' AND date <= '2017-12-31'", execution_profile='node1')
    for row in result:
        pass

def test5():
    result = session.execute("SELECT * FROM test_tweet_select WHERE date >= '1999-01-01' AND rate >= 1 AND rate <= 3 ALLOW FILTERING", execution_profile='node1')
    for row in result:
        pass

def test6():
    result = session.execute("SELECT count(1) as nb FROM test_tweet_select ALLOW FILTERING", execution_profile='node1')
    for row in result:
        pass


if __name__ == '__main__':

    for i in range(0, 1):
        t = time.time()
        #test1()
        print('time_exec_test1=%s' % (time.time() - t))
        t = time.time()
        test2()
        print('time_exec_test2=%s' % (time.time() - t))
        t = time.time()
        test3()
        print('time_exec_test3=%s' % (time.time() - t))
        t = time.time()
        test4()
        print('time_exec_test4=%s' % (time.time() - t))
        t = time.time()
        test5()
        print('time_exec_test5=%s' % (time.time() - t))
        t = time.time()
        test6()
        print('time_exec_test6=%s' % (time.time() - t))


