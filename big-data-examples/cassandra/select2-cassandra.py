from cassandra.query import SimpleStatement
from cassandra.cluster import Cluster
from cassandra.cluster import ExecutionProfile
import time

t = time.time()

execution_profil = ExecutionProfile(request_timeout=600)
profiles = {'node1': execution_profil}
cluster = Cluster(['x'], execution_profiles=profiles)
session = cluster.connect('test')

query = "SELECT * FROM test_tweet1" 
fetch_size=10000
statement = SimpleStatement(query, fetch_size=fetch_size)
for row in session.execute(statement, execution_profile='node1'):
    pass

print(time.time() - t)
print('fetch_size=%s' % (fetch_size))

