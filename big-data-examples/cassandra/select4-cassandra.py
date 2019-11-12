from cassandra.query import SimpleStatement
from cassandra.cluster import Cluster
from cassandra.cluster import ExecutionProfile
import time
from threading import Event


t = time.time()

end_event = Event()

cpt = 0
future = None

def handle_success(rows):
    global cpt
    global future
    try:
        print(len(rows))
    except Exception:
        print("Failed to process user %s")
        # don't re-raise errors in the callback
    finally:
        if future.has_more_pages:
            future.start_fetching_next_page()
        else:
            end_event.set()

def handle_error(exception):
    end_event.set()
    print("Failed to fetch user info: %s", exception)

execution_profil = ExecutionProfile(request_timeout=600)
profiles = {'node1': execution_profil}
cluster = Cluster(['x'], execution_profiles=profiles)
session = cluster.connect('test')


query = "SELECT * FROM test_tweet2" 
fetch_size=10000
statement = SimpleStatement(query, fetch_size=fetch_size)
future = session.execute_async(statement, execution_profile='node1')
future.add_callbacks(handle_success, handle_error)

end_event.wait()

print('fetch_size=%s' % (fetch_size))
print(time.time() - t)
