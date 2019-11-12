from cassandra.query import SimpleStatement
from cassandra.cluster import Cluster
from cassandra.cluster import ExecutionProfile
import time
from threading import Event


t = time.time()

execution_profil = ExecutionProfile(request_timeout=600)
profiles = {'node1': execution_profil}
cluster = Cluster(['x'], execution_profiles=profiles)
session = cluster.connect('test')


class PagedResultHandler(object):

    def __init__(self, future):
        self.error = None
        self.finished_event = Event()
        self.future = future
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_err)

    def handle_page(self, rows):
        for row in rows:
            pass

        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
        else:
            self.finished_event.set()

    def handle_err(self, exc):
        self.error = exc
        self.finished_event.set()

future = session.execute_async("SELECT * FROM test_tweet2")
handler = PagedResultHandler(future)
handler.finished_event.wait()
if handler.error:
    print(handler.error)

print('time_exec_test6=%s' % (time.time() - t))
