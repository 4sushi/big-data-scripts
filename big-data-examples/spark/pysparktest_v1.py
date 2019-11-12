from pyspark.sql import HiveContext
from pyspark import SparkContext
import time


sc = SparkContext(appName="Python Spark SQL Hive integration example")
sc.setSystemProperty("hive.metastore.uris", "thrift://localhost:9083")
sc.setLogLevel('WARN')

sqlContext = HiveContext(sc)
sqlContext.sql("show databases").show()

t = time.time()

query = """SELECT * from x"""

df = sqlContext.sql(query) 

print("time: %s sec" % (time.time() - t))
t = time.time()

sqlContext.sql(query).head(1)

print("time: %s sec" % (time.time() - t))
