from pyspark import SparkContext, SQLContext, sql
from pyspark.sql.functions import to_date
import pandas
import numpy as np
from pyspark.sql import functions as F
import copy
import time

# TODO NOT FINISH

sc = SparkContext(appName="DF spark vs RDD spark")
sc.setLogLevel('WARN')
sqlContext = SQLContext(sc)

file_csv = "file.csv"
df_spark = sqlContext.read.format("csv").options(header='true', inferschema='true').load(file_csv)
rdd_spark = sqlContext.read.format("csv").options(header='true', inferschema='true').load(file_csv).rdd

t = time.time()

# where
df_spark_tmp = df_spark.where("price > 100")
rdd_spark_tmp = rdd_spark.filter(lambda l: l.price > 100)
print(df_spark_tmp.count())
print(rdd_spark_tmp.count())

# Add column
rdd_spark_tmp = rdd_spark.map(lambda row : sql.Row(newPrice=row.price*2, **row.asDict()))
print(df_spark_tmp.count())
print(rdd_spark_tmp.first())

print ("sec : %s " % (time.time() - t))

rdd = rdd_spark_tmp.map(lambda (x): ((x.hotel_id, x.room_id, x.checkin, x.date_extract), (x.price, 1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
print(rdd.take(20))
