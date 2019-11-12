# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark import SparkContext

SparkContext.setSystemProperty("hive.metastore.uris", "thrift://localhost:9083")

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("show databases").show()
spark.sql("use tmp")
spark.sql("ADD JAR /home/x/hive-serdes-1.0-SNAPSHOT.jar")
spark.sql("show tables").show()

df = spark.sql('select * from x')

df.describe(['nbWord']).show()
