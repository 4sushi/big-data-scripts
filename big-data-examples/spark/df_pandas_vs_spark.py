from pyspark import SparkContext, SQLContext, sql
from pyspark.sql.functions import to_date
import pandas
import numpy as np
from pyspark.sql import functions as F
import copy

"""

author: mbouchet
"""

sc = SparkContext(appName="Test dataframe")
sc.setLogLevel('WARN')
sqlContext = SQLContext(sc)

file_csv = "file.csv"
df_spark = sqlContext.read.format("csv").options(header='true', inferschema='true').load(file_csv)
df_pandas = pandas.read_csv(file_csv)

# Nb row
print ("Nb row :")
print (df_spark.count())
print (df_pandas.shape[0])

# Nb column
print ("Nb column :")
print (len(df_spark.schema))
print (df_pandas.shape[1])

# Drop duplicate
print ("Drop duplicate :")
df_spark_tmp = df_spark.drop_duplicates(subset=['field1', 'field2', 'field3', 'field4'])
df_pandas_tmp = df_pandas.drop_duplicates(subset=['field1', 'field2', 'field3', 'field4'])
print (df_spark.count(), df_spark_tmp.count())
print (df_pandas.shape[0], df_pandas_tmp.shape[0])

# Drop duplicate (with group by)
print ("Drop duplicate (with group by) :")
df_spark_tmp = df_spark.groupby(['field1', 'field2', 'field3', 'field4']).agg(F.first('field5').alias('field5'), F.first('field6').alias('field6'))
df_pandas_tmp = df_pandas.groupby(['field1', 'field2', 'field3', 'field4']).first()
print (df_spark.count(), df_spark_tmp.count())
print (df_pandas.shape[0], df_pandas_tmp.shape[0])


# Set values 1 column
print ("Set values 1 column :")
df_spark_tmp = copy.copy(df_spark)
df_pandas_tmp = df_pandas.copy()
df_spark_tmp = df_spark_tmp.withColumn('field5', df_spark_tmp.field5 * 2)
df_pandas_tmp['field5'] = df_pandas_tmp['field5'] * 2
print(df_spark.first()['field5'], df_spark_tmp.first()['field5'])
print(df_pandas.iloc[0]['field5'], df_pandas_tmp.iloc[0]['field5'])

# Add  new column with fix value
print ("Add  new column with fix value :")
df_spark_tmp = copy.copy(df_spark)
df_pandas_tmp = df_pandas.copy()
df_spark_tmp = df_spark_tmp.withColumn('field5_new', F.lit(0))
df_pandas_tmp['field5_new'] = 0
print(df_spark.first()['field5'], df_spark_tmp.first()['field5_new'])
print(df_pandas.iloc[0]['field5'], df_pandas_tmp.iloc[0]['field5_new'])

# Add  new column with value other column
print ("Add  new column :")
df_spark_tmp = copy.copy(df_spark)
df_pandas_tmp = df_pandas.copy()
df_spark_tmp = df_spark_tmp.withColumn('field5_new', df_spark_tmp.field5 * 2)
df_pandas_tmp['field5_new'] = df_pandas_tmp['field5'] * 2
print(df_spark.first()['field5'], df_spark_tmp.first()['field5_new'])
print(df_pandas.iloc[0]['field5'], df_pandas_tmp.iloc[0]['field5_new'])

# Replace values
print ("Replace values :")
df_spark_tmp = copy.copy(df_spark)
df_pandas_tmp = df_pandas.copy()
df_spark_tmp = df_spark.na.replace(48, np.nan, 'field5')
df_pandas_tmp['field5'] = df_pandas['field5'].replace(48, np.nan)
print (df_spark.where(df_spark.field5 == np.nan).count(), df_spark_tmp.where(df_spark_tmp.field5 == np.nan).count())
print (df_pandas['field5'].isnull().sum(), df_pandas_tmp['field5'].isnull().sum())

# Rename column
print ("Rename column :")
df_spark_tmp = copy.copy(df_spark)
df_pandas_tmp = df_pandas.copy()
df_pandas_tmp = df_pandas_tmp.rename(columns={'field5': 'field5_rename'})
df_spark_tmp = df_spark_tmp.select(F.col('field5').alias('field5_rename'))
print(df_spark.first()['field5'], df_spark_tmp.first()['field5_rename'])
print(df_pandas.iloc[0]['field5'], df_pandas_tmp.iloc[0]['field5_rename'])

# Sort values
print ("Sort values :")
df_spark_tmp = df_spark.sort(['field1', 'field2', 'field3', 'field4'])
df_pandas_tmp = df_pandas.sort_values(['field1', 'field2', 'field3', 'field4'])
print (df_spark.first()['field2'], df_spark_tmp.first()['field2'])
print (df_pandas.iloc[0]['field2'], df_pandas_tmp.iloc[0]['field2'])

# Select some columns
print ("Select some columns :")
df_spark_tmp = df_spark[['field1', 'field2', 'field5', 'field3', 'field4','field6']]
df_pandas_tmp = df_pandas[['field1', 'field2', 'field5', 'field3', 'field4','field6']]
print (len(df_spark.schema), len(df_spark_tmp.schema))
print (df_pandas.shape[1], df_pandas_tmp.shape[1])

# Group by with multiple aggregates
print ("Group by with multiple aggregates :")
df_spark_tmp = df_spark.groupby(['field1', 'field2', 'field3']).agg(F.min('field5').alias('field5_min'), F.max('field5').alias('field5_max'), F.first('field5').alias('field5_first'), F.last('field5').alias('field5_last'),F.count('field6').alias('field6_count'))
df_pandas_tmp = df_pandas.groupby(['field1', 'field2', 'field3']).agg({'field5': ['min','max','first','last'],'field6':'count'})
# Set all column name to the same level
df_pandas_tmp.columns = ["_".join(x) for x in df_pandas_tmp.columns.ravel()]
df_pandas_tmp.reset_index(inplace=True)
# Get first row
df_spark_tmp_first = df_spark_tmp.sort(['field1', 'field2', 'field3']).first()
df_pandas_tmp_first = df_pandas_tmp.iloc[0]
print(df_spark_tmp_first['field5_min'], df_spark_tmp_first['field5_max'], df_spark_tmp_first['field5_first'], df_spark_tmp_first['field5_last'], df_spark_tmp_first['field6_count'])
print(df_pandas_tmp_first['field5_min'], df_pandas_tmp_first['field5_max'], df_pandas_tmp_first['field5_first'], df_pandas_tmp_first['field5_last'], df_spark_tmp_first['field6_count'])

# Pandas tail
print ("Pandas tail :")
df_spark_tmp = df_spark.sort(['field3', 'field5'], ascending=False) # invert order
df_pandas_tmp = df_pandas.sort_values(['field3', 'field5']).reset_index()
df_spark_tmp_part = df_spark_tmp.limit(2).sort('field5')
df_pandas_tmp_part = df_pandas_tmp.tail(2)
print(df_spark_tmp_part.count(), df_spark_tmp_part.first()['field5'])
print(df_pandas_tmp_part.shape[0], df_pandas_tmp_part.iloc[0]['field5'])


# Calculate median
print ("Calculate median :")
df_spark_tmp = copy.copy(df_spark)
df_pandas_tmp = df_pandas.copy()
df_spark_tmp = df_spark_tmp.agg(F.collect_list("field5").alias('field5_list'))
def fct_median(list):
    # calculate median
    median = np.median(list)
    # change type numpy.float64 -> float
    median = median.item()
    return median
# Define function you can call in spark function (example in withColumn)
# Note : the second param is the expected type result
# https://stackoverflow.com/a/40162085
fct_median_spark = F.udf(fct_median, sql.types.FloatType())
df_spark_tmp = df_spark_tmp.withColumn('field5_median', fct_median_spark(df_spark_tmp.field5_list))
df_pandas_tmp['field5_median'] = df_pandas_tmp['field5'].median()
print (df_spark_tmp.first()['field5_median'])
print (df_pandas_tmp.iloc[0]['field5_median'])


