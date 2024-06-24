# Databricks notebook source
from pyspark.sql import *
path = ['dbfs:/FileStore/parquetfiles/userdata1.parquet','dbfs:/FileStore/parquetfiles/userdata2.parquet']

dp=spark.read.parquet(*['dbfs:/FileStore/parquetfiles/userdata1.parquet', 'dbfs:/FileStore/parquet/userdata2.parquet'],header=True)
# dp = spark.read.parquet(*path)
dp.count()



# COMMAND ----------

data=[(1,'ritu'),(2,'vin')]
cols=('id','name')
dfp=spark.createDataFrame(data,cols)
display(dfp)


# COMMAND ----------

dfp.write.parquet('dbfs:/FileStore/parquetwrite/sample.parquet', mode='ignore')

# COMMAND ----------

display(spark.read.parquet('dbfs:/FileStore/parquetwrite/sample.parquet/part-00000-tid-5294494395488163857-53f95e1f-7aad-461e-ae05-2512367efec2-233-1.c000.snappy.parquet'))


# COMMAND ----------

dfp.show(n=1,truncate=-2)
