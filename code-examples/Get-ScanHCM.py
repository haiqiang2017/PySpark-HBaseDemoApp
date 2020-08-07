from pyspark.sql import Row
from pyspark.sql import SparkSession

spark = SparkSession \
   .builder \
   .appName("SampleApplication") \
   .getOrCreate()

df = spark.read.format("org.apache.hadoop.hbase.spark") \
.option("hbase.columns.mapping", "key INTEGER :key, empId STRING personal:empId, empName STRING personal:empName, empState STRING personal:empState") \
.option("hbase.table", "tblEmployee") \
.option("hbase.spark.use.hbasecontext", False) \
.load() 

df.show()


#Using PySpark.SQL
#df.createOrReplaceTempView("personView")
#result = spark.sql("SELECT * FROM personView") # SQL Query
#result.show()
