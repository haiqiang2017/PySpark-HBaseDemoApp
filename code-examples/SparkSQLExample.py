from pyspark.sql import Row
from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .appName("PySparkSQLExample") \
  .getOrCreate()

# Catalog

tableCatalog = ''.join("""{
               "table":{"namespace":"default", "name":"tblEmployee", "tableCoder":"PrimitiveType"},
               "rowkey":"key",
               "columns":{
                 "key":{"cf":"rowkey", "col":"key", "type":"int"},
                 "empId":{"cf":"personal","col":"empId","type":"string"},
                 "empName":{"cf":"personal", "col":"empName", "type":"string"},
                 "empState":{"cf":"personal", "col":"empState", "type":"string"}
               }
             }""".split())

# Adding first 2 rows

employee = [(10, 'jonD', 'Jon Daniels', 'CA'), (6, 'billR', 'Bill Robert', 'FL')]
employeeRDD = spark.sparkContext.parallelize(employee)
employeeMap = employeeRDD.map(lambda x: Row(key=int(x[0]), empId=x[1], empName=x[2], empState=x[3]))
employeeDF = spark.createDataFrame(employeeMap)

employeeDF.write.format("org.apache.hadoop.hbase.spark") \
  .options(catalog=tableCatalog, newTable=5) \
  .option("hbase.spark.use.hbasecontext", False) \
  .save()

df = spark.read.format("org.apache.hadoop.hbase.spark") \
  .options(catalog=tableCatalog) \
  .option("hbase.spark.use.hbasecontext", False) \
  .load()

df.createOrReplaceTempView("sampleView")
result = spark.sql("SELECT * FROM sampleView")

print("The PySpark DataFrame with only the first 2 rows")
result.show()

# Adding 2 more rows

employee = [(11, 'bobG', 'Bob Graham', 'TX'), (12, 'manasC', 'Manas Chakka', 'GA')]
employeeRDD = spark.sparkContext.parallelize(employee)
employeeMap = employeeRDD.map(lambda x: Row(key=int(x[0]), empId=x[1], empName=x[2], empState=x[3]))
employeeDF = spark.createDataFrame(employeeMap)

employeeDF.write.format("org.apache.hadoop.hbase.spark") \
  .options(catalog=tableCatalog, newTable=5) \
  .option("hbase.spark.use.hbasecontext", False) \
  .save()

# Notice here I didn't reload "df" before doing result.show() again
print("The PySpark Dataframe immediately after writing 2 more rows")
result.show()
