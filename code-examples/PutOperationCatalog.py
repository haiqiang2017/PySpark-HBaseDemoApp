from pyspark.sql import Row
from pyspark.sql import SparkSession

spark = SparkSession\
   .builder\
   .appName("SampleApplication")\
   .getOrCreate()


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

employee = [(10, 'jonD', 'Jon Daniels', 'CA'), (6, 'billR', 'Bill Robert', 'FL')]
employeeRDD = spark.sparkContext.parallelize(employee)
employeeMap = employeeRDD.map(lambda x: Row(key=int(x[0]), empId=x[1], empName=x[2], empState=x[3]))
employeeDF = spark.createDataFrame(employeeMap)


employeeDF.write.format("org.apache.hadoop.hbase.spark") \
   .options(catalog=tableCatalog, newTable=5) \
   .option("hbase.spark.use.hbasecontext", False) \
   .save()
# newTable refers to the NumberOfRegions which has to be > 3

# scan ‘tblEmployee’, {‘LIMIT => 2}


