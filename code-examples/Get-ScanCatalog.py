from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .appName("SampleApplication") \
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

table = spark.read.format("org.apache.hadoop.hbase.spark") \
  .options(catalog=tableCatalog) \
  .option("hbase.spark.use.hbasecontext", False) \
  .load()

table.show()



#tableCatalog = ''.join("""{
#                "table":{"namespace":"default", "name":"tblEmployee", "tableCoder":"PrimitiveType"},
#                "rowkey":"key",
#                "columns":{
#                  "key":{"cf":"rowkey", "col":"key", "type":"int"},
#                  "empName":{"cf":"personal", "col":"empName", "type":"string"}
#                }
#              }""".split())

