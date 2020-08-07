# Before executing this, make sure you create a table named 'tblEmployee2' with a column family named 'personal'
# create 'tblEmployee2', 'personal'

from pyspark.sql import Row
from pyspark.sql import SparkSession

spark = SparkSession\
   .builder\
   .appName("SampleApplication")\
   .getOrCreate()


employee = [(10, 'jonD', 'Jon Daniels', 'CA'), (6, 'billR', 'Bill Robert', 'FL')]
employeeRDD = spark.sparkContext.parallelize(employee)
employeeMap = employeeRDD.map(lambda x: Row(key=int(x[0]), empId=x[1], empName=x[2], empState=x[3]))
employeeDF = spark.createDataFrame(employeeMap)

employeeDF.write.format("org.apache.hadoop.hbase.spark") \
        .option("hbase.columns.mapping", "key INTEGER :key, empId STRING personal:empId, empName STRING personal:empName, empState STRING personal:empState") \
        .option("hbase.table", "tblEmployee2") \
        .option("hbase.spark.use.hbasecontext", False) \
        .save() 
        
# scan ‘tblEmployee2’, {‘LIMIT => 2}
