from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import concat
from pyspark.sql.functions import lit
import json



#Tells spark to read a csv from HDFS and return the the resulting dataframe
def grabDataFromHDFS(pathToCSV):
  csv_input = pathToCSV
  hdfs_csv_input = 'hdfs://{0}'.format(csv_input)
  dataframe = spark.read.csv(hdfs_csv_input, inferSchema=True, header=True)
  
  return dataframe


#Puts dataframe into HBase based on a given catalog
def loadingIntoHBase(data, givenCatalog):
  data.write.format("org.apache.hadoop.hbase.spark") \
      .options(catalog=givenCatalog, newTable = 5) \
      .option("hbase.spark.use.hbasecontext", False) \
      .save()
  
  print("Added " + json.loads(givenCatalog)["table"]["name"] + " to HBase!")


#returns a catalog for the Training Data
def getTrainingDataCatalog():
  catalog = ''.join("""{
                   "table":{"namespace":"default", "name":"trainingDataFinal", "tableCoder":"PrimitiveType"},
                   "rowkey":"key",
                   "columns":{
                     "Key":{"cf":"rowkey", "col":"key", "type":"string"},
                     "Temperature":{"cf":"weather", "col":"Temperature", "type":"double"},
                     "Humidity":{"cf":"weather", "col":"Humidity", "type":"double"},
                     "Light":{"cf":"weather", "col":"Light", "type":"double"},
                     "CO2":{"cf":"weather", "col":"CO2", "type":"double"},
                     "HumidityRatio":{"cf":"weather", "col":"HumidityRatio", "type":"double"},
                     "Occupancy":{"cf":"weather", "col":"Occupancy", "type":"int"}
                   }
                 }""".split())
  return catalog

#returns a catalog for the Testing Data
def getTestingDataCatalog():
  catalog = ''.join("""{
                   "table":{"namespace":"default", "name":"testingDataFinal", "tableCoder":"PrimitiveType"},
                   "rowkey":"key",
                   "columns":{
                     "id":{"cf":"rowkey", "col":"key", "type":"int"},
                     "Temperature":{"cf":"weather", "col":"Temperature", "type":"double"},
                     "Humidity":{"cf":"weather", "col":"Humidity", "type":"double"},
                     "Light":{"cf":"weather", "col":"Light", "type":"double"},
                     "CO2":{"cf":"weather", "col":"CO2", "type":"double"},
                     "HumidityRatio":{"cf":"weather", "col":"HumidityRatio", "type":"double"},
                     "Occupancy":{"cf":"weather", "col":"Occupancy", "type":"int"}
                   }
                 }""".split())
  return catalog

#returns a catalog for the Testing Data
def getTestingDataCatalog():
  catalog = ''.join("""{
                   "table":{"namespace":"default", "name":"testingDataFinal", "tableCoder":"PrimitiveType"},
                   "rowkey":"key",
                   "columns":{
                     "id":{"cf":"rowkey", "col":"key", "type":"int"},
                     "Temperature":{"cf":"weather", "col":"Temperature", "type":"double"},
                     "Humidity":{"cf":"weather", "col":"Humidity", "type":"double"},
                     "Light":{"cf":"weather", "col":"Light", "type":"double"},
                     "CO2":{"cf":"weather", "col":"CO2", "type":"double"},
                     "HumidityRatio":{"cf":"weather", "col":"HumidityRatio", "type":"double"},
                     "Occupancy":{"cf":"weather", "col":"Occupancy", "type":"int"}
                   }
                 }""".split())
  return catalog


if __name__ == "__main__":
  spark = SparkSession\
  .builder\
  .appName("newApp")\
  .getOrCreate()
  
  rawTrainingDataDF = grabDataFromHDFS("/tmp/trainingData.csv")
  rawTrainingDataDF = rawTrainingDataDF.drop("date")
  rawTrainingDataDF = rawTrainingDataDF.withColumn("Key",concat(rawTrainingDataDF["Temperature"],lit(','), rawTrainingDataDF["Humidity"],lit(','), rawTrainingDataDF["Light"],lit(','), rawTrainingDataDF["HumidityRatio"], lit(','), rawTrainingDataDF["CO2"]))  

  rawTestDataDF = grabDataFromHDFS("/tmp/testingData.csv")
  
  # Just adding a key column for this dataset so we can store it in HBase
  # For the trainingData, i just used the date as the key
  rawTestDataDF = rawTestDataDF.select("*").withColumn("id", monotonically_increasing_id())
  
  loadingIntoHBase(rawTrainingDataDF, getTrainingDataCatalog())
  loadingIntoHBase(rawTestDataDF, getTestingDataCatalog())
