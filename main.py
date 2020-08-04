from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from py4j.java_gateway import java_import
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import DateType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import json
from collections import OrderedDict
from pyspark.sql.types import FloatType
from pyspark.sql.functions import to_json, struct
from pyspark.sql.functions import monotonically_increasing_id 
from pyspark.sql.functions import concat
from pyspark.sql.functions import lit
import numpy as np
from decimal import Decimal
from decimal import getcontext

  
#loading data back from an HBase Table
def loadTableFromHBase(givenCatalog):
      
  result = spark.read.format("org.apache.hadoop.hbase.spark") \
      .options(catalog=givenCatalog) \
      .option("hbase.spark.use.hbasecontext", False) \
      .load()
  
  nameOfHBaseTable = json.loads(givenCatalog)['table']['name']
  print('Here is the Data Read From HBase Table: ' + nameOfHBaseTable)
  result.show()
  
  return result, nameOfHBaseTable

def putTableIntoHBase(df, givenCatalog):
  df.write.format("org.apache.hadoop.hbase.spark") \
      .options(catalog=givenCatalog, newTable = 5) \
      .option("hbase.spark.use.hbasecontext", False) \
      .save() 
  
  print("Added " + json.loads(givenCatalog)["table"]["name"] + " to HBase!")

def grabDataFromHDFS(pathToCSV):
  csv_input = pathToCSV
  hdfs_csv_input = 'hdfs://{0}'.format(csv_input)
  dataframe = spark.read.csv(hdfs_csv_input, inferSchema=True, header=True)
  
  return dataframe
  
#Using the data that we just pulled from HBase, here's a sample model on it
def build_model(training):
  training.cache()
  columns = training.columns
  columns.remove("Occupancy")
  
  assembler = VectorAssembler(inputCols=columns, outputCol="featureVec")
  lr = LogisticRegression(featuresCol="featureVec", labelCol="Occupancy")
  
  pipeline = Pipeline(stages=[assembler, lr])
  
  param_grid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.0001, 0.001, 0.01, 0.1, 1.0]) \
    .build()
  
  evaluator = BinaryClassificationEvaluator(labelCol="Occupancy")
  
  validator = TrainValidationSplit(estimator=pipeline,
                             estimatorParamMaps=param_grid,
                             evaluator=evaluator,
                             trainRatio=0.9)
  validator_model = validator.fit(training)

  return validator_model.bestModel
  
  
def convert_to_row(d: dict) -> Row:
    return Row(**OrderedDict(sorted(d.items())))
  
def classify(input, model):
  target_columns = input.columns + ["Prediction"]
  #input = input.select("Temperature","Humidity","Light","CO2", "HumidityRatio", "Occupancy")
  #target_columns = ["prediction"]
  return model.transform(input).select(target_columns)


def useTheModel(model, modelVersion, dataset, name):
  print("Here's the dataset we are using to test our model")
  dataset.show()

  sqlContext = SQLContext(spark.sparkContext)  
  
  input_df2 = dataset.select("Temperature","Humidity","Light","CO2", "HumidityRatio", "Occupancy")
  input_df2.show()
  
  output = classify(input_df2, model)

  df3 = spark.sparkContext.parallelize(output).toDF()

  
  df3 = df3.withColumn("prediction", df3["prediction"].cast(IntegerType()))


  df3 = df3.withColumn("Inputs",concat(df3["Temperature"],lit(','), df3["Humidity"],lit(','), df3["Light"],lit(','), df3["HumidityRatio"], lit(','), df3["CO2"]))  
  df3 = df3.withColumn("ModelVersion", lit(modelVersion))
  df3 = df3.withColumn("DataSet", lit(name))
  df3 = df3.withColumn("Key", concat(df3["Inputs"],lit('--'), df3["ModelVersion"]))
  df3 = df3.drop('Temperature')
  df3 = df3.drop('Humidity')
  df3 = df3.drop('Light')
  df3 = df3.drop('CO2')
  df3 = df3.drop('HumidityRatio')
  df3 = df3.drop('code')
  df3 = df3.drop('id')
  df3 = df3.withColumnRenamed("prediction","ModelPrediction")
  df3 = df3.withColumnRenamed("Occupancy","ActualValue")
  df3.show()

  return df3

def putTableIntoHBase(df, givenCatalog):
  df.write.format("org.apache.hadoop.hbase.spark") \
      .options(catalog=givenCatalog, newTable = 5) \
      .option("hbase.spark.use.hbasecontext", False) \
      .save() 
  
  print("Added " + json.loads(givenCatalog)["table"]["name"] + " to HBase!")
  
      
      
def createBatchScoreTable(trainingData):
  
  listOfTemps = [19.0, 19.5, 20.0, 20.5, 21.0, 21.5, 22.0, 22.5, 23.0, 23.5]
  listOfHums = [16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40]
  listOfLigh = [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500]
  listOfRats = [.002, .0025, .003, .0035, .004, .0045, .005, .0055, .006, .0065]
  listOfCO2 = [0, 200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000]
  
  p = sqlContext.createDataFrame(listOfTemps, FloatType()).toDF('Temperature')
  q = sqlContext.createDataFrame(listOfHums, IntegerType()).toDF('Humidity')
  r = sqlContext.createDataFrame(listOfLigh, IntegerType()).toDF('Light')
  s = sqlContext.createDataFrame(listOfCO2, IntegerType()).toDF('CO2')
  t = sqlContext.createDataFrame(listOfRats, FloatType()).toDF('HumidityRatio')

  fullouter = p.crossJoin(q).crossJoin(r).crossJoin(s).crossJoin(t)
  #print(fullouter.count())
  return fullouter

      
def classifyBatchScoreTable(model, batchScoreDF):
  withPredictions = classify(fullOuter, actualModel)
  withPredictions = withPredictions.withColumn("Key",concat(withPredictions["Temperature"],lit(','), withPredictions["Humidity"],lit(','), withPredictions["Light"],lit(','), withPredictions["CO2"], lit(','), withPredictions["HumidityRatio"]))  
  withPredictions.show()
  
  return withPredictions
  
      
      
    
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
  
def getModelResultsCatalog():
    modelResultsCatalog = ''.join("""{
                 "table":{"namespace":"default", "name":"ModelResults3", "tableCoder":"PrimitiveType"},
                 "rowkey":"key",
                 "columns":{
                   "Key":{"cf":"rowkey", "col":"key", "type":"string"},
                   "Inputs":{"cf":"outputs", "col":"Inputs", "type":"string"},
                   "ModelPrediction":{"cf":"outputs", "col":"ModelPrediction", "type":"int"},
                   "ActualValue":{"cf":"outputs", "col":"ActualValue", "type":"int"},
                   "ModelVersion":{"cf":"outputs", "col":"ModelVersion", "type":"string"},
                   "DataSet":{"cf":"outputs", "col":"DataSet", "type":"string"}
                 }
               }""".split())
    return modelResultsCatalog

def getBatchScoreTableCatalog():
    modelResultsCatalog = ''.join("""{
                 "table":{"namespace":"default", "name":"BatchTable", "tableCoder":"PrimitiveType"},
                 "rowkey":"key",
                 "columns":{
                     "Key":{"cf":"rowkey", "col":"key", "type":"string"},
                     "Temperature":{"cf":"weather", "col":"Temperature", "type":"double"},
                     "Humidity":{"cf":"weather", "col":"Humidity", "type":"double"},
                     "Light":{"cf":"weather", "col":"Light", "type":"double"},
                     "CO2":{"cf":"weather", "col":"CO2", "type":"double"},
                     "HumidityRatio":{"cf":"weather", "col":"HumidityRatio", "type":"double"},
                     "Prediction":{"cf":"weather", "col":"Prediction", "type":"double"}
                 }
               }""".split())
    return modelResultsCatalog
  
  
# Main Method
if __name__ == "__main__":
  spark = SparkSession\
    .builder\
    .appName("HBase and PySpark Model Demonstration")\
    .getOrCreate()
    
  sc = spark.sparkContext
  sqlContext = SQLContext(sc)
  
  trainDataCtlg = getTrainingDataCatalog()
  
  #grabs tables from HBase
  trainingData, nameOfTrainingTableInHBase = loadTableFromHBase(trainDataCtlg)
  
  #dropping date column since we dont need it
  result = result.drop('date')
  
  additionalTrainingData = grabDataFromHDFS("/tmp/trainingData.csv")
  
  #builds and saves the model
  #actualModel = build_model(trainingData)
  target_path = "hdfs:///tmp/spark-model"
  #actualModel.write().overwrite().save(target_path)
  
  actualModel = PipelineModel.load(target_path)
  
  fullOuter = createBatchScoreTable(trainingData)
  classifiedBatchTable = classifyBatchScoreTable(actualModel, fullOuter)
  putTableIntoHBase(classifiedBatchTable, getBatchScoreTableCatalog())
  
  
  classifiedBatchTable, name = loadTableFromHBase(getBatchScoreTableCatalog())
  

    