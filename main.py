from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.functions import concat
from pyspark.sql.functions import lit

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.evaluation import BinaryClassificationEvaluator

import json
from collections import OrderedDict


# loading data from an HBase Table
def loadTableFromHBase(givenCatalog):
    result = spark.read.format("org.apache.hadoop.hbase.spark") \
        .options(catalog=givenCatalog) \
        .option("hbase.spark.use.hbasecontext", False) \
        .load()

    return result

# putting data into an HBase Table
def putTableIntoHBase(df, givenCatalog):
    df.write.format("org.apache.hadoop.hbase.spark") \
        .options(catalog=givenCatalog, newTable=5) \
        .option("hbase.spark.use.hbasecontext", False) \
        .save()

    print("Added " + json.loads(givenCatalog)["table"]["name"] + " to HBase!")


# Using the data that we just pulled from HBase, here's a sample model on it
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
    # input = input.select("Temperature","Humidity","Light","CO2", "HumidityRatio", "Occupancy")
    # target_columns = ["prediction"]
    return model.transform(input).select(target_columns)


def createBatchScoreTable():
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
    return fullouter


def classifyBatchScoreTable(model, batchScoreDF):
    withPredictions = classify(batchScoreDF, actualModel)
    withPredictions = withPredictions.withColumn("Key", concat(withPredictions["Temperature"], lit(','),
                                                               withPredictions["Humidity"], lit(','),
                                                               withPredictions["Light"], lit(','),
                                                               withPredictions["CO2"], lit(','),
                                                               withPredictions["HumidityRatio"]))
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
                     "Occupancy":{"cf":"weather", "col":"Occupancy", "type":"double"}
                   }
                 }""".split())
    return catalog


def getBatchScoreTableCatalog():
    BatchScoreTableCatalog = ''.join("""{
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
    return BatchScoreTableCatalog


# Main Method
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("HBase and PySpark Model Demonstration") \
        .getOrCreate()

    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    # grabs TrainingData from HBase
    trainDataCtlg = getTrainingDataCatalog()
    trainingData = loadTableFromHBase(trainDataCtlg)

    # dropping key column since we dont need it for the model
    trainingData = trainingData.drop('Key')

    # builds and saves the model
    actualModel = build_model(trainingData)
    target_path = "hdfs:///tmp/spark-model"
    actualModel.write().overwrite().save(target_path)

    #creates, scores, and stores the batch score table
    batchTable = createBatchScoreTable()
    scoredBatchTable = classifyBatchScoreTable(actualModel, batchTable)
    putTableIntoHBase(scoredBatchTable, getBatchScoreTableCatalog())

