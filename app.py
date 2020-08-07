from flask import Flask, jsonify, render_template, request
from pyspark.sql import SparkSession
import os
import logging

log = logging.getLogger('werkzeug')
log.setLevel(logging.DEBUG)

MASTER = 'local'
APPNAME = 'simple-ml-serving'
MODEL_PATH = 'hdfs:///tmp/spark-model'

spark = SparkSession\
    .builder\
    .appName("WebApp!")\
    .getOrCreate()


#Catalogs
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



# webapp
app = Flask(__name__)


def grabPredictionFromBatchScoreTable(keyToUse, modelResultsCatalog):
  try:
      statement ="SELECT * FROM sampleView WHERE Key = '"+keyToUse +"'"
      result = spark.sql(statement)
      t =result.collect()[0]["Prediction"]
      return t
  except:
      return "I do not have a prediction"
  


def addToTrainingTable(key, prediction):
  #Making the row to add to the Training Table
  splitKey = key.split(',')
  splitKey = [float(i) for i in splitKey]
  
  splitKey.insert(0, key)
  splitKey.append(int(prediction))
  
  listToConvert = [tuple(splitKey)]
  
  listOfColumns = ['Key', 'Temperature', 'Humidity', 'Light', "CO2", 'HumidityRatio', "Occupancy"]
  data = spark.createDataFrame(listToConvert, listOfColumns)
  
  data.write.format("org.apache.hadoop.hbase.spark") \
    .options(catalog=getTrainingDataCatalog(), newTable = 5) \
    .option("hbase.spark.use.hbasecontext", False) \
    .save()
  
  data.show()
  print("This is now added to HBase Training Data Table")
      


@app.route('/api', methods=['POST', 'GET'])
def predict():
  keyToUse = request.form['temp6']

  output = grabPredictionFromBatchScoreTable(keyToUse, getBatchScoreTableCatalog())

  if request.form['status'] == "Added":
    addToTrainingTable(request.form['temp6'], output)
  
  if output == 1:
    output = "Occupied"
  elif output == 0:
    output = "Not Occupied"
    
  return render_template("index.html", 
                         output=output, 
                         temp=request.form['temp'],
                        temp2=request.form['temp2'],
                        temp3=request.form['temp3'],
                        temp4=request.form['temp4'],
                        temp5=request.form['temp5'],
                        temp6=request.form['temp6'],
                        status=request.form['status'])

@app.route('/')
def main():
    output = 'No Inputs Yet'
    return render_template("index.html", output=output)


if __name__ == '__main__':
  df = spark.read.format("org.apache.hadoop.hbase.spark") \
    .options(catalog=getBatchScoreTableCatalog()) \
    .option("hbase.spark.use.hbasecontext", False) \
    .load()
  df.createOrReplaceTempView("sampleView")
  
  app.run(port=os.environ["CDSW_APP_PORT"])
    
    
    
# curl -v -H "Accept: application/json" -H "Content-type: application/json" -X POST -d '{"Temperature":23.7,"Humidity":26.272,"Light":585.2,"CO2":749.2,"HumidityRatio":0.00476416302416414}' http://localhost:8100/api/predict