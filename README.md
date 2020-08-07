# PySpark-HBaseDemoApp

## What Is This Repo?

This project consists of example code of how to perform operations (put, get, scan) in PySpark on HBase Tables.
Examples are in the `code-examples` folder of this repository

## How To Run on CDSW


1. Make sure PySpark and HBase are configured - For reference look at Part 1
   - To make sure all these steps are completed, Python3 should installed and the PySpark Enviornment Variables must be set correctly
   - HBase has the right bindings by adding the jar paths to the RegionServer Enviornment Variable through Cloudera Manager (CM)
   - spark-defaults.conf is already included in this repo, double check the path to the "hbase-connectors" jars is correct

2. Make a new project on CDSW and select “Git” under the “Initial Setup” section
   - Use “https://github.com/mchakka/PySpark-HBaseDemoApp.git” for the Git URL

3. Create a new session with Python3

4. Run preprocessing.py on your CDSW project
   - This will put all training data into HBase
   
5. Upload and Run main.py on your CDSW project
   - Creates the model
   - Builds and Scores batch score table
   - Stores batch score table in HBase
   
6. Upload and Run app.py on your CDSW project
   - In order to view the web application, go to http://<$CDSW_ENGINE_ID>.<$CDSW_DOMAIN>
