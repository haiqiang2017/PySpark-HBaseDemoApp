# PySpark-HBaseDemoApp

Make sure PySpark and HBase are configured - For reference look at Part 1
Make a new project on CDSW and select “Git” under the “Initial Setup” section
Use “https://github.com/mchakka/PySpark-HBaseDemoApp.git” for the Git URL

Create a new session with Python3
Run preprocessing.py on your CDSW project
This will put all training data into HBase
Upload and Run main.py on your CDSW project
Creates the model
Builds and Scores batch score table
Stores batch score table in HBase
Upload and Run app.py on your CDSW project
In order to view the web application, go to http://<$CDSW_ENGINE_ID>.<$CDSW_DOMAIN>
