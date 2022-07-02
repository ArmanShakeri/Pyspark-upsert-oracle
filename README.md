# About
Pyspark sample code for upsert data to oracle table with jdbs connection

# How to run
pip install -r requirement.txt

- download compatible jdbc driver with spark
- download and install oracle client
- update variables: fileschema,input_path,table_name,host,port,user_name,password,sid
- input list of key columns for upsert
- submit job: spark-submit --jars /path to file/ojdbc8.jar main.py