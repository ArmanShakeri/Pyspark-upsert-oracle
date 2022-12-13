![GitHub last commit](https://img.shields.io/github/last-commit/ArmanShakeri/Pyspark-upsert-oracle)
[![GitHub license](https://img.shields.io/github/license/ArmanShakeri/Pyspark-upsert-oracle)](https://github.com/ArmanShakeri/Pyspark-upsert-oracle/blob/master/LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/ArmanShakeri/Pyspark-upsert-oracle)](https://github.com/ArmanShakeri/Pyspark-upsert-oracle/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/ArmanShakeri/Pyspark-upsert-oracle)](https://github.com/ArmanShakeri/Pyspark-upsert-oracle/network)
[![GitHub issues](https://img.shields.io/github/issues/ArmanShakeri/Pyspark-upsert-oracle)](https://github.com/ArmanShakeri/Pyspark-upsert-oracle/issues)

# About
Pyspark sample code for upsert data to oracle table with jdbs connection

# How to run
pip install -r requirement.txt

- download compatible jdbc driver with spark
- download and install oracle client
- update variables: fileschema,input_path,table_name,host,port,user_name,password,sid
- input list of key columns for upsert
- submit job: spark-submit --jars /path to file/ojdbc8.jar main.py
