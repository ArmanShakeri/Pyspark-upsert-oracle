from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.conf import SparkConf
_conf = SparkConf()

_conf.set("spark.streaming.concurrentJobs","30")

builder = SparkSession. \
    builder. \
    config(conf=_conf)
session = builder.getOrCreate()

fileschema=(StructType()
            .add(StructField("MSISDN",StringType()))
            .add(StructField("CDR_TYPE",StringType()))
            .add(StructField("DURATION",IntegerType()))

            )

input_path='hdfs://172.17.135.31:9000/input/'

df=(session
  .readStream
  .format("csv")
  .option("sep", "|")
  .option("header", "false")
  .option("maxFilesPerTrigger", 20)
  .schema(fileschema)
  .load(input_path)
        )
  


def SaveToOracle(df,epoch_id):
    try:
        print("***********Start*************")
        pandasDF = df.toPandas()
        rows= pandasDF.to_dict(orient='records')
        print(rows)
        import cx_Oracle
        sql_statement="""
                        MERGE INTO RESULT_TABLE USING DUAL
                        ON (MSISDN=:MSISDN AND CDR_TYPE=:CDR_TYPE)                    
                        WHEN NOT MATCHED THEN INSERT 
                            (MSISDN,
                            CDR_TYPE,
                            DURATION
                            
                            ) 
                        VALUES
                            (:MSISDN,
                            :CDR_TYPE,
                            :DURATION
                            ) 
                        WHEN MATCHED THEN UPDATE SET
                            DURATION=:DURATION
                        """


        host='172.17.135.46'
        port='1521'
        user_name='system'
        password='hadoop'
        sid='orcl'
        dsn_tns = cx_Oracle.makedsn(host,port, sid=sid)
        conn = cx_Oracle.connect(user=user_name, password=password, dsn=dsn_tns)
        c = conn.cursor()

        c.executemany(sql_statement,rows)
        conn.commit()
        conn.close()
        pass
    except Exception as e:
        response = e.__str__()
        print(response)
        
        



streamingQuery = (df.writeStream
  .outputMode("append")
  .foreachBatch(SaveToOracle)
  .start()
  .awaitTermination()
                 )
                 

