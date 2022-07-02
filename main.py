from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.conf import SparkConf
_conf = SparkConf()

import config

builder = SparkSession. \
    builder. \
    config(conf=_conf)
session = builder.getOrCreate()

fileschema=(StructType()
            .add(StructField("MSISDN",StringType()))
            .add(StructField("CDR_TYPE",StringType()))
            .add(StructField("DURATION",IntegerType()))

            )

input_path=config.input_path

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
        import cx_Oracle
        table_name=config.table_name

        def sql_statement_maker(df,table_name: str,list_of_keys: list):
            import pandas as pd

            def string_manipulation(s, suffix):
                if suffix and s.endswith(suffix):
                    return s[:-len(suffix)]
                return s

            columns=list(df)
            list_of_keys=list_of_keys

            table_name=table_name
            sql_statement="MERGE INTO {} USING DUAL ON (".format(table_name)

            if len(list_of_keys)==1:
                sql_statement+="{item}=:{item}".format(item=list_of_keys[0])
            elif len(list_of_keys)==0:
                print("please input key columns in list_of_keys varaible!")
            else:
                i=0
                for item in list_of_keys:
                    
                    if i==0:
                        sql_statement+="{item}=:{item}".format(item=item)
                    else:
                        sql_statement+=" AND {item}=:{item}".format(item=item)
                    i+=1
            sql_statement+=""")
            WHEN NOT MATCHED THEN INSERT(
            """
            str_values=""
            for item in columns:
                sql_statement+="{},".format(item)
                str_values+=":{},".format(item)

            sql_statement=string_manipulation(sql_statement,',')
            str_values=string_manipulation(str_values,',')
            sql_statement+=") VALUES ("+str_values+") WHEN MATCHED THEN UPDATE SET"

            value_columns = [item for item in columns if item not in list_of_keys]

            for item in value_columns:
                sql_statement+=" {item}=:{item},".format(item=item)


            sql_statement=string_manipulation(sql_statement,',')

            return sql_statement

        list_of_keys=config.list_of_keys
        sql_statement=sql_statement_maker(pandasDF,table_name,list_of_keys)
        host=config.host
        port=config.port
        user_name=config.user_name
        password=config.password
        sid=config.sid
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
                 

