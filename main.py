from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.conf import SparkConf
import pandas
_conf = SparkConf()

import config

builder = SparkSession. \
    builder. \
    config(conf=_conf)
session = builder.getOrCreate()
session.sparkContext.setLogLevel("ERROR")
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
  
class oracle_db:
    def __init__(self,host,port,sid,user_name,password):
        self.host=host
        self.port=port
        self.sid=sid
        self.user_name=user_name
        self.password=password
    
    def execute(self,sql_statement,rows):
        import cx_Oracle
        dsn_tns = cx_Oracle.makedsn(self.host,self.port,self.sid)
        conn = cx_Oracle.connect(user=self.user_name, password=self.password, dsn=dsn_tns)
        c = conn.cursor()

        c.executemany(sql_statement,rows)
        conn.commit()
        conn.close()


def string_manipulation(s, suffix):
    if suffix and s.endswith(suffix):
        return s[:-len(suffix)]
    return s

class sql_statement_maker:

    def __init__(self):
        pass

    def upsert(self,df,table_name: str,list_of_keys: list):
        
        columns=list(df)

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


def SaveToOracle(df,epoch_id):
    try:
        print("***********Start*************")
        pandasDF = df.toPandas()
        rows= pandasDF.to_dict(orient='records')
        
        table_name=config.table_name
        list_of_keys=config.list_of_keys


        sql_statement_maker_obj=sql_statement_maker()
        sql_statement=sql_statement_maker_obj.upsert(pandasDF,table_name,list_of_keys)

        host=config.host
        port=config.port
        user_name=config.user_name
        password=config.password
        sid=config.sid
        oracle_db_obj=oracle_db(host,port,sid,user_name,password)
        oracle_db_obj.execute(sql_statement,rows)

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
                 

