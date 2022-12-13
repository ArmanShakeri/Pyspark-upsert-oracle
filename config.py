import os
from dotenv import load_dotenv

load_dotenv()

input_path=os.environ.get("input_path","hdfs://172.17.135.31:9000/input/")
table_name=os.environ.get("table_name","RESULT_TABLE")
list_of_keys=os.environ.get("list_of_keys",["MSISDN","CDR_TYPE"])
host=os.environ.get("host","172.17.135.46")
port=os.environ.get("port","1521")
user_name=os.environ.get("user_name","system")
password=os.environ.get("password","hadoop")
sid=os.environ.get("sid","orcl")