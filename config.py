
'''
API tocken to kaggle
{"username":"dmitrysolonnikov","key":"033712ebfab777c58b6711801711ba4c"}
'''
KAGGLE_USERNAME = 'dmitrysolonnikov'
KAGGLE_KEY = 'fb88c25fc9afec68cfc52f2a174f0197'
CSV_API_CRIME = 'crime.csv'
CSV_API_OFFENSE_CODE = 'offense_codes.csv'

'''
Connect Spark from remote PySpark
You can also verify the Spark cluster by starting a connection from PySpark

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("spark://62.84.122.34:7077").getOrCreate()
print("spark session created")
'''

# DB connect
HOST = 'rc1b-gi39w1dnxk5qhkag.mdb.yandexcloud.net'
PORT = '6432'
SSLMODE = 'verify-full'
DBNAME = 'analytics'
USER = 'otus-psql'
PASSWORD = 'g-am#-c-SUW'
sslrootcert ='root@ainflow:/home/airflow-user/.postgresql/root.crt'
TAB_CRIME = 'crime'
TAB_OFFENSE_CODES = 'offense_codes'

# SPARK CONNECT
LOCALHOST = 'http://127.0.0.1'

# csv data
CSV_CRIME = "csv_data/crime.csv"
CSV_OFFENSE_CODES = "csv_data/offense_codes.csv"
