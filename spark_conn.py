import os

import pandas.core.frame
import pyspark.sql
from pyspark.sql import SparkSession
import datetime
import pandas as pd

# import org.apache.spark.sql.types.IntegerType
from nltk.probability import FreqDist
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import split, concat, col, lit, count, percentile_approx, mean, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# local import
import config as cfg

DATA_TIME = datetime.datetime.now()
APP_NAME = 'PySparkShell'


def read_data_spark(csv_file: str):
    spark = SparkSession.builder.master("local[1]").appName(APP_NAME).getOrCreate()
    df = spark.read.csv(csv_file, header=True)
    return df


def write_data_csv(data_frame: DataFrame, csv_file_name: str):
    clean_csv = 'csv_data/clean_' + csv_file_name.split('/')[1].split('.')[0]
    data_frame.coalesce(1).write.options(header='True', delimiter=',').csv(clean_csv)


def clean_offense_codes_df():
    data_frame = read_data_spark(csv_file=cfg.CSV_OFFENSE_CODES)
    df = data_frame.withColumn('crime_type', split(col('name'), ' - ').getItem(0)) \
        .distinct() \
        .withColumn('code', col('code').cast('int'))
    print(f'''
    INFO: {DATA_TIME}: >> Clean up data count in DataFrame <{cfg.CSV_OFFENSE_CODES.split('/')[1].split('.')[0]}> is :
    ''', df.count())
    return df


def clean_crime_df():
    data_frame = read_data_spark(csv_file=cfg.CSV_CRIME)
    df = data_frame \
        .distinct() \
        .withColumn('offense_code', col('offense_code').cast('int')) \
        .withColumn('year_month_date', concat(col('YEAR'), lit('-'), col('MONTH')))
    print(f'''
       INFO: {DATA_TIME}: >> Clean up data count in DataFrame <{cfg.CSV_CRIME.split('/')[1].split('.')[0]}> is :
       ''', df.count())
    return df


def join_df_func(df_left, df_right, join_col_left: str, join_col_right: str, join_type: str):
    try:
        return df_left.join(df_right, df_left[join_col_left] == df_right[join_col_right], join_type)
    except Exception as e:
        print(f'[INFO] : {DATA_TIME} >> Error reading : ', e)


def join_df():
    # looks not good, but working
    try:
        df_crime = clean_crime_df()
        df_offense_codes = clean_offense_codes_df()
        df = df_crime.join(df_offense_codes,
                           df_crime['OFFENSE_CODE'] == df_offense_codes['code'],
                           'inner')
        return df
    except Exception as e:
        print(f'[INFO] : {DATA_TIME} >> Error reading : ', e)


def df_with_crime_rate(df: pyspark.sql.DataFrame, rate: int):
    df = df.toPandas()
    df = df.groupby(by='DISTRICT', as_index=False, dropna=False).agg({'crime_type': lambda x: FreqDist(list(x)).most_common(rate)})
    print(f'DF_TO_PANDAS GROUPBY "DISTRICT":\n{df}')
    return df


def pandas_to_spark(pandas_df: pandas.core.frame.DataFrame):
    spark = SparkSession.builder.master("local[1]").appName(APP_NAME).getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark_df = spark.createDataFrame(pandas_df)
    return spark_df


def aggregate_df():
    # add columns for aggregate table
    df = join_df().withColumns({'counter': lit(1),
                                'crimes_total': lit(1),
                                'crimes_monthly_count': lit(1),
                                'crimes_monthly': lit(1),
                                'crime_type_count': lit(1),
                                'frequent_crime_types': lit('-')})
    print(f'JOiN WITH NEW COLUMNS TAB:\n{df.show(1)}')
    df = df.select('DISTRICT',
                   'year_month_date',
                   'crimes_total',
                   'crimes_monthly',
                   'frequent_crime_types',
                   'crime_type',
                   'crime_type_count',
                   'Lat',
                   'Long', )
    print(f'SELECT TAB:\n{df.show(1)}')

    # left dataframe for join
    df_ = df.select('DISTRICT',
                    'year_month_date',
                    'crimes_total',
                    'crimes_monthly',
                    'frequent_crime_types',
                    'crime_type',
                    'crime_type_count',
                    'Lat',
                    'Long', ) \
        .groupBy('DISTRICT', 'year_month_date', 'crime_type') \
        .agg(count('crimes_total').alias('crimes_total'),
             count('crimes_monthly').alias('crimes_monthly'),
             count('crime_type').alias('crime_type_count'),
             mean('Lat').alias('Lat'),
             mean('Long').alias('Long'),
             ) \
        .withColumn('frequent_crime_types', lit('-'))
    print(f'GROUP BY TAB_1:\n{df_.show(1)}')

    df_ = df_.select('DISTRICT',
                     'year_month_date',
                     'crimes_total',
                     'crimes_monthly',
                     'frequent_crime_types',
                     'crime_type',
                     'crime_type_count',
                     'Lat',
                     'Long', ) \
        .groupBy(['DISTRICT']) \
        .agg(count('crimes_total').alias('crimes_total'),
             count('crime_type'),
             percentile_approx(col='crimes_monthly', percentage=0.5, accuracy=10000).alias('crimes_monthly'),
             mean('Lat').alias('Lat'),
             mean('Long').alias('Long'), ).drop('count(crime_type)')
    print(f'DF_ FOR JOIN:\n'
          f'{df_.show(5)}\n'
          f'DF_ COUNT : {df_.count()}\n'
          f'DF_ FOR JOIN TYPE:\n'
          f'{type(df_)}')

    # convert pyspark.DataFrame to pandas.DataFrame to group dataframe be DISTRICT
    # and apply for any value in column list of values with crime_types
    df_freq = df_with_crime_rate(df=df, rate=3).astype({'DISTRICT': str, 'crime_type': str})
    df_freq = pandas_to_spark(df_freq)
    df_freq = df_freq.withColumnRenamed('DISTRICT', 'DIST')

    df_agg = join_df_func(df_left=df_,
                          df_right=df_freq,
                          join_col_left='DISTRICT',
                          join_col_right='DIST',
                          join_type='right').drop('DIST')
    # method below calls exception
    # df_agg = df_agg.withColumn('crime_type', regexp_replace(col('crime_type'), "(", ""))
    print(f'AGGREGATE TAB: \n{df_agg.show()}')
    return df_agg


def save_df_to_parquet(df: pyspark.sql.dataframe.DataFrame):
    df.write.parquet('parquet_data/aggregate_table')

# read_data_spark(cfg.CSV_OFFENSE_CODES)
# read_data_spark(cfg.CSV_CRIME)
# clean_offense_codes_df()
# clean_crime_df()
# join_df()
# print(join_df().show(3, truncate=False))
# aggregate_df()
save_df_to_parquet(aggregate_df())


if __name__ == '__main__':
    print('')
