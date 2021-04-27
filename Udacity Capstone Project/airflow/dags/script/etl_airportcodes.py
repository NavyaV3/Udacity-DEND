# Do all imports and installs here
import os
import pandas as pd
from pyspark.sql.functions import udf,substring
from datetime import datetime, timedelta
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable

#Getting AWS credentials and file path information
aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()
os.environ['AWS_ACCESS_KEY_ID']=credentials.access_key
os.environ['AWS_SECRET_ACCESS_KEY' ]=credentials.secret_key

Inpath=Variable.get("INPUT")
Outpath = Variable.get("OUT")


def create_spark_session():
    """
    Create and run a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport().getOrCreate()
    return spark

def process_etl_airport(spark):
    """
    Function to load data from dataset, process it and write to parquet files
    """
    # Reading airport codes data
    airport_df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(Inpath+"airport-codes_csv.csv")
    
    # Cleaning data
    airport_df = airport_df.filter(airport_df.iso_country == 'US')
    
    airport_df = airport_df.dropna(how='any',subset = ['ident','type','name','iso_region','municipality','iata_code'])
    airport_df = airport_df.drop_duplicates()
    
    airport_df = airport_df.withColumn("state_cd",substring("iso_region",4,5))
    
    # create view of airport data to extract using SQL queries
    airport_df.createOrReplaceTempView("airport_data")

    # extract columns to create staging airport table
    stg_airport = spark.sql("""
                        SELECT DISTINCT 
                        ident AS airport_id,
                        municipality AS city,
                        state_cd AS state_cd,
                        name AS airport_name,
                        type AS airport_type,
                        iata_code AS iata_cd
                        FROM airport_data""")
    
#     stg_airport.show(5)
    
    stg_airport.write.parquet(Outpath + "/airport",mode = 'overwrite')
    
def main():
    
    spark = create_spark_session()
    
    process_etl_airport(spark)    
        
    print("Airport Codes ETL pipeline completed")
    

if __name__ == "__main__":
    main()



