# Do all imports and installs here
import os
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

def process_etl_city(spark):
    """
    Function to load data from dataset, process it and write to parquet files
    """
    # Reading US cities data
    city_df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(Inpath + "uscities.csv")
    
    # Cleaning data
    city_df = city_df.dropna(how='any')
    city_df = city_df.drop_duplicates()
    
    # create view of city data to extract using SQL queries
    city_df.createOrReplaceTempView('city_data')
    
    # extract columns to create staging cities files
    stg_city = spark.sql('''
                        SELECT DISTINCT 
                        id AS city_id,
                        city AS city,
                        state_name AS state,
                        state_id AS state_cd,
                        county_name AS county,
                        'United States' AS country,
                        lat AS latitude,
                        lng AS longitude
                        FROM city_data
                        ''')
    
#     stg_city.show(5)
       
    stg_city.write.parquet(Outpath+"/city",mode = 'overwrite')
    
def main():
    
    spark = create_spark_session()
        
    process_etl_city(spark)    
        
    print("US Cities ETL pipeline completed")
   
if __name__ == "__main__":
    main()



