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

# Inpath = "/home/workspace/"

def create_spark_session():
    """
    Create and run a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport().getOrCreate()
    return spark

def process_etl_temp(spark):
    """
    Function to load data from dataset, process it and write to parquet files
    """
    
    # Reading temperature data
    temp_df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(Inpath + "GlobalLandTemperaturesByCity.csv")
    
    # Cleaning data
    temp_df = temp_df.filter(temp_df.Country == 'United States')
    temp_df = temp_df.dropna(how='any',subset = ['AverageTemperature','City'] )
    temp_df = temp_df.drop_duplicates()
    
    # create view of temperature data to extract using SQL queries
    temp_df.createOrReplaceTempView('temp_data')
    
    # extract columns to create staging temperature table
    stg_temp = spark.sql("""SELECT DISTINCT     
                                ROW_NUMBER() OVER(ORDER BY dt,City) AS temp_id,
                                City AS city,
                                AverageTemperature AS avg_temp,
                                year(dt) AS year,
                                month(dt) AS month
                                FROM temp_data
                                """)
    
#     stg_temp.show(10)
       
    stg_temp.write.parquet(Outpath+"/temperature",mode = 'overwrite')
    
def main():
    
    spark = create_spark_session()
        
    process_etl_temp(spark)    
        
    print("Temperature ETL pipeline completed")
   
if __name__ == "__main__":
    main()



