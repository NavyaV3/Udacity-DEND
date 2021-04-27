import os
import pyspark
from pyspark.sql import SparkSession
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable

# Getting AWS credentials and file path information
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

def process_etl_usdemog(spark):
    """
    Function to load data from dataset, process it and write to parquet files
    """
    # Reading US city demographic data
    usdem_df = spark.read.options(header='True', inferSchema='True', delimiter=';').csv(Inpath + "us-cities-demographics.csv")
    
    # Transforming the data
    usdem_df = usdem_df.drop_duplicates()
    
    grpcol = ("City","State","Median Age","Male Population","Female Population",
              "Total Population","Number of Veterans","Foreign-born","Average Household Size","State Code")
    
    usdem_df = usdem_df.groupby(*grpcol).pivot("Race").sum("Count")
    
    # Cleaning the data
    usdem_df = usdem_df.dropna(how='any')
    usdem_df = usdem_df.drop_duplicates()
    
    # create view of demographics data to extract using SQL queries
    usdem_df.createOrReplaceTempView("us_demo_data")
    
    # extract columns to create staging files
    stg_usdem = spark.sql("""
                        SELECT DISTINCT 
                        City AS city,
                        State AS state,
                        `Median Age` AS median_age,
                        `Male Population` AS male_pop,
                        `Female Population` AS female_pop,
                        `Total Population` AS tot_pop,
                        `Number of Veterans` AS num_veterans,
                        `Foreign-born` AS foreign_born,
                        `Average Household Size` AS avg_household_size,
                        `American Indian and Alaska Native` AS num_american_indian,
                        Asian AS num_asian,
                        `Black or African-American` AS num_black,
                        `Hispanic or Latino` AS num_hispanic,
                        White AS num_white
                        FROM us_demo_data""")

#     stg_usdem.show(5)

    stg_usdem.write.parquet(Outpath + "/demographics",mode = 'overwrite')
    
def main():
    
    spark = create_spark_session()
    
    process_etl_usdemog(spark)    
        
    print("US City Demographics ETL pipeline completed")
    

if __name__ == "__main__":
    main()





    