import os
from pyspark.sql.functions import udf
from datetime import datetime, timedelta
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable

###Getting AWS credentials and file path information
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
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport().getOrCreate()
    return spark

def process_etl_immig(spark):
    """
    Function to load data from dataset, process it and write to parquet files
    """
    # Reading Immigration data
    immig_df = spark.read.format('com.github.saurfang.sas.spark').load(Inpath + 'i94_apr16_sub.sas7bdat')
    #immig_df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(Inpath + "immigration_data_apr16.csv")

    # Reading supporting documents for immigration data
    i94res_df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(Inpath + "i94res_country_codes_immigration.csv")
    i94res_df = i94res_df.drop("_c3")
    i94res_df = i94res_df.dropna(how='any',subset = ['i94res','Country']).drop_duplicates()


    i94port_df = spark.read.options(header='True', inferSchema='True', delimiter=';').csv(Inpath + "i94port_city_codes_immigration.csv")
    i94port_df = i94port_df.drop("_c3")
    i94port_df = i94port_df.dropna(how='any',subset = ['i94port','City','State_CD']).drop_duplicates()
    
    # Cleaning the data
    immig_df = immig_df.dropna(how='any',subset=['cicid','i94res','i94port','arrdate','i94addr','i94bir','gender','visatype'])
    immig_df = immig_df.drop_duplicates()
    
    get_date_sas = udf(lambda x: (datetime(1960, 1, 1) + timedelta(days=int(x))), T.DateType())
    
    immig_df = immig_df.withColumn("arrival_date",get_date_sas(immig_df.arrdate))
    
    # create view of immigration and supporting data to extract using SQL queries
    immig_df.createOrReplaceTempView("immigration_data")
    
    i94res_df.createOrReplaceTempView("country_data")
    
    i94port_df.createOrReplaceTempView("port_data")
    
    # extract columns to create staging immigration table
    stg_immig = spark.sql("""SELECT DISTINCT 
                                CAST(id.cicid AS INT) AS ID,
                                INITCAP(cd.Country) AS origin_country,
                                INITCAP(pd.City) AS city,
                                id.i94addr AS state_cd,
                                id.arrival_date,
                                CAST(id.i94bir AS INT) AS age,
                                id.gender AS gender,
                                id.visatype AS visa_type
                          FROM immigration_data id 
                          JOIN country_data cd ON id.i94res = cd.i94res
                          JOIN port_data pd ON id.i94port = pd.i94port AND id.i94addr = pd.State_CD
                          WHERE city IS NOT NULL
                             """)
    
#     stg_immig.show(25)
    
    stg_immig.write.parquet(Outpath + "/immigration",mode = 'overwrite')

def main():
    
    spark = create_spark_session()    
    
    process_etl_immig(spark)    
        
    print("ETL pipeline completed")
    
    
if __name__ == "__main__":
    main()



