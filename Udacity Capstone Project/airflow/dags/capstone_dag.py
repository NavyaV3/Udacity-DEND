from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import CopyToRedshiftOperator, DataQualityOperator
from helpers import DataChecks
from airflow.models import Variable

### Uncomment the below code to get the variables
#S3_bucket = Variable.get("S3_bucket")
#S3_key = Variable.get("S3_key")

# Setting default parameters
default_args = {
    'owner': 'udacity_nv',
    'start_date': datetime(2021, 4, 7),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Defining the DAG
dag = DAG('udac_capstone_dag',
          default_args=default_args,
          description='Transform data in S3 and load to Redshift with Airflow',
          schedule_interval='@monthly',
          max_active_runs=1,
          catchup=False
        )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Task to create tables in Redshift
create_tables_task = PostgresOperator(
    task_id="create_tables",
    sql='capstone_create_tables.sql',
    postgres_conn_id="redshift",
    dag=dag
)

# Initial processing of datasets and loading cleansed data to S3

S3_immig_task = BashOperator(
    task_id='load_to_S3_immig',
    bash_command='python /home/workspace/airflow/dags/script/etl_immig.py',
    dag=dag)


S3_temp_task = BashOperator(
    task_id='load_to_S3_temp',
    bash_command='python /home/workspace/airflow/dags/script/etl_temp.py',
    dag=dag)


S3_us_demog_task = BashOperator(
    task_id='load_to_S3_usdem',
    bash_command='python /home/workspace/airflow/dags/script/etl_demog.py',
    dag=dag)


S3_airport_task = BashOperator(
    task_id='load_to_S3_airport',
    bash_command='python /home/workspace/airflow/dags/script/etl_airportcodes.py',
    dag=dag)


S3_city_task = BashOperator(
    task_id='load_to_S3_cities',
    bash_command='python /home/workspace/airflow/dags/script/etl_cities.py',
    dag=dag)

load_S3 = DummyOperator(task_id='Load_to_S3_completed',  dag=dag)

# Tasks to load data from S3 parquet files to Redshift tables
copy_immig_to_redshift = CopyToRedshiftOperator(
    task_id='load_to_Redshift_immig',
    dag=dag,
    table="immigration",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=S3_bucket,
    s3_key=S3_key + "/immigration",
    append_data = True
    )
    
copy_temperature_to_redshift = CopyToRedshiftOperator(
    task_id='load_to_Redshift_temperature',
    dag=dag,
    table="temperature",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=S3_bucket,
    s3_key=S3_key+"/temperature",
    append_data = True    
    )

copy_us_demog_to_redshift = CopyToRedshiftOperator(
    task_id='load_to_Redshift_demographics',
    dag=dag,
    table="demographics",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=S3_bucket,
    s3_key=S3_key+"/demographics",
    append_data = True
    )

copy_airport_codes_to_redshift = CopyToRedshiftOperator(
    task_id='load_to_Redshift_airport_codes',
    dag=dag,
    table="airport",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=S3_bucket,
    s3_key=S3_key+"/airport",
    append_data = True
    )

copy_cities_to_redshift = CopyToRedshiftOperator(
    task_id='load_to_Redshift_region',
    dag=dag,
    table="region",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=S3_bucket,
    s3_key=S3_key+"/city",
    append_data = True
    )

# Task to run data quality checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=['immigration','demographics','temperature','airport','region']
   
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Defining task dependencies
start_operator>> create_tables_task >> [S3_immig_task, S3_temp_task, S3_us_demog_task, S3_airport_task, S3_city_task] >>load_S3

load_S3 >> copy_immig_to_redshift 
load_S3 >> copy_temperature_to_redshift 
load_S3 >> copy_us_demog_to_redshift 
load_S3 >> copy_airport_codes_to_redshift 
load_S3 >> copy_cities_to_redshift

[copy_immig_to_redshift, copy_temperature_to_redshift, copy_us_demog_to_redshift, copy_airport_codes_to_redshift, copy_cities_to_redshift] >> run_quality_checks

run_quality_checks >> end_operator