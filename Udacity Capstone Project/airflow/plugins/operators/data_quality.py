from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import DataChecks

class DataQualityOperator(BaseOperator):
    """
    Custom operator that runs data quality checks by receiving test cases (SQL queries) and expected results
    
    :param redshift_conn_id: Redshift connection ID
    :param tables: list of tables to run check on in the Redshift data warehouse
    
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 tables=['immigration','demographics','temperature','airport','city'],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables=tables
                
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for i,table in enumerate(self.tables):
            # Checking the record count for tables
            count_check = redshift.get_records("SELECT COUNT(*) FROM {}".format(self.tables[i]))
            if count_check[0][0] == 0:
                raise ValueError(f"Count of records for table {table} is zero.Data quality check failed.")
            else:
                self.log.info(f"Data quality check for count of records of table {table} passed.")
             
            # Checking for NULL values in the tables
            if table == 'immigration':
                null_check = redshift.get_records(DataChecks.immigration)
            elif table == 'demographics':
                null_check = redshift.get_records(DataChecks.demographics)
            elif table == 'temperature':
                null_check = redshift.get_records(DataChecks.temperature)
            elif table == 'airport':
                null_check = redshift.get_records(DataChecks.airport)
            elif table == 'region':
                null_check = redshift.get_records(DataChecks.region)
            
            if null_check[0][0] != 0:
                raise ValueError(f"Data quality check for NULL values in table {table} failed.")
            else:
                self.log.info(f"Data quality check for NULL values passed for table {table}.")
                
        self.log.info(f"Data Quality checks complete.")
         