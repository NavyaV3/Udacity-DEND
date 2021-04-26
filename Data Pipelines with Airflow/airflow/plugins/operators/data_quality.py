from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Custom operator that runs data quality checks by receiving test cases (SQL queries) and expected results
    
    :param redshift_conn_id: Redshift connection ID
    :param test_case: list of SQL queries to run on Redshift data warehouse
    :param expected_result: List of expected results to match against results of test_case respectively
    
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 test_case=[],
                 expected_result=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_case=test_case
        self.expected_result=expected_result
        
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for i,test in enumerate(self.test_case):
            check = redshift.get_records(test)
            if check[0][0] != self.expected_result[i]:
                raise ValueError(f"{test} returned result {check[0][0]} which does not match expected value {self.expected_result[i]}. Data quality check failed.")
            else:
                self.log.info(f"{test} result matches expected result {self.expected_result[i]}. Data quality check for NULL values passed.")
        self.log.info(f"Data Quality checks complete.")
         