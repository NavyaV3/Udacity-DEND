from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Custom operator to load data from staging tables to fact tables in Redshift
    
    :param redshift_conn_id: Redshift connection ID
    :param table: Target table to be loaded in Redshift
    :param sqlqry: SQL query to be executed to load data into target table
    
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table="",
                 sqlqry="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sqlqry = sqlqry

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
#         self.log.info("Clearing data from destination Fact table")
#         redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info('Loading Fact tables.')
        redshift.run("INSERT INTO {} {}".format(self.table,self.sqlqry))
        
