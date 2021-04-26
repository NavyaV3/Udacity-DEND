from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Custom operator to load data from staging tables to dimension tables in Redshift
    
    :param redshift_conn_id: Redshift connection ID
    :param table: Target table to be loaded in Redshift
    :param sqlqry: SQL query to be executed to load data into target table
    :param append_data: value that decides between append and truncate-insert functionality.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table="",
                 sqlqry="",
                 append_data=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sqlqry = sqlqry
        self.append_data=append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append_data:
            self.log.info("Clearing data from destination dimension table")
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info('Loading Dim tables.')
        redshift.run("INSERT INTO {} {}".format(self.table,self.sqlqry))
