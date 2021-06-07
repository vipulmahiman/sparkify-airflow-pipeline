from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries


class LoadFactOperator(BaseOperator):
"""
    Custom Operator to load from staging to Fact table in Redshift
    
    :redshift_conn_id  Redshift Connection Id Object
    :table:            Redshift staging table.
    :sql:              Sql for selection from staging table
"""    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql="",
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql_insert_stmt = f"INSERT INTO {self.table} {self.sql}"
        self.log.info("Inser table statement: " + sql_insert_stmt)
        self.log.info(f"Inserting data in Redshift {self.table} Fact table")

        redshift.run(sql_insert_stmt)
