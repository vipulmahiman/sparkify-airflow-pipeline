from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries as s

class LoadDimensionOperator(BaseOperator):
"""
    Custom Operator to load from staging to dimension table in Redshift
    
    :redshift_conn_id  Redshift Connection Id Object
    :table:            Redshift staging table.
    :sql:              Sql for selection from staging table
"""    

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 to_truncate="false",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.to_truncate = to_truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        sql_insert_stmt = f"INSERT INTO {self.table} {self.sql}"
        self.log.info("Inser table statement: " + sql_insert_stmt)
        self.log.info(f"Inserting data in Redshift {self.table} Dimension table")
        
        if self.to_truncate:
            redshift.run(f"DELETE FROM {self.table}") 
        redshift.run(sql_insert_stmt)
        
        