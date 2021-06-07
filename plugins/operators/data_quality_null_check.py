import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityNullCheckOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_check="",
                 *args, **kwargs):

        super(DataQualityNullCheckOperator, self).__init__(*args, **kwargs)
        self.table_check = table_check
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table,col in self.table_check.items():
            logging.info(f"Data Null Check on table {table} & Column {col} Checking")
            sql_statement = f"SELECT COUNT(1) FROM {table} where {col} is null"
            records = redshift_hook.get_records(sql_statement)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results for {col}")
            num_records = records[0][0]
            if num_records > 1:
                logging.info(f"Data Null check failed. {table}.{col} contained {num_records} rows")
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
