import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
"""
    Custom Operator to check for data quality
    
    :redshift_conn_id  Redshift Connection Id Object
    :check_list:       An array of data quality checks and expected outputs match.
"""    

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 check_list="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.check_list = check_list
        self.redshift_conn_id = redshift_conn_id

#    data_quality_checks = [ 
#    { check_type = 'info','test_sql': "select count(*) from users"},
#    {check_type = 'check','test_sql': "select count(*) from time where weekday is null, 'expected_result': 0, #'comparison':'='}
#]
    
        
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for item in self.check_list:
            sql_statement = item['test_sql']
            logging.info(f"SQL Statement: {sql_statement}")
            records = redshift_hook.get_records(f"{sql_statement}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {sql_statement} returned no results")
            num_records = {records[0][0]}     
            if item['check_type'] == 'info':
                logging.info(f"***INFO***-> {sql_statement} -> num_records.")
            else:
                check_statement = "{} {} {}".format(num_records,item['comparison'],item['expected_result'])
                logging.info(f"***check_statement***-> {check_statement}.")
                if not check_statement:
                    raise ValueError(f"Data quality check failed.")
