from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,DataQualityNullCheckOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Vipul Mahiman',
    'start_date': datetime.utcnow(),
    #'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'max_active_runs': 1,
    'email': ['vipul.mahiman@gmail.com'],
    'email_on_retry': False
}

dag = DAG('sparkify_etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          template_searchpath = ['/home/workspace/airflow'],
          schedule_interval= '@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql="create_tables.sql",
    postgres_conn_id="redshift"
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    #aws_credentials_id="aws_credentials",
    aws_iam_role="arn:aws:iam::611617861465:role/dwhRole",
    s3_bucket="udacity-dend",
    s3_key="log-data",
    json="s3://udacity-dend/log_json_path.json"
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    #aws_credentials_id="aws_credentials",
    aws_iam_role="arn:aws:iam::611617861465:role/dwhRole",
    s3_bucket="udacity-dend",
    s3_key="song-data"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql = SqlQueries.user_table_insert,
    to_truncate="true"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql = SqlQueries.song_table_insert,
    to_truncate="true"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.artists",
    sql = SqlQueries.artist_table_insert,
    to_truncate="true"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql = SqlQueries.time_table_insert,
    to_truncate="true"
)

# Following is an additional operator created to log the record counts of table list passed

data_quality_checks = [ 
    { 'check_type' : 'info','test_sql': "select count(*) from users"},
    {'check_type' : 'check','test_sql': "select count(*) from time where weekday is null", 'expected_result': 0, 'comparison':'='}
]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    check_list=data_quality_checks
)

# Following is a configurable information on which table columns to check for null values

table_null_check =	{
  "time": "weekday",
  "users": "first_name",
  "songs": "title"
}


#run_quality_null_checks = DataQualityNullCheckOperator(
 #   task_id='Run_data_quality_null_checks',
  #  dag=dag,
   # redshift_conn_id="redshift",
    #table_check=table_null_check
#)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



#start_operator >> create_tables_task >> [ stage_events_to_redshift , stage_songs_to_redshift ] >> load_songplays_table >> \
#[ load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table ] >>  \
#run_count_checks >> run_quality_null_checks >> end_operator


start_operator.set_downstream(create_tables_task)
create_tables_task.set_downstream([stage_events_to_redshift , stage_songs_to_redshift])
load_songplays_table.set_upstream([stage_events_to_redshift , stage_songs_to_redshift])
load_songplays_table.set_downstream([load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table])
run_quality_checks.set_upstream([load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table])
#run_count_checks.set_downstream(run_quality_null_checks)
run_quality_checks.set_downstream(end_operator)




