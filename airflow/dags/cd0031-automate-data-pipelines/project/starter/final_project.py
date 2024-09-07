from datetime import datetime, timedelta
import pendulum
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements

#BG_TZ = pendulum.timezone('Europe')
default_args = {
    'owner': 'udacity',
    'depends on past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'email_on_retry': False,
}

dag = DAG('final_project_legacy',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False
        )

# Define tasks

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    schema='public',
    table='staging_events',
    create_table_sql=final_project_sql_statements.staging_events_table_create,
    s3_bucket='airflow-udacity2024',
    s3_key='log-data/2018/11/',
    file_format='JSON',
    redshift_conn_id='redshift',
    aws_credential_id='awsuser',
    copy_options=("'s3://airflow-udacity2024/log_json_path.json'",),
    region='us-east-1',
    dag=dag
    )

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    schema='public',
    table='staging_songs',
    create_table_sql=final_project_sql_statements.staging_songs_table_create,
    s3_bucket='airflow-udacity2024',
    s3_key='song-data/',
    file_format='JSON',
    redshift_conn_id='redshift',
    aws_credential_id='awsuser',
    copy_options=("'auto'",),
    region='us-east-1',
    dag=dag
    )

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    table='songplays',
    create_table_sql=final_project_sql_statements.songplay_table_create,
    sql_query=final_project_sql_statements.songplay_table_insert,
    append_data=False,
    dag=dag
    )

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    table='songs',
    create_table_sql=final_project_sql_statements.song_table_create,
    sql_query=final_project_sql_statements.song_table_insert,
    append_data=False,
    dag=dag
    )

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    table='users',
    create_table_sql=final_project_sql_statements.user_table_create,
    sql_query=final_project_sql_statements.user_table_insert,
    append_data=False,
    dag=dag
    )

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    table='artists',
    create_table_sql=final_project_sql_statements.artist_table_create,
    sql_query=final_project_sql_statements.artist_table_insert,
    append_data=False,
    dag=dag
    )

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    table='time',
    create_table_sql=final_project_sql_statements.time_table_create,
    sql_query=final_project_sql_statements.time_table_insert,
    append_data=False,
    dag=dag
    )

run_data_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    tables=['songplays','songs','users','artists','time'],
    checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE user_id IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE song_id IS NULL", 'expected_result': 0}
    ],
    dag=dag
    )

end_operator = DummyOperator(task_id='End_execution',  dag=dag)

# Additional logging for debugging
stage_songs_to_redshift.log.info(f"Schema: {stage_songs_to_redshift.schema}")
stage_songs_to_redshift.log.info(f"Table: {stage_songs_to_redshift.table}")
stage_songs_to_redshift.log.info(f"S3 Path: s3://{stage_songs_to_redshift.s3_bucket}/{stage_songs_to_redshift.s3_key}")

#start_operator >> [songplay_table_create, song_table_create, user_table_create, artist_table_create, time_table_create] 

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table >> run_data_checks >> end_operator
load_songplays_table >> load_user_dimension_table >> run_data_checks >> end_operator
load_songplays_table >> load_artist_dimension_table >> run_data_checks >> end_operator
load_songplays_table >> load_time_dimension_table >> run_data_checks >> end_operator
