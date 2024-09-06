from datetime import datetime, timedelta
import pendulum
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),}

dag = DAG('final_project_legacy',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

def final_project_dag():
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        schema='dev',
        table='staging_songs',
        #s3_bucket='airflow-udacity2024',
        s3_key='s3://airflow-udacity2024/song-data/',
        redshift_conn_id='redshift',
        aws_credential_id='awsuser',
        copy_options=["FORMAT as JSON 'auto'"],
        dag=dag
    )

    ##stage_events_to_redshift >> 
    final_project_sql_statements.staging_songs_table_create >> stage_songs_to_redshift

final_udacity_project_dag = final_project_dag()