from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator

from operators import (
    StageToRedshiftOperator,
    DataQualityOperator
)


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# default arguments for dag
default_args = {
    'owner': 'yp',
    'start_date': datetime(2018, 11, 1, 0, 0, 0, 0),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

# Dag to load, tranform and quality control data in Redshift with Airflow
dag = DAG('immigration_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# task to create tables for US immigration data
create_table = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql'
)


# # Task to load reference data in Redshift

load_reference_data_csv_to_redshift = StageToRedshiftOperator(
    task_id='Load_reference_data',
    dag=dag,
    table_list=["DIM_I94_PORT","DIM_I94_MODE","DIM_I94_VISA","DIM_I94_ADDR","DIM_I94_CIT_RES"],
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="dataengineer",
    s3_key="reference_data",
    file_format='csv',
    file_list=["I94PORT.csv","I94MODE.csv","I94VISA.csv","I94ADDR.csv","I94CITRES.csv"]
)

load_temperature_parquet_to_redshift = StageToRedshiftOperator(
    task_id='Load_temperature_data',
    dag=dag,
    table_list=["DIM_TEMPERATURE"],
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="dataengineer",
    s3_key="temperature",
    file_format='parquet',
    file_list=[""]
)

load_demographics_parquet_to_redshift = StageToRedshiftOperator(
    task_id='Load_demographics_data',
    dag=dag,
    table_list=["DIM_DEMOGRAPHICS"],
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="dataengineer",
    s3_key="demographics",
    file_format='parquet',
    file_list=[""]
)

load_fact_immi_parquet_to_redshift = StageToRedshiftOperator(
    task_id='Load_immigration_data',
    dag=dag,
    table_list=["FACT_IMMIGRATION"],
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="dataengineer",
    s3_key="immigration",
    file_format='parquet',
    file_list=[""]
)

# Task to perform quality checks on the data uploaded in Redshift
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    qa_check_list = [
{'check_sql': "SELECT COUNT(*) FROM DIM_I94_PORT WHERE poe_code is null", 'expected_result': 0},
{'check_sql': "SELECT COUNT(*) FROM DIM_I94_VISA", 'expected_result': 3},
{'check_sql': "SELECT COUNT(*) FROM DIM_I94_MODE", 'expected_result': 4},
{'check_sql': "SELECT COUNT(*) FROM DIM_I94_ADDR WHERE state_code is null", 'expected_result': 0},
{'check_sql': "SELECT COUNT(*) FROM DIM_I94_CIT_RES WHERE cit_res_id is null", 'expected_result': 0},
{'check_sql': "SELECT COUNT(*) FROM DIM_TEMPERATURE WHERE state_code is null", 'expected_result': 0},
{'check_sql': "SELECT COUNT(*) FROM DIM_DEMOGRAPHICS WHERE state_code is null", 'expected_result': 0},
{'check_sql': "SELECT COUNT(*) FROM FACT_IMMIGRATION", 'expected_result': 40790529}
],
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# order of execution for the dag
start_operator >> create_table 
create_table >> load_reference_data_csv_to_redshift
load_reference_data_csv_to_redshift >> [load_temperature_parquet_to_redshift, load_demographics_parquet_to_redshift]
[load_temperature_parquet_to_redshift, load_demographics_parquet_to_redshift] >> load_fact_immi_parquet_to_redshift
load_fact_immi_parquet_to_redshift >> run_quality_checks
run_quality_checks >> end_operator

