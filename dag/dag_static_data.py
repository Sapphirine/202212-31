from datetime import datetime, timedelta
from textwrap import dedent
import time
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago


PROJECT_ID = ''  #Enter your project id
CLUSTER_NAME = ''  #Enter your cluster name
BUCKET_NAME = ''  #Enter your bucket name
REGION = 'us-central1'
PYSPARK_FILE_PREPROCESS = 'preprocess_static_data.py'
PYSPARK_FILE_LR = 'lr_static_data.py'
PYSPARK_FILE_DT = 'dt_static_data.py'
PYSPARK_FILE_RF = 'rf_static_data.py'
PYSPARK_FILE_NB = 'nb_static_data.py'
PYSPARK_FILE_FINAL = 'final_model_static_data.py'


PYSPARK_JOB_PREPROCESS = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME}/code/{PYSPARK_FILE_PREPROCESS}",
                    "jar_file_uris":[f"gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar"]},
}

PYSPARK_JOB_LR = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME}/code/{PYSPARK_FILE_LR}",
                    "jar_file_uris":[f"gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar"]},
}

PYSPARK_JOB_DT = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME}/code/{PYSPARK_FILE_DT}",
                    "jar_file_uris":[f"gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar"]},
}

PYSPARK_JOB_RF = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME}/code/{PYSPARK_FILE_RF}",
                    "jar_file_uris":[f"gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar"]},
}

PYSPARK_JOB_NB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME}/code/{PYSPARK_FILE_NB}",
                    "jar_file_uris":[f"gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar"]},
}

PYSPARK_JOB_FINAL = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME}/code/{PYSPARK_FILE_FINAL}",
                    "jar_file_uris":[f"gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar"]},
}


############################################
# DEFINE AIRFLOW DAG (SETTINGS + SCHEDULE)
############################################

# change user details if needed
default_args = {
    'owner': 'vinays',
    'depends_on_past': False,
    'email': ['vs2779@columbia.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30),
    "start_date": days_ago(1),
}

with DAG(
    'bda_static_data',
    default_args=default_args,
    description='DAG for static data',
    schedule_interval='@once',
    catchup=False,
    tags=['vs2279','smc2306'],
) as dag:

##########################################
# DEFINE AIRFLOW OPERATORS
##########################################

    t1_pp = DataprocSubmitJobOperator(
        task_id="t1_preprocess",
        job=PYSPARK_JOB_PREPROCESS,
        region=REGION,
        project_id=PROJECT_ID
    )

    t2_lr = DataprocSubmitJobOperator(
        task_id="t2_lr",
        job=PYSPARK_JOB_LR,
        region=REGION,
        project_id=PROJECT_ID
    )

    t2_dt = DataprocSubmitJobOperator(
        task_id="t2_dt",
        job=PYSPARK_JOB_DT,
        region=REGION,
        project_id=PROJECT_ID
    )

    t2_rf = DataprocSubmitJobOperator(
        task_id="t2_rf",
        job=PYSPARK_JOB_RF,
        region=REGION,
        project_id=PROJECT_ID
    )

    t2_nb = DataprocSubmitJobOperator(
        task_id="t2_nb",
        job=PYSPARK_JOB_NB,
        region=REGION,
        project_id=PROJECT_ID
    )

    t3_final = DataprocSubmitJobOperator(
        task_id="t3_final",
        job=PYSPARK_JOB_FINAL,
        region=REGION,
        project_id=PROJECT_ID
    )
    

##########################################
# DEFINE TASKS HIERARCHY
##########################################

    # task dependencies 
    t1_pp >> [t2_lr, t2_dt]
    t2_lr >> t2_rf
    t2_dt >> t2_nb
    [t2_rf, t2_nb] >> t3_final
