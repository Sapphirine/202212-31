from datetime import datetime, timedelta
from textwrap import dedent
import time
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago


####################################################
# DEFINE PYTHON FUNCTIONS
####################################################


PROJECT_ID = ''  #Enter your project id
CLUSTER_NAME = ''  #Enter your cluster name
BUCKET_NAME = ''  #Enter your bucket name
REGION = 'us-central1'
PYSPARK_FILE_PREPROCESS = 'preprocess_yt_data.py'
PYSPARK_FILE_LR = 'lr_yt_data.py'
PYSPARK_FILE_DT = 'dt_yt_data.py'
PYSPARK_FILE_RF = 'rf_yt_data.py'
PYSPARK_FILE_NB = 'nb_yt_data.py'
PYSPARK_FILE_FINAL = 'final_model_yt_data.py'
PYSPARK_FILE_VIDS = 'videos_scraper.py'
PYSPARK_FILE_COMMENTS = 'comments_scraper.py'

PYSPARK_JOB_VIDS = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME}/code/{PYSPARK_FILE_VIDS}"},
}

PYSPARK_JOB_COMMENTS = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME}/code/{PYSPARK_FILE_COMMENTS}"},
}


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
    'bda_yt_data',
    default_args=default_args,
    description='DAG for YT data',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['vs2279','smc2306'],
) as dag:

##########################################
# DEFINE AIRFLOW OPERATORS
##########################################

    # t* examples of tasks created by instantiating operators
    t1_videos = DataprocSubmitJobOperator(
        task_id="t1_videos",
        job=PYSPARK_JOB_VIDS,
        region=REGION,
        project_id=PROJECT_ID
    )

    t2_comments = DataprocSubmitJobOperator(
        task_id="t2_comments",
        job=PYSPARK_JOB_COMMENTS,
        region=REGION,
        project_id=PROJECT_ID
    )

    t3_pp = DataprocSubmitJobOperator(
        task_id="t3_preprocess",
        job=PYSPARK_JOB_PREPROCESS,
        region=REGION,
        project_id=PROJECT_ID
    )

    t4_lr = DataprocSubmitJobOperator(
        task_id="t4_lr",
        job=PYSPARK_JOB_LR,
        region=REGION,
        project_id=PROJECT_ID
    )

    t4_dt = DataprocSubmitJobOperator(
        task_id="t4_dt",
        job=PYSPARK_JOB_DT,
        region=REGION,
        project_id=PROJECT_ID
    )

    t4_rf = DataprocSubmitJobOperator(
        task_id="t4_rf",
        job=PYSPARK_JOB_RF,
        region=REGION,
        project_id=PROJECT_ID
    )

    t4_nb = DataprocSubmitJobOperator(
        task_id="t4_nb",
        job=PYSPARK_JOB_NB,
        region=REGION,
        project_id=PROJECT_ID
    )

    t5_final = DataprocSubmitJobOperator(
        task_id="t5_final",
        job=PYSPARK_JOB_FINAL,
        region=REGION,
        project_id=PROJECT_ID
    )
    

##########################################
# DEFINE TASKS HIERARCHY
##########################################

    # task dependencies 
    t1_videos >> t2_comments
    t2_comments >> t3_pp
    t3_pp >> [t4_lr, t4_dt]
    t4_dt >> t4_rf
    t4_lr >> t4_nb
    [t4_rf, t4_nb] >> t5_final
