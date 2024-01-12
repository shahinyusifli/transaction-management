from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
import json

###############################################
# Parameters
###############################################

config_file = "/usr/local/spark/resources/json/config.json"

###############################################
# config loader and callback func
###############################################

with open(config_file, 'r') as f:
    config = json.load(f)

###############################################
# DAG Definition
###############################################
now = datetime.now()
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

#define the dag
dag = DAG(
    dag_id="transaction-management-task",
    description="This dag does very simple etl with the help of docker, pyspark and postgres.",
    default_args=default_args,
    schedule_interval=timedelta(1)
)

start = DummyOperator(task_id="start", dag=dag)

# running the spark job with spark submit operator
spark_job_extract = SparkSubmitOperator(
    task_id="spark_extract_job",
    application="/usr/local/spark/src/app/extract_app.py",
    name="extract_app",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": config['spark_master']},
    py_files='/usr/local/spark/src/app/extract_job.py',
    application_args=[config_file],
    jars=config['postgres_driver_jar'],
    driver_class_path=config['postgres_driver_jar'],
    dag=dag)

# running the spark job with spark submit operator
spark_job_transform = SparkSubmitOperator(
    task_id="spark_transform_job",
    application="/usr/local/spark/src/app/transform_app.py",
    name="transform_app",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": config['spark_master']},
    py_files='/usr/local/spark/src/app/transform_job.py',
    application_args=[config_file],
    jars=config['postgres_driver_jar'],
    driver_class_path=config['postgres_driver_jar'],
    dag=dag)

# running the spark job with spark submit operator
spark_job_to_datamart = SparkSubmitOperator(
    task_id="spark_to_datamart_job",
    application="/usr/local/spark/src/app/to_datamart_app.py",
    name="to_datamart_app",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": config['spark_master']},
    py_files='/usr/local/spark/src/app/to_datamart_job.py',
    application_args=[config_file],
    jars=config['postgres_driver_jar'],
    driver_class_path=config['postgres_driver_jar'],
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

# create the dependency chain
start >> spark_job_extract >> spark_job_transform >> spark_job_to_datamart >> end
