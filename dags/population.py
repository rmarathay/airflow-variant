"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.s3_key_sensor import S3KeySensor
from airflow.contrib.hooks.aws_hook import AwsHook
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 11, 24),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("population", default_args=default_args, schedule_interval=timedelta(1))


# Task1: Wait for file to appear in S3 bucket (SensorOperator)
#   Only want this to run if there is a file, but we want it to go ahead 
# Task2: population_manager.py                (LambdaOperator)
# Task3: population_companies.py              (LambdaOperator)
# Task4: population_commands.py               (LambdaOperator)


t1 = S3KeySensor(
    task_id="s3_file_sensor",
    poke_interval=60*60,
    bucket_key="s3://cdp-fs-airflow/to_do/company_info_input.tsv",
    dag=dag)


t2 = AW(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag)


t3 = BashOperator(
    task_id="templated",
    bash_command=templated_command,
    params={"my_param": "Parameter I passed in"},
    dag=dag,
)

t2.set_upstream(t1)
t3.set_upstream(t1)
