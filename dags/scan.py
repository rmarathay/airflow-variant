from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import psycopg2


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 12, 7),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG("population",
        default_args=default_args,
        schedule_interval="@daily"
    )

t1 = BashOperator(
    task_id="run_manager",
    bash_command="python3 /usr/local/pipeline-variant/scan/fetch_subdomains.py staging",
    run_as_user="airflow",
    dag=dag
    )