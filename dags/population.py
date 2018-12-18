from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 12, 17),
    "email": ["rmarathay@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG("population",
        default_args=default_args,
        schedule_interval=None
    )


# Task1: Wait for file to appear in S3 bucket (SensorOperator)
#   Only want this to run if there is a file, but we want it to go ahead 
# Task2: population_manager.py                (LambdaOperator)
# Task3: population_companies.py              (LambdaOperator)
# Task4: population_commands.py               (LambdaOperator)

t1 = BashOperator(
    task_id="run_manager",
    bash_command="python3 /usr/local/pipeline-variant/population/population_manager.py staging",
    run_as_user="airflow",
    dag=dag
    )

t2 = BashOperator(
    task_id="run_population_companies",
    bash_command="python3 /usr/local/pipeline-variant/population/population_companies.py /usr/local/pipeline-variant/population/company_info_input.tsv staging",
    run_as_user="airflow",
    dag=dag
    )

t3 = BashOperator(
    task_id="run_population_commands",
    bash_command="python3 /usr/local/pipeline-variant/population/population_commands.py /usr/local/pipeline-variant/population/top_level_domain_input.tsv staging",
    run_as_user="airflow",
    dag=dag
    )


t1.set_downstream(t2)
t2.set_downstream(t3)

