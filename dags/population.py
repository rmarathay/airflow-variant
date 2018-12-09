from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 12, 7),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill', 
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("population",
        default_args=default_args,
        schedule_interval="@hourly"
    )


# Task1: Wait for file to appear in S3 bucket (SensorOperator)
#   Only want this to run if there is a file, but we want it to go ahead 
# Task2: population_manager.py                (LambdaOperator)
# Task3: population_companies.py              (LambdaOperator)
# Task4: population_commands.py               (LambdaOperator)

# t1 = BashOperator(
#     task_id="change_permissions1",
#     bash_command="chmod -R a+x /usr/local/pipeline-variant/",
#     dag=dag
#     )

# t1 = BashOperator(
#     task_id="change_permissions2",
#     bash_command="chmod 777 /usr/local/pipeline-variant/population/top_level_domain_input.tsv",
#     dag=dag
#     )

t2 = BashOperator(
    task_id="run_manager",
    bash_command="python3 /usr/local/pipeline-variant/population/population_manager.py staging",
    run_as_user="airflow",
    dag=dag
    )

t3 = BashOperator(
    task_id="run_population_companies",
    bash_command="python3 /usr/local/pipeline-variant/population/population_companies.py /usr/local/pipeline-variant/population/company_info_input.tsv staging",
    dag=dag
    )

t4 = BashOperator(
    task_id="run_population_commands",
    bash_command="python3 /usr/local/pipeline-variant/population/population_commands.py /usr/local/pipeline-variant/population/top_level_domain_input.tsv staging",
    dag=dag
    )
# t0.set_downstream(t1)
# t1.set_downstream(t2)
t2.set_downstream(t3)
t3.set_downstream(t4)

