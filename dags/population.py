"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "cdp_admin",
    "depends_on_past": False,
    "start_date": datetime(2018, 11, 24),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    'queue': 'bash_queue',
    'pool': 'backfill',
    'schedule_interval' : None,
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("population", default_args=default_args)


# Task1: Wait for file to appear in S3 bucket (SensorOperator)
#   Only want this to run if there is a file, but we want it to go ahead 
# Task2: population_manager.py                (LambdaOperator)
# Task3: population_companies.py              (LambdaOperator)
# Task4: population_commands.py               (LambdaOperator)


t1 = BashOperator(
    task_id="run_manager",
    #bash_command="python3 /srv/etl/pipeline-variant/population/population_manager.py staging ",
    bash_command="python3 /Users/rmarathay/code/variant/pipeline-variant/population/population_manager.py staging",
    dag=dag
    )

t2 = BashOperator(
    task_id="run_population_companies",
    #bash_command="python3 /srv/etl/pipeline-variant/population/population_companies.py company_info_input.tsv staging",
    bash_command="python3 /Users/rmarathay/code/variant/pipeline-variant/population/population_companies.py company_info_input.tsv staging",
    dag=dag
    )

t3 = BashOperator(
    task_id="run_population_commands",
    #bash_command="python3 /srv/etl/pipeline-variant/population/population_commands.py top_level_domain_input.tsv staging",
    bash_command="python3 /Users/rmarathay/code/variant/pipeline-variant/population/population_commands.py top_level_domain_input.tsv staging",
    dag=dag
    )

t1.set_downstream(t2)
t2.set_downstream(t3)

