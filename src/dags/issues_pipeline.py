from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'magalorian',
    'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

def _success_criteria(record):
    return record

def _failure_criteria(record):
    return True if not record else False

with DAG(
    'Github issues data pipeline',
    default_args=default_args,
    description='Fetch and load github issues to datalake',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2021, 2, 7),
    catchup=False,
    tags=['ETL'],
) as dag:



    t1 = BashOperator(
        task_id='load_issues',
        bash_command='cd ../.. && python3 main.py fetch-issues'
    )

    t2 = BashOperator(
        task_id='load_issues',
        bash_command='cd ../.. && python3 main.py load-issues'
    )

    t1 >> t2