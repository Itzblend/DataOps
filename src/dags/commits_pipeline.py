from datetime import datetime, timedelta
import os
from airflow.models import Variable

os.environ["DATAOPS_BOB_VAULT_PASS"] = Variable.get("DATAOPS_BOB_VAULT_PASS")
os.environ["VAULT_ADDR"] = Variable.get("VAULT_ADDR")
os.environ["github_token"] = Variable.get("github_token")
os.environ["DATAOPS_CODE_ENV"] = Variable.get("DATAOPS_CODE_ENV", "DEV")

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'magalorian',
    'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
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

with DAG(
        'github_commits_data_pipeline',
        default_args=default_args,
        description='Fetch and load github issues to datalake',
        schedule_interval=timedelta(minutes=5),
        start_date=datetime(2021, 2, 7),
        catchup=False,
        tags=['ETL'],
) as dag:
    t1 = BashOperator(
        task_id='fetch_commit',
        bash_command='cd /opt/airflow/dags/Dataops && python3 main.py fetch-commits --database github'
    )

    t2 = BashOperator(
        task_id='load_commits',
        bash_command='cd /opt/airflow/dags/Dataops && python3 main.py load-github-commits --database github'
    )

    t1 >> t2
