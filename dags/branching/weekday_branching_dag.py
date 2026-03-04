from datetime import timedelta
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import datetime

def only_weekdays(**context):
    execution_date = context['execution_date']

    if execution_date.weekday() < 5:
        return 'do_work'
    return None

def push_execution_date(**context):
    return context['ds']

default_args = {
    "owner": "marija-shkurat-wrn7887",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "concurrency": 1
}

with DAG(
        "marija-shkurat-wrn7887_7",
        default_args=default_args,
        description='Final Airflow DAG',
        schedule_interval="@daily",
        start_date=days_ago(7),
        tags=['marija-shkurat-wrn7887'],
        catchup=True,
) as dag:

    branch = BranchPythonOperator(
        task_id='check_weekday',
        python_callable=only_weekdays,
    )

    do_work = PythonOperator(
        task_id='do_work',
        python_callable=push_execution_date
    )


    branch >> do_work
