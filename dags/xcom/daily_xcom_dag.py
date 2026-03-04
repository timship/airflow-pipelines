from datetime import timedelta, date
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
import requests

# Аргументы по умолчанию
default_args = {
    "owner": "marija-shkurat-wrn7887",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="marija-shkurat-wrn7887_4",
    default_args=default_args,
    description="DAG4, unit 3, homework",
    schedule_interval="@daily",
    start_date=days_ago(2),
    concurrency=1,
    tags=["marija-shkurat-wrn7887"],
)
def task_flow():
    
    @task()
    def get_info_count():
        url = 'https://rickandmortyapi.com/api/character/'
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        return data['info']['count']


    get_info_count()

dag = task_flow()
