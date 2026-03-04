from datetime import timedelta, date
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

# Аргументы по умолчанию
default_args = {
    "owner": "marija-shkurat-wrn7887",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="marija-shkurat-wrn7887_3",
    default_args=default_args,
    description="DAG, version 3",
    schedule_interval="@daily",
    start_date=days_ago(2),
    tags=["marija-shkurat-wrn7887"],
)
def task_flow():
    @task()
    def get_date():
        return str(date.today())

    @task()
    def get_summ():
        return 8 + 23

    @task()
    def print_date_sum(some_day: str, some_num: int):
        print(f"Today is {some_day}, the number is {some_num}")

    print_date_sum(get_date(), get_summ())

def task_flow():
    @task()
    def print_hello():
        return "Hello, World, it's third project!"

    print_hello()

dag = task_flow()

