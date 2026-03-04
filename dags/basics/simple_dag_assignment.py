from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Вывод "Hello, World!"
def print_hello_func():
    return "Hello, World, it's first project!"


# Аргументы по умолчанию
default_args = {
    "owner": "marija-shkurat-wrn7887",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Описание DAG
dag = DAG(
    "theory_v1",
    default_args=default_args,
    description="DAG, version 1",
    schedule_interval="@daily",
    start_date=days_ago(2),
    tags=["theory"],
  
)

# Таск 1: Вывод текущей даты
print_date = BashOperator(
    task_id="print_date",
    bash_command="date",
    dag=dag
)

# Таск 2: Ожидание 5 секунд
sleep = BashOperator(
    task_id="sleep",
    bash_command="sleep 5",
    retries=3,
    dag=dag
)

# Таск 3: Вывод "Hello, World!"
def print_hello_func():
    print("Hello, World!")

print_hello = PythonOperator(
    task_id="print_hello",
    python_callable=print_hello_func,
    dag=dag
)

# Установка зависимостей
sleep.set_upstream(print_date)
print_hello.set_upstream(print_date)

