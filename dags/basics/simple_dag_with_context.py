from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

<<<<<<< HEAD
=======

# Вывод "Hello, World!"
def print_hello_func():
    return "Hello, World, it's second project!"

>>>>>>> f3d3057e1734a1490799ceab40a01d4fcfdab451
# Аргументы по умолчанию
default_args = {
    "owner": "marija-shkurat-wrn7887",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Использование конструкции `with`
with DAG(
<<<<<<< HEAD
    "theory_v2",
    default_args=default_args,
    description="DAG, version 2",
    schedule_interval="@daily",
    start_date=days_ago(2),
    tags=["theory"],
) as dag:
    # Таск 1: Вывод текущей даты
    print_date = BashOperator(
        task_id="print_date",
        bash_command="date"
    )

    # Таск 2: Ожидание 5 секунд
    sleep = BashOperator(
        task_id="sleep",
        bash_command="sleep 5",
        retries=3
    )

    # Таск 3: Вывод "Hello, World!"
    def print_hello_func():
        print("Hello, World!")
=======
    "marija-shkurat-wrn7887_2",
    default_args=default_args,
    description="DAG, project 2",
    schedule_interval="@daily",
    start_date=days_ago(2),
    tags=["marija-shkurat-wrn7887"],
) as dag:
>>>>>>> f3d3057e1734a1490799ceab40a01d4fcfdab451

    print_hello = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello_func
    )

<<<<<<< HEAD
    # Установка зависимостей
    print_date >> [sleep, print_hello]
=======
    print_hello
>>>>>>> f3d3057e1734a1490799ceab40a01d4fcfdab451
