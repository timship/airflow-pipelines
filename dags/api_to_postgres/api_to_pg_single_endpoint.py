from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import requests
from psycopg2 import extras
import time

default_args = {
    "owner": "marija-shkurat-wrn7887",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False
}

with DAG(
        "marija-shkurat-wrn7887_5",
        default_args=default_args,
        description='Homework Airflow DAG to fetch data from PostgreSQL, Rick&Morty',
        schedule_interval="@daily",
        start_date=days_ago(2),
        max_active_runs=1,
        tags=['marija-shkurat-wrn7887'],
) as dag:

    create_table_sql = """
            CREATE TABLE IF NOT EXISTS marija_shkurat_wrn7887.chars (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            status VARCHAR(50),
            species VARCHAR(100),
            type VARCHAR(100),
	        gender VARCHAR(50)
        )
    """


    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="marija-shkurat-wrn7887_pg",
        sql=create_table_sql
    )

    truncate_table = PostgresOperator(
        task_id="truncate_table",
        postgres_conn_id="marija-shkurat-wrn7887_pg",
        sql="TRUNCATE TABLE marija_shkurat_wrn7887.chars;"
    )


    def fetch_characters_json():
        url = 'https://rickandmortyapi.com/api/character/'
        all_results = []

        session = requests.Session()

        while url:
            for i in range(5):  # повторная попытка на случай 429
                response = session.get(url, timeout=30)
                if response.status_code == 429:
                    time.sleep(5)
                    continue
                response.raise_for_status()
                break

            data = response.json()
            for char in data["results"]:
                all_results.append((
                    char["id"],
                    char["name"],
                    char["status"],
                    char["species"],
                    char["type"],
                    char["gender"]
                ))

            url = data["info"]["next"]
            time.sleep(0.3)

        #Получение соединения из Airflow
        pg_hook = PostgresHook(postgres_conn_id="marija-shkurat-wrn7887_pg")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        #Вставка данных в таблицу одним запросом
        insert_query = """
        INSERT INTO marija_shkurat_wrn7887.chars (id, name, status, species, type, gender) 
        VALUES %s 
        ON CONFLICT (id) DO NOTHING"""

        #Используем psycopg2.extras для вставки нескольких строк
        extras.execute_values(cursor, insert_query, all_results, page_size=100)


        #Сохранение изменений и закрытие соединения
        conn.commit()
        cursor.close()
        conn.close()
        print("Данные успешно сохранены в базу данных")


    save_chars = PythonOperator(
        task_id="save_chars",
        python_callable=fetch_characters_json
    )

    create_table >> truncate_table >> save_chars
