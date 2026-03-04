from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

import requests
from psycopg2 import extras

default_args = {
    "owner": "marija-shkurat-wrn7887",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False
}

with DAG(
        "marija-shkurat-wrn7887_6",
        default_args=default_args,
        description='Homework Airflow DAG to fetch data from PostgreSQL, Rick&Morty',
        schedule_interval="@daily",
        start_date=days_ago(2),
        max_active_runs=1,
        max_active_tasks=3,
        tags=['marija-shkurat-wrn7887'],
) as dag:

    create_table1_sql = """
            CREATE TABLE IF NOT EXISTS marija_shkurat_wrn7887.chars (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            status VARCHAR(50),
            species VARCHAR(100),
            type VARCHAR(100),
	        gender VARCHAR(50)
        )
    """
    create_table2_sql = """
                CREATE TABLE IF NOT EXISTS marija_shkurat_wrn7887.locs (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                type VARCHAR(100),
                dimension VARCHAR(100)
            )
        """
    create_table3_sql = """
                CREATE TABLE IF NOT EXISTS marija_shkurat_wrn7887.eps (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                air_date VARCHAR(50),
                episode VARCHAR(10)
            )
        """

    table_config = {
        'chars' : {
            'fields': ['id', 'name', 'status', 'species', 'type', 'gender'],
            'json_mapper': lambda item: (
                item['id'],
                item['name'],
                item['status'],
                item['species'],
                item['type'],
                item['gender'],
            )
        },
        'locs' : {
            'fields': ['id', 'name', 'type', 'dimension'],
            'json_mapper': lambda item: (
                item['id'],
                item['name'],
                item['type'],
                item['dimension'],
            )
        },
        'eps': {
            'fields': ['id', 'name', 'air_date', 'episode'],
            'json_mapper': lambda item: (
                item['id'],
                item['name'],
                item['air_date'],
                item['episode'],
            )
        },
    }

    def fetch_and_save_rm_data(url, table_name):
        if table_name not in table_config:
            raise ValueError(f"Unknown table {table_name}")

        config = table_config[table_name]
        all_results = []

        while url:
            session = requests.Session()
            response = session.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data['results']:
                all_results.append(config['json_mapper'](item))

            url = data["info"]["next"]  # переход на следующую страницу

        if not all_results:
            return

        #Получение соединения из Airflow
        pg_hook = PostgresHook(postgres_conn_id="marija-shkurat-wrn7887_pg")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        columns = ', '.join(config['fields'])
        #Вставка данных в таблицу одним запросом
        insert_query = f"""
        INSERT INTO marija_shkurat_wrn7887.{table_name} ({columns}) 
        VALUES %s
        ON CONFLICT (id) DO NOTHING"""

        #Используем psycopg2.extras для вставки нескольких строк
        extras.execute_values(cursor, insert_query, all_results)


        #Сохранение изменений и закрытие соединения
        conn.commit()
        cursor.close()
        conn.close()
        print("Данные успешно сохранены в базу данных")

    rm_list = [
        {
            'name': 'chars_gr',
            'url': 'https://rickandmortyapi.com/api/character/',
            'table_name': 'chars',
            'ddl': create_table1_sql,},
        {
            'name': 'locs_gr',
            'url': 'https://rickandmortyapi.com/api/location/',
            'table_name': 'locs',
            'ddl': create_table2_sql,},
        {
            'name': 'eps_gr',
            'url': 'https://rickandmortyapi.com/api/episode/',
            'table_name': 'eps',
            'ddl': create_table3_sql},
    ]

    for rm_dict in rm_list:
        rm_name = rm_dict["name"]
        rm_url = rm_dict["url"]
        rm_table_name = rm_dict["table_name"]
        rm_ddl = rm_dict["ddl"]

        with TaskGroup(group_id=rm_name) as tg1:

            create_table = PostgresOperator(
                task_id="create_table_" + rm_name,
                postgres_conn_id="marija-shkurat-wrn7887_pg",
                sql=rm_ddl
            )

            truncate_table = PostgresOperator(
                task_id="truncate_table_" + rm_name,
                postgres_conn_id="marija-shkurat-wrn7887_pg",
                sql=f"TRUNCATE TABLE marija_shkurat_wrn7887.{rm_table_name}"
            )
            save_rm = PythonOperator(
                task_id="save_" + rm_name,
                op_args=[rm_url, rm_table_name],
                python_callable=fetch_and_save_rm_data
            )

        create_table >> truncate_table >> save_rm
