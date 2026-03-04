# Apache Airflow Portfolio

Портфолио с примерами DAG'ов Apache Airflow, демонстрирующее навыки
проектирования пайплайнов, работы с API, PostgreSQL, XCom и ветвления логики.

## 🛠 Стек
- Apache Airflow
- Python
- PostgreSQL
- REST API

## 📂 Структура проекта

- dags/ — все DAG'и, сгруппированные по темам
- screenshots/ — примеры Graph / Tree View

## 🚀 Реализованные проекты

### 1. Базовые DAG'и
- создание DAG через dag = DAG(...)
- использование with DAG(...)
- TaskFlow API

### 2. DAG с XCom
- передача данных между задачами
- ежедневное расписание

### 3. API → PostgreSQL
- загрузка данных из API
- сохранение в Postgres
- идемпотентная загрузка

### 4. API → PostgreSQL (3 эндпоинта)
- параллельная обработка нескольких эндпоинтов
- разные таблицы назначения

### 5. Ветвление DAG
- BranchPythonOperator
- логика выполнения по дням недели
