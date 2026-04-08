"""
DAG с динамической генерацией задач на основе внешней конфигурации.
Поддерживает Jinja-шаблоны в SQL-запросах для гибкости и адаптивности.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import logging
import pandas as pd
import io
import json
import os
from jinja2 import Template

# Путь к конфигурации
CONFIG_PATH = Variable.get("table_config_path", default_var="/opt/airflow/config/table_configs.json")

# Загружаем конфигурацию
def load_config():
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Конфигурационный файл не найден: {CONFIG_PATH}")

    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

# Читаем SQL из файла
def read_sql_file(filename: str) -> str:
    base_sql_dir = Variable.get("sql_queries_dir", default_var="/opt/airflow/sql")
    filepath = os.path.join(base_sql_dir, filename)

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"SQL-файл не найден: {filepath}")

    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read().strip()

# ФУНКЦИИ ДЛЯ ТАСКОВ
def create_table(table_name: str, table_ddl: str, **context):
    """
    Создает таблицу, если она не существует. Идемпотентная операция.
    Поддерживает Jinja-рендеринг DDL.
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    logging.info(f"Создание/проверка таблицы: {table_name}")

    # Рендеринг Jinja-шаблона для DDL
    try:
        template = Template(table_ddl)
        rendered_ddl = template.render(**context)
        logging.info(f"Отрендеренный DDL: {rendered_ddl}")
    except Exception as e:
        logging.error(f"Ошибка рендеринга DDL для {table_name}: {e}")
        raise

    try:
        hook.run(rendered_ddl, True)
        logging.info(f"Таблица {table_name} создана или уже существует")
    except Exception as e:
        logging.error(f"Ошибка при создании таблицы {table_name}: {e}")
        raise RuntimeError(f"Не удалось создать таблицу {table_name}") from e
    return f"Таблица {table_name} готова"

def load_data(table_name: str, sql_query: str, **context):
    """
    Загружает данные в таблицу с проверкой качества.
    Использует TRUNCATE + INSERT для идемпотентности.
    Принимает уже прочитанный SQL-запрос (оптимизация).
    Поддерживает Jinja-рендеринг.
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')

    # Рендеринг Jinja-шаблона для SQL
    try:
        template = Template(sql_query)
        rendered_sql = template.render(**context)
        logging.info(f"Отрендеренный SQL:\n{rendered_sql}")
    except Exception as e:
        logging.error(f"Ошибка рендеринга SQL для {table_name}: {e}")
        raise

    try:
        df = hook.get_pandas_df(rendered_sql)
    except Exception as e:
        logging.error(f"Ошибка при выполнении SQL-запроса для {table_name}: {e}")
        logging.error(f"Запрос: \n{rendered_sql}")
        raise ValueError(f"Не удалось выполнить запрос для таблицы {table_name}: {e}") from e

    # Проверка качества данных
    if df.empty:
        logging.error(f"Запрос для таблицы {table_name} вернул пустой результат.")
        raise ValueError("Пустой результат")

    if 'id' not in df.columns:
        logging.warning(f"В таблице {table_name} нет колонки 'id'. Пропуск проверки дубликатов.")
    else:
        if df['id'].duplicated().any():
            sample = df[df['id'].duplicated(keep=False)].head(5)
            logging.error(f"Дубликаты по 'id' в {table_name}:\n{sample}")
            raise ValueError("Обнаружены дубликаты")

    # Очистка и вставка
    truncate_sql = f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"
    hook.run(truncate_sql)
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(table_name, con=engine, if_exists='append', index=False)

    logging.info(f"Загружено {len(df)} строк в {table_name}")
    context['ti'].xcom_push(key="row_count", value=len(df))
    return "Загрузка успешна"

def export_to_s3(table_name: str, s3_bucket: str, s3_key_prefix: str, **context):
    """
    Экспортирует данные из таблицы PostgreSQL в S3 в формате CSV.
    Путь в S3: {s3_key_prefix}/{table_name}/{ds}/{table_name}.csv
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    s3_hook = S3Hook(aws_conn_id='aws_default')
    ds = context['ds']
    key = f"{s3_key_prefix.rstrip('/')}/{table_name}/{ds}/{table_name}.csv"
    df = hook.get_pandas_df(f"SELECT * FROM {table_name}")

    if df.empty:
        logging.warning(f"Нет данных для экспорта из {table_name}")
        return "Пропущено"

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    try:
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            bucket_name=s3_bucket,
            key=key,
            replace=True  # Перезаписывает, если файл существует
        )
        logging.info(f"Данные из {table_name} успешно экспортированы в s3://{s3_bucket}/{key}")
        context['ti'].xcom_push(key="exported_rows", value=len(df))
    except Exception as e:
        logging.error(f"Ошибка при экспорте {table_name} в S3: {e}")
        raise

    return f"Экспорт завершён: {len(df)} строк"

# ЗАГРУЗКА КОНФИГУРАЦИИ
try:
    config_data = load_config()
    configs = config_data.get("tables", [])
except Exception as e:
    logging.error(f"Не удалось загрузить конфигурацию: {e}")
    raise

# ОСНОВНОЙ DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dynamic_table_creation',
    default_args=default_args,
    description='Динамический ETL: конфиг из JSON, SQL с Jinja-поддержкой',
    schedule_interval='@daily',
    catchup=False,
    tags=['dynamic', 'etl', 'configurable', 'jinja'],
    max_active_runs=1
) as dag:

    # Старт и завершение
    start_task = EmptyOperator(task_id='start')
    end_task = EmptyOperator(task_id='end')

    # Списки задач
    tasks = []
    final_tasks = []

    # Динамическое создание задач
    for table_config in configs:
        table_name = table_config['table_name']
        ddl_filename = table_config['ddl_file']
        sql_filename = table_config['table_dml']
        export_enabled = table_config.get('need_to_export', False)

        # Читаем SQL-файлы один раз
        try:
            ddl_content = read_sql_file(ddl_filename)
            dml_content = read_sql_file(sql_filename)
        except Exception as e:
            logging.error(f"Ошибка загрузки SQL-файлов для {table_name}: {e}")
            raise

        # Задача: создать таблицу
        create_task = PythonOperator(
            task_id=f'create_table_{table_name}',
            python_callable=create_table,
            op_kwargs={'table_name': table_name, 'table_ddl': ddl_content},
        )

        # Задача: загрузить данные
        load_task = PythonOperator(
            task_id=f'load_data_{table_name}',
            python_callable=load_data,
            op_kwargs={
                'table_name': table_name,
                'sql_query': dml_content
            },
        )

        # Задача: экспорт в S3 (если нужно)
        if export_enabled:
            export_task = PythonOperator(
                task_id=f'export_{table_name}',
                python_callable=export_to_s3,
                op_kwargs={
                    'table_name': table_name,
                    's3_bucket': table_config['s3_bucket'],
                    's3_key_prefix': table_config.get('s3_key_prefix', 'exports/')
                },
            )
            # Устанавливаем зависимости
            create_task >> load_task >> export_task
            final_tasks.append(export_task)
            tasks.extend([create_task, load_task, export_task])
        else:
            # Устанавливаем зависимости без экспорта
            create_task >> load_task
            final_tasks.append(load_task)
            tasks.extend([create_task, load_task])

    # Граф зависимостей
    start_task >> [t for t in set(tasks)]  # Все задачи после старта
    final_tasks >> end_task  # Только финальные задачи перед завершением