"""
DAG с динамической генерацией задач на основе Python-списка словарей.
Обеспечивает идемпотентное создание таблиц, контроль качества данных и опциональную выгрузку в S3.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.empty import EmptyOperator
import logging
import pandas as pd
import io

# КОНФИГУРАЦИЯ ТАБЛИЦ
config = [
    {
        'table_name': 'test',
        'table_ddl': 'CREATE TABLE IF NOT EXISTS test (id bigint, category varchar(255), amount decimal(10,2))',
        'table_dml': 'SELECT id, category, SUM(amount) as amount FROM raw_t GROUP BY id, category',
        'need_to_export': True,
        's3_bucket': 'my-data-lake',
        's3_key_prefix': 'exports/',
    },
    {
        'table_name': 'test_2',
        'table_ddl': 'CREATE TABLE IF NOT EXISTS test_2 (id bigint, category varchar(255), amount decimal(10,2))',
        'table_dml': 'SELECT id, category, SUM(amount) as amount FROM raw_t GROUP BY id, category',
        'need_to_export': False,
        # s3 параметры не требуются
    },
    # Пример добавления новой таблицы:
    # {
    #     'table_name': 'sales_summary',
    #     'table_ddl': 'CREATE TABLE IF NOT EXISTS sales_summary (region VARCHAR, total_sales DECIMAL, sale_date DATE)',
    #     'table_dml': 'SELECT region, SUM(sales) AS total_sales, sale_date FROM raw_sales GROUP BY region, sale_date',
    #     'need_to_export': True,
    #     's3_bucket': 'analytics-archive',
    #     's3_key_prefix': 'daily/sales/'
    # },
]

# ФУНКЦИИ ДЛЯ ТАСКОВ
def create_table(table_name: str, table_ddl: str, **context):
    """
    Создает таблицу, если она не существует. Идемпотентная операция.
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    logging.info(f"Создание/проверка таблицы: {table_name}")
    logging.info(f"DDL: {table_ddl}")

    try:
        hook.run(table_ddl, True)
        logging.info(f"Таблица {table_name} создана или уже существует")
    except Exception as e:
        logging.error(f"Ошибка при создании таблицы {table_name}: {e}")
        raise
    return f"Таблица {table_name} готова"


def load_data(table_name: str, table_dml: str, **context):
    """
    Загружает данные в таблицу с проверкой качества.
    Использует TRUNCATE + INSERT для идемпотентности.
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    rendered_dml = context['task'].render_template(table_dml, context)

    logging.info(f"=== Начало задачи: load_data_{table_name} ===")
    logging.info(f"Выполняется запрос загрузки:\n{rendered_dml}")

    try:
        df = hook.get_pandas_df(rendered_dml)
    except Exception as e:
        logging.error(f"Ошибка при выполнении SQL-запроса для {table_name}: {str(e)}")
        logging.error(f"Запрос: {rendered_dml}")
        raise ValueError(f"Не удалось выполнить запрос для таблицы {table_name}: {e}") from e

    # Проверка качества данных
    if df.empty:
        error_msg = f"Запрос для таблицы {table_name} вернул пустой результат."
        logging.error(error_msg)
        logging.warning(f"SQL-запрос, вернувший 0 строк:\n{rendered_dml}")
        raise ValueError(error_msg)

    logging.info(f"Получено {len(df)} строк для загрузки.")

    # Проверка дубликатов по id (если колонка существует)
    if 'id' in df.columns:
        duplicated_count = df['id'].duplicated().sum()
        if duplicated_count > 0:
            sample_duplicates = df[df['id'].duplicated(keep=False)].head(10)
            logging.error(f"Обнаружено {duplicated_count} дубликатов по 'id' в таблице {table_name}.")
            logging.error(f"Пример дубликатов:\n{sample_duplicates.to_string()}")
            raise ValueError(f"Дубликаты по 'id' обнаружены в таблице {table_name}")

    # Идемпотентная очистка
    truncate_sql = f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"
    logging.info(f"Выполнение очистки таблицы: {truncate_sql}")
    try:
        hook.run(truncate_sql)
        logging.info(f"Таблица {table_name} очищена.")


def export_to_s3(table_name: str, **context):
    """
    Экспортирует данные таблицы в S3 в формате CSV с использованием Airflow S3Hook.
    Путь: s3://bucket/prefix/table_name/execution_date/table_name.csv
    """
    # Получаем конфигурацию текущей таблицы
    table_config = next((cfg for cfg in config if cfg['table_name'] == table_name), None)
    if not table_config or not table_config.get('need_to_export'):
        logging.info(f"Экспорт отключён для {table_name}")
        return "Пропущено"

    bucket_name = table_config.get('s3_bucket')
    key_prefix = table_config.get('s3_key_prefix', 'exports/')
    ds = context['ds']  # execution date

    hook = PostgresHook(postgres_conn_id='postgres_default')
    s3_hook = S3Hook(aws_conn_id='aws_default')  # использует Airflow Connection

    logging.info(f"Экспорт таблицы {table_name} в S3: s3://{bucket_name}/{key_prefix}{table_name}/{ds}/")

    df = hook.get_pandas_df(f"SELECT * FROM {table_name}")
    if df.empty:
        logging.warning(f"Нет данных для экспорта из {table_name}")
        return "Пропущено: нет данных"

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    key = f"{key_prefix}{table_name}/{ds}/{table_name}.csv"

    try:
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            bucket_name=bucket_name,
            key=key,
            replace=True
        )
        logging.info(f"Данные из {table_name} успешно экспортированы в s3://{bucket_name}/{key}")
        context['ti'].xcom_push(key="exported_rows", value=len(df))
    except Exception as e:
        logging.error(f"Ошибка при экспорте {table_name} в S3: {e}")
        raise

    return f"Экспорт завершён: {len(df)} строк"


# ОСНОВНОЙ DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': lambda context: logging.error(f"Задача провалилась: {context['task_instance'].task_id}")
}

with DAG(
    'dynamic_table_creation',
    default_args=default_args,
    description='DAG с динамической генерацией задач: создание, загрузка, опциональный экспорт',
    schedule_interval='@daily',
    catchup=False,
    tags=['dynamic', 'etl', 'quality', 's3'],
    max_active_runs=1
) as dag:

    # Создаем стартовую задачу
    start_task = EmptyOperator(
        task_id='start',
        dag=dag,
        python_callable=lambda: logging.info("DAG стартовал")
    )

    end_task = EmptyOperator(
        task_id='end',
        dag=dag,
        python_callable=lambda: logging.info("DAG завершен")
    )

    # Динамически создаем задачи для каждой таблицы
    tasks = []

    for table_config in config:
        table_name = table_config['table_name']
        table_ddl = table_config['table_ddl']
        table_dml = table_config['table_dml']

        # Задача: создать таблицу
        create_task = PythonOperator(
            task_id=f'create_table_{table_name}',
            python_callable=create_table,
            op_kwargs={'table_name': table_name, 'table_ddl': table_ddl},
            dag=dag
        )

        # Задача: загрузить данные
        load_task = PythonOperator(
            task_id=f'load_data_{table_name}',
            python_callable=load_data,
            op_kwargs={'table_name': table_name, 'table_dml': table_dml},
            dag=dag
        )

        # Задача: экспорт в S3 (если нужно)
        if table_config.get('need_to_export'):
            export_task = PythonOperator(
                task_id=f'export_{table_name}',
                python_callable=export_to_s3,
                op_kwargs={'table_name': table_name},
                dag=dag
            )
            # Устанавливаем зависимости
            create_task >> load_task >> export_task
            tasks.extend([create_task, load_task, export_task])
        else:
            # Устанавливаем зависимости без экспорта
            create_task >> load_task
            tasks.extend([create_task, load_task])

    # Граф зависимостей
    start_task >> tasks
    for task in tasks:
        task >> end_task