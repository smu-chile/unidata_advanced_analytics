# Default
import json
import platform
import importlib
from datetime import timedelta

# pip
import pendulum
from airflow.models import DAG
from airflow.configuration import conf
from airflow.models.baseoperator import chain


if platform.system() == 'Windows':
    from common.operators.dataproc_create_batch import (
        ExtendedDataprocCreateBatchOperator,
    )
elif platform.system() == 'Linux':
    ExtendedDataprocCreateBatchOperator = (importlib.import_module(
        'BRANCH_PLACEHOLDER.'
        'smu-chile.unidata_advanced_analytics.'
        'src.common.operators.dataproc_create_batch'
    )).ExtendedDataprocCreateBatchOperator
else:
    err_msg = 'Only Linux and Windows are supported.'
    raise NotImplementedError(err_msg)

# Globals
with open(
    f'{conf.get("core", "dags_folder")}/'
    'BRANCH_PLACEHOLDER/'
    'smu-chile/unidata_advanced_analytics/src/common/constants/dag_env_config.json'
) as f:
    dag_env_config = json.load(f)['BRANCH_PLACEHOLDER']

PROJECT_NAME = 'ecommerce_personalized_products'

dag_args = {
    'dag_id': 'ecommerce_personalized_products',
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [PROJECT_NAME,'abravom'],
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['abravom@unidata.cl'],
        'start_date': pendulum.datetime(
            2026, 1, 1,
            tz=pendulum.timezone('America/Santiago'),
        ),
        'depends_on_past': False,
        'catchup': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
    }
}

with DAG(**dag_args) as dag:
    EXECUTION_DATE = "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).next(3).strftime('%Y-%m-%d')) }}"  # noqa: E501
    ean_per_subcategory = "{{ dag_run.conf.get('ean_per_subcategory', 2) }}"
    top_n = "{{ dag_run.conf.get('top_n', 35) }}"
    month_interval = "{{ dag_run.conf.get('month_interval', 6) }}"
    batch_size = "{{ dag_run.conf.get('batch_size', 50000) }}"

    store_banner_list = ['Unimarc']

    personalized_products_tasks = []

    for store_banner in store_banner_list:
        banner_suffix = store_banner.replace(' ', '_').lower()

        personalized_products_task = ExtendedDataprocCreateBatchOperator(
            task_id = f'personalized_products_{banner_suffix}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                'personalized_products_sin_usuals.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
                '--store_banner', store_banner,
                '--ean_per_subcategory', ean_per_subcategory,
                '--top_n', top_n,
                '--month_interval', month_interval,
                '--batch_size', batch_size
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],

            # Driver config (dinámico por banner)
            spark_driver_cores = 8,
            spark_driver_memory = 40
        )

        personalized_products_tasks.append(personalized_products_task)

    chain(personalized_products_tasks)
