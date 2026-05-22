# Default
import json
import platform
import importlib
from datetime import timedelta

# pip
import pendulum
from airflow.models import DAG
from airflow.configuration import conf


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

PROJECT_NAME = 'personalized_catalog'

store_banner_list = ['Unimarc']

dag_args = {
    'dag_id': 'personalized_catalog_allocation',
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
    EXECUTION_DATE = "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}"  # noqa: E501
    sku_per_category = "{{ dag_run.conf.get('sku_per_category', 1) }}"
    top_n = "{{ dag_run.conf.get('top_n', 10) }}"
    month_interval = "{{ dag_run.conf.get('month_interval', 12) }}"
    batch_size = "{{ dag_run.conf.get('batch_size', 50000) }}"

    default_catalog_tasks = []
    personalized_catalog_tasks = []

    for store_banner in store_banner_list:
        banner_suffix = store_banner.replace(' ', '_').lower()


        default_catalog_tasks = ExtendedDataprocCreateBatchOperator(
            task_id = f'default_catalog_allocation_{banner_suffix}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                'default_catalog_allocation.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
                '--store_banner', store_banner,
                '--sku_per_category', sku_per_category,
                '--top_n', top_n
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],
        )

        personalized_catalog_tasks = ExtendedDataprocCreateBatchOperator(
            task_id = f'personalized_catalog_allocation__{banner_suffix}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                'personalized_catalog_allocation.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
                '--store_banner', store_banner,
                '--sku_per_category', sku_per_category,
                '--top_n', top_n,
                '--month_interval', month_interval,
                '--batch_size', batch_size
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],
        )

        default_catalog_tasks >> personalized_catalog_tasks

        default_catalog_tasks.append(default_catalog_tasks)
        personalized_catalog_tasks.append(personalized_catalog_tasks)
