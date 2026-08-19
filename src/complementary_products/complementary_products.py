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

PROJECT_NAME = 'complementary_products'

dag_args = {
    'dag_id': 'complementary_products',
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
    month_interval = "{{ dag_run.conf.get('month_interval', 3) }}"
    min_canasta = "{{ dag_run.conf.get('min_canasta', 500) }}"
    min_freq_conj = "{{ dag_run.conf.get('min_freq_conj', 500) }}"
    max_ir = "{{ dag_run.conf.get('max_ir', 0.8) }}"
    max_compl = "{{ dag_run.conf.get('max_ir', 250) }}"


    store_banner_list = ['Unimarc','Alvi','Super 10']

    complementary_products_tasks = []

    for store_banner in store_banner_list:
        if store_banner == 'Super 10':
            lower_banner = 's10'
        elif (store_banner == 'Unimarc' or store_banner == 'Alvi'):
            lower_banner = store_banner.lower()

        complementary_products_task = ExtendedDataprocCreateBatchOperator(
            task_id = f'complementary_products_{lower_banner}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                'complementary_products.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
                '--store_banner', store_banner,
                '--month_interval', month_interval,
                '--min_canasta', min_canasta,
                '--min_freq_conj', min_freq_conj,
                '--max_ir', max_ir,
                '--max_compl', max_compl
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],
        )

        complementary_products_tasks.append(complementary_products_task)
