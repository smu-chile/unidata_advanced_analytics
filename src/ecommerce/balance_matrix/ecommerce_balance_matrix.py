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

PROJECT_NAME = 'ecommerce'
SUBPROJECT_NAME = 'balance_matrix'

dag_args = {
    'dag_id': 'ecommerce_balance_matrix', # nombre lógico
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [PROJECT_NAME, SUBPROJECT_NAME, 'abravom'], # cambio
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['abravom@unidata.cl'],
        'start_date': pendulum.datetime(
            2025, 1, 1,
            tz=pendulum.timezone('America/Santiago')
        ),
        'depends_on_past': False,
        'catchup': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
    }
}


with DAG(**dag_args) as dag:
    EXECUTION_DATE = (
        "{{ dag_run.conf.get('execution_date', "
        "dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}"
    )

    store_banners = ['Unimarc', 'Alvi']

    sensibility_tasks = []
    processed_data_tasks = []
    elasticity_tasks = []
    bm_tasks = []

    for store_banner in store_banners:
        sensibility_task = ExtendedDataprocCreateBatchOperator(
            task_id = 'ecommerce_sensibility_'
            f'{store_banner.lower()}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                f'{SUBPROJECT_NAME}/'
                'scripts/'
                'product_sensibility_ecommerce.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}-{SUBPROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
                '--store_banner',store_banner,
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/{SUBPROJECT_NAME}/gbq_objects/'
            ],

             # Driver config (dinámico por banner)
            spark_driver_cores = 4,
            spark_driver_memory = 20

        )

        processed_data_task = ExtendedDataprocCreateBatchOperator(
            task_id = 'processed_data_tasks_'
            f'{store_banner.lower()}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                f'{SUBPROJECT_NAME}/'
                'scripts/'
                'ecommerce_processed_regression_data.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}-{SUBPROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
                '--store_banner',store_banner,
                '--use','ELASTICITY'
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/{SUBPROJECT_NAME}/gbq_objects/'
            ],

             # Driver config (dinámico por banner)
            spark_driver_cores = 4,
            spark_driver_memory = 20

        )

        elasticity_task = ExtendedDataprocCreateBatchOperator(
            task_id = 'elasticity_tasks_'
            f'{store_banner.lower()}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                f'{SUBPROJECT_NAME}/'
                'scripts/'
                'product_elasticity_ecommerce.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}-{SUBPROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
                '--store_banner',store_banner,
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/{SUBPROJECT_NAME}/gbq_objects/'
            ],

             # Driver config (dinámico por banner)
            spark_driver_cores = 8,
            spark_driver_memory = 40

        )

        bm_task = ExtendedDataprocCreateBatchOperator(
            task_id = 'bm_tasks_'
            f'{store_banner.lower()}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                f'{SUBPROJECT_NAME}/'
                'scripts/'
                'balance_matrix_ecommerce.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}-{SUBPROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
                '--store_banner',store_banner,
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/{SUBPROJECT_NAME}/gbq_objects/'
            ],

             # Driver config (dinámico por banner)
            spark_driver_cores = 4,
            spark_driver_memory = 10

        )

        sensibility_task >> processed_data_task >> elasticity_task >> bm_task

        sensibility_tasks.append(sensibility_task)
        processed_data_tasks.append(processed_data_task)
        elasticity_tasks.append(elasticity_task)
        bm_tasks.append(bm_task)
