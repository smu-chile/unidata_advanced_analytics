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

PROJECT_NAME = 'balance_matrix_sector_oriente'

# Task 1
# Parche 1: nombre de script igual al nombre del archivo .py
script1 = 'product_sensibility_sector_oriente'
# Task 2
script2 = 'processed_regression_data_sector_oriente'
# Task 3
script3 = 'product_elasticity_sector_oriente'
# Task 4
script4 = 'balance_matrix_sector_oriente'

PROJECT_NAME = 'pricing'
dag_id = 'balance_matrix_sector_oriente'
schedule_interval = None
catchup = False
start_date = [2025, 6, 20]
store_banner_list = ['Unimarc']

dag_args = {
    'dag_id': dag_id, #parche 1
    'schedule_interval': schedule_interval,
    'dagrun_timeout': None,
    'catchup': catchup,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [PROJECT_NAME,'rlagosg'],#parche 2
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['rlagosg@unidata.cl'], #parche 3
        'start_date': pendulum.datetime(
            start_date[0],
            start_date[1],
            start_date[2],
            tz=pendulum.timezone('America/Santiago'),
        ),
        'depends_on_past': False,
        'catchup': catchup,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
    }
}

with DAG(**dag_args) as dag:
    EXECUTION_DATE = "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}"  # noqa: E501

    sensibility_tasks = []
    processed_data_tasks = []
    elasticity_tasks = []
    bm_tasks = []

    for store_banner in store_banner_list:
        banner_suffix = store_banner.replace(' ', '_').lower()


        sensibility_task = ExtendedDataprocCreateBatchOperator(
            task_id = f'{script1}_{banner_suffix}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                f'{script1}.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],
        )

        processed_data_task = ExtendedDataprocCreateBatchOperator(
            task_id = f'{script2}_{banner_suffix}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                f'{script2}.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],
        )

        elasticity_task = ExtendedDataprocCreateBatchOperator(
            task_id = f'{script3}_{banner_suffix}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                f'{script3}.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],
        )


        bm_task = ExtendedDataprocCreateBatchOperator(
            task_id = f'{script4}_{banner_suffix}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                f'{script4}.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],
        )

        # Dependencia por formato: primero script1, luego script2
        sensibility_task >> processed_data_task >> elasticity_task >> bm_task

        sensibility_tasks.append(sensibility_task)
        processed_data_tasks.append(processed_data_task)
        elasticity_tasks.append(elasticity_task)
        bm_tasks.append(bm_task)
