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

PROJECT_NAME = 'ecommerce' # cambio a mismo nombre de la carpeta 
dag_args = {
    'dag_id': 'ecommerce_sophistication', # nombre lógico
    'schedule_interval': None,
    'dagrun_timeout': None, 
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [PROJECT_NAME, 'abravom'], # cambio
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

    computing_customer_segmentation_sophistication = []

    for store_banner in store_banners:
        driver_cores = '8' if store_banner == 'Unimarc' else '4'
        driver_memory = '35g' if store_banner == 'Unimarc' else '20g'

        ecommerce_sophistication = ExtendedDataprocCreateBatchOperator(
            task_id = 'ecommerce_segmentation_sophistication_'
            f'{store_banner.lower()}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                'computing_customer_segmentation_sophistication_ecommerce.py' 
            ),
            dag_env_config=dag_env_config,
            docker_image_name=PROJECT_NAME,
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
                '--store_banner',store_banner,
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],

            spark_executor_instances = 2,
            spark_executor_cores = 4,
            spark_executor_memory = '4096m', 
             # Driver config (dinámico por banner)
            spark_driver_cores = driver_cores,
            spark_driver_memory = driver_memory

        )

        computing_customer_segmentation_sophistication.append(ecommerce_sophistication)

    chain(computing_customer_segmentation_sophistication)
