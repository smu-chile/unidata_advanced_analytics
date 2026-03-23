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

PROJECT_NAME = 'semantic_basket_segmentation'
dag_args = {
    'dag_id': 'semantic_basket_topic',
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [PROJECT_NAME, 'abravom'],
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
    EXECUTION_DATE = "{{ dag_run.conf.get('execution_date',dag.timezone.convert(data_interval_start).strftime('%Y-%m-%d')) }}"  # noqa: E501

    semantic_basket_topic = ExtendedDataprocCreateBatchOperator(
        task_id = 'basket_topic_predict',
        python_script_path=(
            f'{PROJECT_NAME}/'
            'scripts/'
            'basket_topic_predict.py'
        ),
        dag_env_config=dag_env_config,
        docker_image_name=PROJECT_NAME,
        pyspark_batch_args=[
            '--project_id', dag_env_config['project_id'],
            '--execution_date', EXECUTION_DATE,
            '--batch_size', "{{ dag_run.conf.get('batch_size', 1000000) }}",

        ],
        include_paths=[
            'common/',
            f'{PROJECT_NAME}/gbq_objects/'
        ],
    )


