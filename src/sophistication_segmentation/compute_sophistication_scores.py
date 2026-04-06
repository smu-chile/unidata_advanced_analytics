"""Defines the DAG that computes brand-category sophistication score."""
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

PROJECT_NAME = 'sophistication_segmentation'
dag_args = {
    'dag_id': 'sophistication_segmentation_scores',
    'schedule_interval': '30 0 1 * *',
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 4,
    'tags': [PROJECT_NAME, 'ecastrot'],
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['ecastrot@unidata.cl'],
        'start_date': pendulum.datetime(
            2023, 12, 5,
            tz=pendulum.timezone('America/Santiago')
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
    brand_compute_score_tasks = [
        ExtendedDataprocCreateBatchOperator(
            task_id=f"compute_brand_score_{store_banner.replace(' ', '_').lower()}",
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                'brand_sophistication_score.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=PROJECT_NAME,
            pyspark_batch_args=[
                '--project_name', PROJECT_NAME,
                '--gcp_project', dag_env_config['project_id'],
                '--execution_date', "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}",  # noqa: E501
                '--store_banner', store_banner,
                '--min_category_transacted_items', "{{ dag_run.conf.get('min_category_transacted_items', 3) }}"  # noqa: E501
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],

            # Driver config (dinámico por banner)
            spark_driver_cores = 8 if store_banner == 'Unimarc' else 4,
            spark_driver_memory = 35 if store_banner == 'Unimarc' else 20
        )

        for store_banner in [
            'Unimarc',
            'Mayorista',
            'Alvi',
            'Super 10'
        ]
    ]

    customer_compute_score_tasks = [
        ExtendedDataprocCreateBatchOperator(
            task_id=f"compute_customer_score_{store_banner.replace(' ', '_').lower()}",
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                'customer_sophistication_score.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=PROJECT_NAME,
            pyspark_batch_args=[
                '--project_name', PROJECT_NAME,
                '--gcp_project', dag_env_config['project_id'],
                '--execution_date', "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}",  # noqa: E501
                '--store_banner', store_banner,
                '--min_category_transacted_items', "{{ dag_run.conf.get('min_category_transacted_items', 3) }}"  # noqa: E501
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],

            # Driver config (dinámico por banner)
            spark_driver_cores = 8 if store_banner == 'Unimarc' else 4,
            spark_driver_memory = 35 if store_banner == 'Unimarc' else 20
        )

        for store_banner in [
            'Unimarc',
            'Mayorista',
            'Alvi',
            'Super 10'
        ]
    ]


chain([customer_compute_score_tasks,brand_compute_score_tasks])
