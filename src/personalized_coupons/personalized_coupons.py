"""Defines the DAG that allocates my usuals products."""
# Default
import json
import platform
import importlib
from datetime import timedelta

# pip
import pendulum
from airflow.models import DAG
from airflow.configuration import conf
from airflow.operators.python import ShortCircuitOperator


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

PROJECT_NAME = 'personalized_coupons'
dag_args = {
    'dag_id': 'personalized_coupons',
    'schedule_interval': '0 20 * * 1-5',
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 2,
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
    # Ingest PDC
    ingest_pdc = ExtendedDataprocCreateBatchOperator(
        task_id='ingest_pdc',
        python_script_path=(
            f'{PROJECT_NAME}/'
            'scripts/'
            'ingest_pdc.py'
        ),
        dag_env_config=dag_env_config,
        docker_image_name=PROJECT_NAME,
        pyspark_batch_args=[
            '--project_name', PROJECT_NAME,
            '--gcp_project', dag_env_config['project_id'],
            '--execution_date', "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}",  # noqa: E501
        ],
        include_paths=[
            'common/',
            f'{PROJECT_NAME}/gbq_objects/'
        ],
    )

    # Ingest ean per offer id
    ingest_ean_per_offerid = ExtendedDataprocCreateBatchOperator(
        task_id='ingest_ean_per_offerid',
        python_script_path=(
            f'{PROJECT_NAME}/'
            'scripts/'
            'ingest_ean_per_offerid.py'
        ),
        dag_env_config=dag_env_config,
        docker_image_name=PROJECT_NAME,
        pyspark_batch_args=[
            '--project_name', PROJECT_NAME,
            '--gcp_project', dag_env_config['project_id'],
            '--execution_date', "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}",  # noqa: E501
        ],
        include_paths=[
            'common/',
            f'{PROJECT_NAME}/gbq_objects/'
        ],
    )

    # ShortCircuitOperator
    def _check_ingest_ean_per_offerid_state(**context):
        """Return False if B was ingest_ean_per_offerid, True otherwise."""
        task_instance = context['dag_run'].get_task_instance('task_b')
        return task_instance.state == 'success'

    check_ingest_ean_per_offerid_state = ShortCircuitOperator(
        task_id='check_ingest_ean_per_offerid_state',
        python_callable=_check_ingest_ean_per_offerid_state,
        trigger_rule='all_done'
    )

    # Main allocation task
    coupon_allocation = ExtendedDataprocCreateBatchOperator(
        task_id='coupon_allocation',
        python_script_path=(
            f'{PROJECT_NAME}/'
            'scripts/'
            'coupon_allocation.py'
        ),
        dag_env_config=dag_env_config,
        docker_image_name=PROJECT_NAME,
        pyspark_batch_args=[
            '--project_name', PROJECT_NAME,
            '--gcp_project', dag_env_config['project_id'],
            '--execution_date', "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}",  # noqa: E501
        ],
        include_paths=[
            'common/',
            f'{PROJECT_NAME}/gbq_objects/'
        ],
    )

[ingest_pdc, ingest_ean_per_offerid] >> check_ingest_ean_per_offerid_state >> coupon_allocation
