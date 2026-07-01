"""Contains the DAG that loads the Ecommdata - Maestra Chile Censo from
PostgreSQL to BigQuery.
"""  # noqa: D205

# -------------------------------------------------------------------------
# Imports
# -------------------------------------------------------------------------
import json
import platform
import importlib
from datetime import timedelta

# pip
import pendulum
from airflow.models import DAG
from airflow.configuration import conf


# -------------------------------------------------------------------------
# ExtendedDataprocCreateBatchOperator import
# -------------------------------------------------------------------------
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


# -------------------------------------------------------------------------
# Globals
# -------------------------------------------------------------------------
with open(
    f'{conf.get("core", "dags_folder")}/'
    'BRANCH_PLACEHOLDER/'
    'smu-chile/unidata_advanced_analytics/src/common/constants/dag_env_config.json'
) as f:
    dag_env_config = json.load(f)['BRANCH_PLACEHOLDER']

PROJECT_NAME = 'ingest'
SUBPROJECT_NAME = 'rds'
SCRIPT_NAME = 'ingest_ecommdata_maestra_chile_censo.py'

dag_args = {
    'dag_id': 'ingest_ecommdata_maestra_chile_censo',
    'schedule_interval': None,
    'dagrun_timeout': timedelta(hours=2),
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [
        PROJECT_NAME,
        SUBPROJECT_NAME,
        'unimarc',
        'cenco_chile',
        'bigquery',
        'ilopeze'
    ],
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['ilopeze@unidata.cl'],
        'email_on_failure': True,
        'email_on_retry': False,
        'start_date': pendulum.datetime(
            2024, 1, 1,
            tz=pendulum.timezone('America/Santiago')
        ),
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(minutes=30)
    }
}

# -------------------------------------------------------------------------
# DAG Definition
# -------------------------------------------------------------------------
with DAG(**dag_args) as dag:
    EXECUTION_DATE="{{dag_run.conf.get('execution_date'," \
        "data_interval_end.strftime('%Y-%m-%d')) }}"  # noqa: ISC002

    ingest_maestra_chile_censo = ExtendedDataprocCreateBatchOperator(
        task_id='ingest_ecommdata_maestra_chile_censo',
        python_script_path=(
            f'{PROJECT_NAME}/'
            f'{SUBPROJECT_NAME}/'
            'scripts/'
            f'{SCRIPT_NAME}'
        ),
        dag_env_config=dag_env_config,
        docker_image_name=f'{PROJECT_NAME}-{SUBPROJECT_NAME}',
        pyspark_batch_args=[
            '--project_id', dag_env_config['project_id'],
            '--execution_date', EXECUTION_DATE,
        ],
        include_paths=[
            'common/',
            f'{PROJECT_NAME}/{SUBPROJECT_NAME}/gbq_objects/'
        ]
    )
