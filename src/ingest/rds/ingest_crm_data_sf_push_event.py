import json  # noqa: D100
import platform
import importlib
from datetime import timedelta

import pendulum
from airflow.models import DAG
from airflow.configuration import conf


if platform.system() == 'Windows':
    from common.operators.dataproc_create_batch import (
        ExtendedDataprocCreateBatchOperator,
    )
elif platform.system() == 'Linux':
    ExtendedDataprocCreateBatchOperator = (
        importlib.import_module(
            'BRANCH_PLACEHOLDER.'
            'smu-chile.unidata_advanced_analytics.'
            'src.common.operators.dataproc_create_batch'
        )
    ).ExtendedDataprocCreateBatchOperator
else:
    raise NotImplementedError('Only Linux and Windows are supported.')  # noqa: EM101

with open(
    f'{conf.get("core", "dags_folder")}/'
    'BRANCH_PLACEHOLDER/'
    'smu-chile/unidata_advanced_analytics/src/common/constants/'
    'dag_env_config.json'
) as f:
    dag_env_config = json.load(f)['BRANCH_PLACEHOLDER']

PROJECT_NAME = 'ingest'
SUBPROJECT_NAME = 'rds'

dag_args = {
    'dag_id': 'ingest_crm_data_sf_push_event',
    'schedule_interval': '0 8 * * 1',
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [
        PROJECT_NAME,
        SUBPROJECT_NAME,
        'CRM',
        'SF_PUSH_EVENT',
        'ilopeze'
    ],
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['ilopeze@unidata.cl'],
        'start_date': pendulum.datetime(
            2026, 6, 15,
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

    ingest_data = ExtendedDataprocCreateBatchOperator(
    task_id='ingest_data',
    python_script_path=(
        f'{PROJECT_NAME}/'
        f'{SUBPROJECT_NAME}/'
        'scripts/'
        'ingest_crm_data_sf_push_event.py'
    ),
    dag_env_config=dag_env_config,
    docker_image_name=f'{PROJECT_NAME}-{SUBPROJECT_NAME}',
    pyspark_batch_args=[
        '--project_id',
        dag_env_config['project_id'],
    ],
    include_paths=[
        'common/'
    ],
)
