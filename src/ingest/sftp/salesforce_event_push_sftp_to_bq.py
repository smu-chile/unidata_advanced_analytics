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
    err_msg = 'Only Linux and Windows are supported.'
    raise NotImplementedError(err_msg)


with open(
    f'{conf.get("core", "dags_folder")}/'
    'BRANCH_PLACEHOLDER/'
    'smu-chile/unidata_advanced_analytics/'
    'src/common/constants/dag_env_config.json'
) as f:
    dag_env_config = json.load(f)['BRANCH_PLACEHOLDER']


# ---------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------

PROJECT_NAME = 'ingest'
SUBPROJECT_NAME = 'sftp'

GCP_PROJECT_ID = dag_env_config['project_id']
REGION = dag_env_config['region']


# ---------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------

dag_args = {
    'dag_id': 'salesforce_event_push_sftp_to_bq',
    'schedule_interval': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [
        PROJECT_NAME,
        SUBPROJECT_NAME,
        'salesforce',
        'sftp',
        'bigquery',
        'event_push',
        'ilopeze',
    ],
    'default_args': {
        'project_id': GCP_PROJECT_ID,
        'region': REGION,
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['ilopeze@unidata.cl'],
        'start_date': pendulum.datetime(
            2026,
            8,
            25,
            tz=pendulum.timezone('America/Santiago'),
        ),
        'depends_on_past': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
}


with DAG(**dag_args) as dag:
    event_push_sftp_to_bq = (
        ExtendedDataprocCreateBatchOperator(
            task_id='event_push_sftp_to_bq',

            python_script_path=(
                f'{PROJECT_NAME}/'
                f'{SUBPROJECT_NAME}/'
                'scripts/'
                'salesforce_event_push_sftp_to_bq.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=(
                f'{PROJECT_NAME}-{SUBPROJECT_NAME}'
            ),
            pyspark_batch_args=[
                        '--project_id',
                        GCP_PROJECT_ID,
                        '--schema_file',
                        '/var/dataproc/tmp/gbq_objects/CRM_DATA_SF_PUSH_EVENT_STG.json',
                    ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/{SUBPROJECT_NAME}/gbq_objects/',
            ],
        )
    )
