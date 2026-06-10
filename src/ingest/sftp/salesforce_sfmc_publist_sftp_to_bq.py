"""DAG SFMC Publication List SFTP -> GCS -> BQ STG -> BQ FINAL."""

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
    ExtendedDataprocCreateBatchOperator = (
        importlib.import_module(
        'BRANCH_PLACEHOLDER.'
        'smu-chile.unidata_advanced_analytics.'
        'src.common.operators.dataproc_create_batch')
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

PROJECT_NAME = 'ingest'
SUBPROJECT_NAME = 'sftp'

GCP_PROJECT_ID = dag_env_config['project_id']
REGION = dag_env_config['region']

dag_args = {
    'dag_id': 'salesforce_sfmc_publist_sftp_to_bq',
    'schedule_interval': '0 8 * * 1-5',
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [
        PROJECT_NAME,
        SUBPROJECT_NAME,
        'salesforce',
        'sftp',
        'gcs',
        'bigquery',
        'ilopeze'
    ],
        'default_args': {
        'project_id': GCP_PROJECT_ID,
        'region': REGION,
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['[ilopeze@unidata.cl](mailto:ilopeze@unidata.cl)'],
        'start_date': pendulum.datetime(
            2026,
            6,
            1,
            tz=pendulum.timezone(
            'America/Santiago'
            ),
        ),
        'depends_on_past': False,
        'catchup': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
}

with DAG(**dag_args) as dag:  # noqa: AIR002, AIR311

    EXECUTION_DATE = (
        "{{ dag_run.conf.get("
        "'execution_date', "
        "dag.timezone.convert("
        "data_interval_end"
        ").strftime('%Y%m%d')) }}"
    )

    sfmc_publist_sftp_to_gcs = (
        ExtendedDataprocCreateBatchOperator(
            task_id='sfmc_publist_sftp_to_gcs',
            python_script_path=(
                f'{PROJECT_NAME}/'
                f'{SUBPROJECT_NAME}/'
                'scripts/'
                'salesforce_sfmc_publist_sftp_to_gcs.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}-{SUBPROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id',
                GCP_PROJECT_ID,
                '--execution_date',
                EXECUTION_DATE,
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/{SUBPROJECT_NAME}/gbq_objects/',
            ],
        )
    )

    sfmc_publist_gcs_to_bq_stg = (
        ExtendedDataprocCreateBatchOperator(
            task_id='sfmc_publist_gcs_to_bq_stg',
            python_script_path=(
                f'{PROJECT_NAME}/'
                f'{SUBPROJECT_NAME}/'
                'scripts/'
                'salesforce_sfmc_publist_gcs_to_stg.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}-{SUBPROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id',
                GCP_PROJECT_ID,
                '--execution_date',
                EXECUTION_DATE,
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/{SUBPROJECT_NAME}/gbq_objects/',
            ],
        )
    )

    sfmc_publist_stg_to_bq_final = (
        ExtendedDataprocCreateBatchOperator(
            task_id='sfmc_publist_stg_to_bq_final',
            python_script_path=(
                f'{PROJECT_NAME}/'
                f'{SUBPROJECT_NAME}/'
                'scripts/'
                'salesforce_sfmc_publist_stg_to_bq.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}-{SUBPROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id',
                GCP_PROJECT_ID,
                '--execution_date',
                EXECUTION_DATE,
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/{SUBPROJECT_NAME}/gbq_objects/',
            ],
        )
    )

# ------------------------------------------------------------------
# Task Dependencies
# ------------------------------------------------------------------

sfmc_publist_sftp_to_gcs >> sfmc_publist_gcs_to_bq_stg
sfmc_publist_gcs_to_bq_stg >> sfmc_publist_stg_to_bq_final
