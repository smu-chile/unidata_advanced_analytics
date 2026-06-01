"""DAG SFMC Publication List SFTP -> GCS."""

import json
from datetime import timedelta

import pendulum
from airflow.models import DAG
from airflow.configuration import conf
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


PROJECT_NAME = 'ingest'
SUBPROJECT_NAME = 'sftp'

with open(
    f'{conf.get("core", "dags_folder")}/'
    'BRANCH_PLACEHOLDER/'
    'smu-chile/unidata_advanced_analytics/src/common/constants/dag_env_config.json'
) as f:
    dag_env_config = json.load(f)['BRANCH_PLACEHOLDER']

GCP_PROJECT_ID = dag_env_config['project_id']
REGION = dag_env_config['region']
SCRIPTS_GCS = dag_env_config['scripts_gcs']
SERVICE_ACCOUNT = dag_env_config['g_service_account']
NETWORK = dag_env_config['network']
SUBNETWORK = dag_env_config['subnetwork']

dag_args = {
    'dag_id': 'salesforce_sfmc_publist_sftp_to_gcs',
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [
        PROJECT_NAME,
        SUBPROJECT_NAME,
        'sfmc',
        'sftp',
        'gcs'
    ],
    'default_args': {
        'project_id': GCP_PROJECT_ID,
        'region': REGION,
        'owner': 'BIGDATA_ANALYTICS',
        'start_date': pendulum.datetime(
            2026,
            6,
            1,
            tz=pendulum.timezone(
                'America/Santiago'
            )
        ),
        'depends_on_past': False,
        'catchup': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    }
}

with DAG(**dag_args) as dag:

    EXECUTION_DATE = (
        "{{ dag_run.conf.get("
        "'execution_date', "
        "dag.timezone.convert("
        "data_interval_end"
        ").strftime('%Y%m%d')) }}"
    )

    sfmc_publist_sftp_to_gcs = (
        DataprocCreateBatchOperator(
            task_id='sfmc_publist_sftp_to_gcs',

            batch={
                'pyspark_batch': {

                    'main_python_file_uri': (
                        f'gs://{SCRIPTS_GCS}/'
                        f'{PROJECT_NAME}/'
                        f'{SUBPROJECT_NAME}/'
                        'scripts/'
                        'salesforce_sfmc_publist_sftp_to_gcs.py'
                    ),

                    'python_file_uris': [
                        (
                            f'gs://{SCRIPTS_GCS}/'
                            'common/'
                        )
                    ],

                    'args': [
                        '--project_id',
                        GCP_PROJECT_ID,

                        '--execution_date',
                        EXECUTION_DATE
                    ]
                },

                'runtime_config': {
                    'version': '2.2',
                    'container_image': (
                        'us-east1-docker.pkg.dev/'
                        f'{GCP_PROJECT_ID}/'
                        'dataproc-worker-images/'
                        f'{PROJECT_NAME}-{SUBPROJECT_NAME}:latest'
                    )
                },

                'environment_config': {
                    'execution_config': {
                        'service_account': SERVICE_ACCOUNT,
                        'network_uri': NETWORK,
                        'subnetwork_uri': SUBNETWORK,
                        'ttl': '14400s'
                    }
                }
            },

            batch_id='batch-{{ macros.uuid.uuid4() }}',

            project_id=GCP_PROJECT_ID,

            deferrable=True
        )
    )  # noqa: W292