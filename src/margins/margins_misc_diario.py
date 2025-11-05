"""DAG Carga Diaria Fuentes Manuales Reporte de Margen (SharePoint)."""
# Default
import json
from datetime import timedelta

# pip
import pendulum
from airflow.models import DAG
from airflow.configuration import conf
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


# Globals
PROJECT_NAME = 'margins'
with open(
    f'{conf.get("core", "dags_folder")}/'
    'BRANCH_PLACEHOLDER/'
    'smu-chile/unidata_advanced_analytics/src/common/constants/dag_env_config.json'
) as f:
    dag_env_config = json.load(f)['BRANCH_PLACEHOLDER']
GCP_PROJECT_ID = dag_env_config['project_id']
REGION = dag_env_config['region']
SCRIPTS_GCS =  dag_env_config['scripts_gcs']
SERVICE_ACCOUNT = dag_env_config['g_service_account']
NETWORK = dag_env_config['network']
SUBNETWORK = dag_env_config['subnetwork']


dag_args = {
    'dag_id': 'margins_fuentes_manuales_misc_diario',
    'schedule_interval': '00 8 * * *',
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [PROJECT_NAME, 'csotob'],
    'default_args': {
        'project_id': GCP_PROJECT_ID,
        'region': REGION,
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['csotob@unidata.cl'],
        'start_date': pendulum.datetime(
            2025, 6, 25,
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
    EXECUTION_DATE = "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}"  # noqa: E501
    LOAD_FILES = "{{ dag_run.conf.get('load_files','all')}}"

    margins_misc_diario = DataprocCreateBatchOperator(
        task_id = 'margins_misc_diario',

        batch = {
            'pyspark_batch': {
                # Main file to run in the dataproc pod
                'main_python_file_uri': (
                    f'gs://{SCRIPTS_GCS}/'
                    f'{PROJECT_NAME}/'
                    'scripts/'
                    'margins_misc_diario.py'
                ),
                # Common files
                'python_file_uris': [
                    (
                        f'gs://{SCRIPTS_GCS}/'
                        'common/'
                    ),
                    (
                        f'gs://{SCRIPTS_GCS}/'
                        f'{PROJECT_NAME}/'
                        'gbq_objects/'
                    )
                ],
                # For Google Big Query read/write
                'jar_file_uris': ['gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.2.jar'],
                # Main file arguments
                'args': [
                    '--project_id', GCP_PROJECT_ID,
                    '--execution_date', EXECUTION_DATE,
                    '--load_files', LOAD_FILES,
                ],
            },
            # Docker image to be used in the dataproc pod
            'runtime_config': {
                'version': '2.2',
                'container_image': (
                    'us-east1-docker.pkg.dev/'
                    f'{GCP_PROJECT_ID}/'
                    'dataproc-worker-images/'
                    f"{PROJECT_NAME.replace('_', '-')}:latest"
                ),
            },

            # Privileges config
            'environment_config': {
                'execution_config': {
                    'service_account': SERVICE_ACCOUNT,
                    'network_uri': NETWORK,
                    'subnetwork_uri': SUBNETWORK,
                    'ttl': '14400s',
                },
            },
        },

        # Batch ID
        batch_id = 'batch-{{ macros.uuid.uuid4() }}',
        project_id = GCP_PROJECT_ID,
        deferrable = True,
    )
