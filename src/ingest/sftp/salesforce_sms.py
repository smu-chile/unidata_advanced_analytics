# Default
import json
from datetime import timedelta

# pip
import pendulum
from airflow.models import DAG
from airflow.configuration import conf
from airflow.providers.google.cloud.sensors.dataproc import DataprocBatchSensor
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


# Globals
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
SCRIPTS_GCS =  dag_env_config['scripts_gcs']
SERVICE_ACCOUNT = dag_env_config['g_service_account']
NETWORK = dag_env_config['network']
SUBNETWORK = dag_env_config['subnetwork']

dag_args = {
    'dag_id': 'salesforce_ing_sms_data',
    'schedule_interval': '20 8 * * *',
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 2,
    'tags': [PROJECT_NAME, SUBPROJECT_NAME, 'salesforce', 'csotob'],
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
    EXECUTION_DATE = "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y%m%d')) }}"  # noqa: E501
    BATCH_ID = 'batch-{{ macros.uuid.uuid4() }}'
    salesforce_sms = DataprocCreateBatchOperator(
        task_id = 'salesforce_sms',

        batch = {
            'pyspark_batch': {
                # Main file to run in the dataproc pod
                'main_python_file_uri': (
                    f'gs://{SCRIPTS_GCS}/'
                    f'{PROJECT_NAME}/'
                    f'{SUBPROJECT_NAME}/'
                    'scripts/'
                    'salesforce_sms.py'
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
                        f'{SUBPROJECT_NAME}/'
                        'gbq_objects/'
                    )
                ],
                # For Google Big Query read/write
                'jar_file_uris': ['gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.2.jar'],
                # Main file arguments
                'args': [
                    '--project_id', GCP_PROJECT_ID,
                    '--execution_date', EXECUTION_DATE
                ],
            },
            # Docker image to be used in the dataproc pod
            'runtime_config': {
                'version': '2.2',
                'container_image': (
                    'us-east1-docker.pkg.dev/'
                    f'{GCP_PROJECT_ID}/'
                    'dataproc-worker-images/'
                    f"{PROJECT_NAME.replace('_', '-')}-{SUBPROJECT_NAME}:latest"
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
        batch_id = BATCH_ID,
        project_id = GCP_PROJECT_ID,
        deferrable = True,
        do_xcom_push=True
    )

    salesforce_sms_sensor = DataprocBatchSensor(
        task_id = 'salesforce_sms_sensor',
        batch_id =BATCH_ID ,
        region = REGION,
        project_id = GCP_PROJECT_ID,
        poke_interval=10,
    )

salesforce_sms >> salesforce_sms_sensor
