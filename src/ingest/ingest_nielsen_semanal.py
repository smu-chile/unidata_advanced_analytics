# Default
from datetime import timedelta

# pip
import pendulum
from airflow.models import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


# Globals
PROJECT_NAME = 'ingest'
GCP_PROJECT_ID =  '{{ var.value.develop_smu_unidata_default_project_id }}'

dag_args = {
    'dag_id': 'ingest_data_nielsen_semanal',
    'schedule_interval': '30 10 * * 4',
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [PROJECT_NAME, 'csotob'],
    'default_args': {
        'project_id': GCP_PROJECT_ID,
        'region': '{{ var.value.develop_smu_unidata_default_region }}',
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
    EXECUTION_WEEK = "{{ dag_run.conf.get('execution_week', dag.timezone.convert(data_interval_start).strftime('%Y%V')) }}"  # noqa: E501
    LOAD_FILES = "{{ dag_run.conf.get('load_files','all')}}"
    ingest_nielsen_semanal = DataprocCreateBatchOperator(
        task_id = 'ingest_nielsen_semanal',

        batch = {
            'pyspark_batch': {
                # Main file to run in the dataproc pod
                'main_python_file_uri': (
                    'gs://{{ var.value.develop_smu_unidata_dataproc_scripts_storage }}/'
                    f'{PROJECT_NAME}/'
                    'scripts/'
                    'ingest_nielsen_semanal.py'
                ),
                # Common files
                'python_file_uris': [
                    (
                        'gs://{{ var.value.develop_smu_unidata_dataproc_scripts_storage }}/'
                        'common/'
                    ),
                    (
                        'gs://{{ var.value.develop_smu_unidata_dataproc_scripts_storage }}/'
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
                    '--execution_week', EXECUTION_WEEK,
                    '--load_files', LOAD_FILES
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
                    'service_account': '{{ var.value.develop_smu_unidata_dataproc_sa }}',
                    'network_uri': '{{ var.value.develop_smu_unidata_dataproc_network }}',
                    'subnetwork_uri': '{{ var.value.develop_smu_unidata_dataproc_subnetwork }}',
                    'ttl': '14400s',
                },
            },
        },

        # Batch ID
        batch_id = 'batch-{{ macros.uuid.uuid4() }}',
        project_id = GCP_PROJECT_ID,
    )
