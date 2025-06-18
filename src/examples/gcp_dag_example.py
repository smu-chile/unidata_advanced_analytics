"""Example DAG."""
# Default
from datetime import timedelta

# pip
from airflow.models import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


# Globals
PROJECT_NAME = 'examples'
GCP_PROJECT_ID =  '{{ var.value.develop_smu_unidata_default_project_id }}'

dag_args = {
    'dag_id': 'gcp_dag_example',
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [PROJECT_NAME, 'ecastrot'],
    'default_args': {
        'project_id': GCP_PROJECT_ID,
        'region': '{{ var.value.develop_smu_unidata_default_region }}',
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['ecastrot@unidata.cl'],
        'start_date': None,
        'depends_on_past': False,
        'catchup': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
    }
}

with DAG(**dag_args) as dag:

    EXECUTION_DATE = "{{ dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d') }}"

    create_batch = DataprocCreateBatchOperator(
        task_id = 'create_batch',

        batch = {
            'pyspark_batch': {
                # Main file to run in the dataproc pod
                'main_python_file_uri': (
                    'gs://{{ var.value.develop_smu_unidata_dataproc_scripts_storage }}/'
                    f'{PROJECT_NAME}/'
                    'scripts/'
                    'gcp_dag_example.py'
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
                ],
            },

            # Docker image to be used in the dataproc pod
            'runtime_config': {
                'version': '2.2',
                'container_image': '{{ var.value.develop_smu_unidata_docker_image }}',
            },

            # Privileges config
            'environment_config': {
                'execution_config': {
                    'service_account': '{{ var.value.develop_smu_unidata_dataproc_sa }}',
                    'network_uri': '{{ var.value.develop_smu_unidata_dataproc_network }}',
                    'subnetwork_uri': '{{ var.value.develop_smu_unidata_dataproc_subnetwork }}',
                },
            },
        },

        # Batch ID
        batch_id = 'batch-{{ macros.uuid.uuid4() }}',
        project_id = GCP_PROJECT_ID,
    )
