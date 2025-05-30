# Default
from datetime import timedelta

# pip
import pendulum
from airflow.models import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


# Globals
PROJECT_ID =  '{{ var.value.develop_smu_unidata_default_project_id }}'

dag_args = {
    'dag_id': 'ecastrot_cost_analyzer',
    'schedule_interval': '0 3 * * *',
    'dagrun_timeout': None,
    'catchup': True,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': ['example'],
    'default_args': {
        'project_id': PROJECT_ID,
        'region': '{{ var.value.develop_smu_unidata_default_region }}',
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['ecastrot@unidata.cl'],
        'start_date': pendulum.datetime(
            2025, 5, 22,
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

    EXECUTION_DATE = "{{ dag.timezone.convert(data_interval_start).strftime('%Y-%m-%d') }}"

    get_costs = DataprocCreateBatchOperator(
        task_id = 'get_costs',

        batch = {
            'pyspark_batch': {
                # Main file to run in the dataproc pod
                'main_python_file_uri': (
                    'gs://{{ var.value.develop_smu_unidata_dataproc_scripts_storage }}/'
                    'cost_analyzer/'
                    'scripts/'
                    'cost_analyzer.py'
                ),
                # Common files
                'python_file_uris': [
                    (
                        'gs://{{ var.value.develop_smu_unidata_dataproc_scripts_storage }}/'
                        'common/'
                    ),
                    (
                        'gs://{{ var.value.develop_smu_unidata_dataproc_scripts_storage }}/'
                        'cost_analyzer/'
                        'gbq_objects/'
                    )
                ],
                # For Google Big Query read/write
                'jar_file_uris': ['gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.2.jar'],
                # Main file arguments
                'args': [
                    '--project_id', PROJECT_ID,
                    '--execution_date', EXECUTION_DATE,
                ],
            },

            # Docker image to be used in the dataproc pod
            'runtime_config': {
                'version': '2.2',
                'container_image': (
                    'us-east1-docker.pkg.dev/'
                    'cl-bigdata-analytics/'
                    'dataproc-worker-images/'
                    'cost-analyzer'
                ),
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
        project_id = PROJECT_ID,
    )
