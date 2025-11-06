"""Example DAG."""
# Default
import json
from datetime import timedelta

# pip
from airflow.models import DAG
from airflow.configuration import conf
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


# Globals
with open(
    f'{conf.get("core", "dags_folder")}/'
    'BRANCH_PLACEHOLDER/'
    'smu-chile/unidata_advanced_analytics/src/common/constants/dag_env_config.json'
) as f:
    dag_env_config = json.load(f)['BRANCH_PLACEHOLDER']

PROJECT_NAME = 'examples'
dag_args = {
    'dag_id': 'gcp_dag_example',
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [PROJECT_NAME, 'ecastrot'],
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
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
                    f'gs://{dag_env_config["scripts_gcs"]}/'
                    f'{PROJECT_NAME}/'
                    'scripts/'
                    'gcp_dag_example.py'
                ),
                # Common files
                'python_file_uris': [
                    (
                        f'gs://{dag_env_config["scripts_gcs"]}/'
                        'common/'
                    ),
                    (
                        f'gs://{dag_env_config["scripts_gcs"]}/'
                        f'{PROJECT_NAME}/'
                        'gbq_objects/'
                    )
                ],
                # For Google Big Query read/write
                'jar_file_uris': ['gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.2.jar'],
                # Main file arguments
                'args': [
                    '--project_id', dag_env_config['project_id'],
                    '--execution_date', EXECUTION_DATE,
                ],
            },

            # Docker image to be used in the dataproc pod
            'runtime_config': {
                'version': '2.2',
                'container_image': '{{ var.value.develop_smu_unidata_docker_image }}',

                # Executor hardware config
                'properties' : {
                    # Executor instances
                    # (0 as PySpark capabilities are not being used)
                    'spark.executor.instances': '0',
                    # Dirver instances
                    'spark.driver.cores': '4',
                    'spark.driver.memory': '24g',
                    'dataproc:dataproc.driver.machine.type': 'n1-highmem-4',
                },
            },

            # Privileges config
            'environment_config': {
                'execution_config': {
                    'service_account': dag_env_config['g_service_account'],
                    'network_uri': dag_env_config['network'],
                    'subnetwork_uri': dag_env_config['subnetwork'],
                    'ttl': '14400s',
                },
            },
        },

        # Batch ID
        batch_id='batch-{{ macros.uuid.uuid4() }}',
        project_id=dag_env_config['project_id'],
    )
