# Default
import json
from datetime import datetime, timedelta

# Airflow
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

PROJECT_NAME = 'sibling_products'

dag_args = {
    'dag_id': 'compute_sibling_products',
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': True,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [PROJECT_NAME, 'bescalanter'],
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['bescalanter@unidata.cl'],
        'start_date': datetime(2023, 1, 1),  # noqa: DTZ001
        'depends_on_past': False,
        'catchup': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
    }
}

with DAG(**dag_args) as dag:  # noqa: AIR002, AIR311
    EXECUTION_DATE = "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_start).strftime('%Y-%m-%d')) }}"  # noqa: E501

    compute_sibling_products_task = DataprocCreateBatchOperator(
        task_id='compute_sibling_products_task',
        batch={
            'pyspark_batch': {
                # Main file to run in the dataproc pod
                'main_python_file_uri': (
                    f'gs://{dag_env_config["scripts_gcs"]}/'
                    f'{PROJECT_NAME}/scripts/'
                    'sibling_products.py'
                ),
                # Common files
                'python_file_uris': [
                    f'gs://{dag_env_config["scripts_gcs"]}/common/',
                    f'gs://{dag_env_config["scripts_gcs"]}/{PROJECT_NAME}/gbq_objects/'
                ],
                # For Google Big Query read/write
                'jar_file_uris': ['gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.2.jar'],
                # Argumentos para el script
                'args': [
                    '--project_name', PROJECT_NAME,
                    '--project_id', dag_env_config['project_id'],
                    '--execution_date', EXECUTION_DATE,
                ],
            },
            # Docker image to be used in the dataproc pod
            'runtime_config': {
                'version': '2.2',
                'container_image': (
                    'us-east1-docker.pkg.dev/'
                    f'{dag_env_config["project_id"]}/'
                    'dataproc-worker-images/'
                    f'{PROJECT_NAME.replace("_", "-")}:latest'
                ),
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
        deferrable=True,
        batch_id='batch-{{ macros.uuid.uuid4() }}',
        project_id=dag_env_config['project_id'],
)
