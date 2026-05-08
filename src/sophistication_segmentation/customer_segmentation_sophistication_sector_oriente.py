# Default
import json
from datetime import timedelta

# pip
import pendulum
from airflow.sdk import chain
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

PROJECT_NAME = 'sophistication_segmentation' #tiene que coincidir con la carpeta

dag_args = {
    'dag_id': 'customer_segmentation_sophisticaction_sector_oriente',
    'schedule_interval': '0 9 2 * *',
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 4,
    'tags': [PROJECT_NAME, 'rlagosg'],
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['rlagosg@unidata.cl'],
        'start_date': pendulum.datetime(
            2025,
            6,
            20,
            tz=pendulum.timezone('America/Santiago'),
        ),
        'depends_on_past': False,
        'catchup': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
}

with DAG(**dag_args) as dag:
    EXECUTION_DATE = (
        "{{ dag_run.conf.get('execution_date', "
        "dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}"
    )

    store_banners = ['Unimarc']

    computing_customer_segmentation_sophistication = []

    for store_banner in store_banners:
        # driver_cores = '8' if store_banner == 'Unimarc' else '4'
        # driver_memory = '35g' if store_banner == 'Unimarc' else '20g'

        task = DataprocCreateBatchOperator(
            task_id=(
                'computing_customer_segmentation_sophistication_sector_oriente'
                f'{store_banner.replace(" ", "_").lower()}'
            ),
            batch={
                'pyspark_batch': {
                    # Main file to run in the dataproc pod
                    'main_python_file_uri': (
                        f'gs://{dag_env_config["scripts_gcs"]}/'
                        f'{PROJECT_NAME}/'
                        'scripts/'
                        'computing_customer_segmentation_sophistication_sector_oriente.py'
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
                        ),
                    ],
                    # For Google Big Query read/write
                    'jar_file_uris': [
                        'gs://spark-lib/bigquery/'
                        'spark-3.5-bigquery-0.42.2.jar'
                    ],
                    # Main file arguments
                    'args': [
                        '--project_id',
                        dag_env_config['project_id'],
                        '--execution_date',
                        EXECUTION_DATE,
                        '--store_banner',
                        store_banner,
                    ],
                },
                # Docker image to be used in the dataproc pod
                'runtime_config': {
                    'version': '2.2',
                    'container_image': (
                        'us-east1-docker.pkg.dev/'
                        f'{dag_env_config["project_id"]}/'
                        'dataproc-worker-images/'
                        f"{PROJECT_NAME.replace('_', '-')}:latest"
                    ),
                    # Executor hardware config
                    'properties': {
                        # Executor instances
                        'spark.executor.instances': '2',
                        'spark.executor.cores': '4',
                        'spark.executor.memory': '4096m',
                        # Driver config (dinámico por banner)
                        # 'spark.driver.cores': driver_cores,
                        # 'spark.driver.memory': driver_memory,
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

        computing_customer_segmentation_sophistication.append(task)

    chain(computing_customer_segmentation_sophistication)
