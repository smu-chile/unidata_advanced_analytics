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
with open(
    f'{conf.get("core", "dags_folder")}/'
    'BRANCH_PLACEHOLDER/'
    'smu-chile/unidata_advanced_analytics/src/common/constants/dag_env_config.json'
) as f:
    dag_env_config = json.load(f)['BRANCH_PLACEHOLDER']

PROJECT_NAME = 'pricing'
dag_id = 'balance_matrix'
schedule_interval = None
catchup = False
start_date = [2025, 6, 20]

# Task 1
script1 = 'product_sensibility'
# Task 2
script2 = 'processed_regression_data'
# Task 3
script3 = 'product_elasticity'


store_banner_list = ['Unimarc', 'Alvi']

dag_args = {
    'dag_id': 'balance_matrix',
    'schedule_interval': schedule_interval,
    'dagrun_timeout': None,
    'catchup': catchup,
    'max_active_runs': 1,
    'concurrency': 4,
    'tags': [PROJECT_NAME, 'bmolinab'],
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['bmolinab@unidata.cl'],
        'start_date': pendulum.datetime(
            start_date[0],
            start_date[1],
            start_date[2],
            tz=pendulum.timezone('America/Santiago'),
        ),
        'depends_on_past': False,
        'catchup': catchup,
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
    )  # noqa: E501

    sensibility_tasks = []
    processed_data_tasks = []
    elasticity_tasks = []

    for store_banner in store_banner_list:
        banner_suffix = store_banner.replace(' ', '_').lower()

        # ---------- Task script1: product_sensibility ----------
        sensibility_task = DataprocCreateBatchOperator(
            task_id=f'{script1}_{banner_suffix}',
            batch={
                'pyspark_batch': {
                    # Main file to run in the dataproc pod
                    'main_python_file_uri': (
                        f'gs://{dag_env_config["scripts_gcs"]}/'
                        f'{PROJECT_NAME}/'
                        'scripts/'
                        f'{script1}.py'
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
                        # Driver instances
                        'spark.driver.cores': '4',
                        'spark.driver.memory': '20g',
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
            # Leaves Airflow Trigger to track the status of Dataproc batch
            batch_id='batch-{{ macros.uuid.uuid4() }}',
            project_id=dag_env_config['project_id'],
        )

        # ---------- Task script2: forecast_processed_data ----------
        processed_data_task = DataprocCreateBatchOperator(
            task_id=f'{script2}_{banner_suffix}',
            batch={
                'pyspark_batch': {
                    'main_python_file_uri': (
                        f'gs://{dag_env_config["scripts_gcs"]}/'
                        f'{PROJECT_NAME}/'
                        'scripts/'
                        f'{script2}.py'
                    ),
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
                    'jar_file_uris': [
                        'gs://spark-lib/bigquery/'
                        'spark-3.5-bigquery-0.42.2.jar'
                    ],
                    'args': [
                        '--project_id',
                        dag_env_config['project_id'],
                        '--execution_date',
                        EXECUTION_DATE,
                        '--store_banner',
                        store_banner,
                        '--use',
                        'ELASTICITY',
                    ],
                },
                'runtime_config': {
                    'version': '2.2',
                    'container_image': (
                        'us-east1-docker.pkg.dev/'
                        f'{dag_env_config["project_id"]}/'
                        'dataproc-worker-images/'
                        f"{PROJECT_NAME.replace('_', '-')}:latest"
                    ),
                    'properties': {
                        'spark.executor.instances': '2',
                        'spark.executor.cores': '4',
                        'spark.executor.memory': '4096m',
                        'spark.driver.cores': '4',
                        'spark.driver.memory': '20g',
                    },
                },
                'environment_config': {
                    'execution_config': {
                        'service_account': dag_env_config['g_service_account'],
                        'network_uri': dag_env_config['network'],
                        'subnetwork_uri': dag_env_config['subnetwork'],
                        'ttl': '14400s',
                    },
                },
            },
            batch_id='batch-{{ macros.uuid.uuid4() }}',
            project_id=dag_env_config['project_id'],
        )


        # ---------- Task script3: product_elasticity ----------
        elasticity_task = DataprocCreateBatchOperator(
            task_id=f'{script3}_{banner_suffix}',
            batch={
                'pyspark_batch': {
                    'main_python_file_uri': (
                        f'gs://{dag_env_config["scripts_gcs"]}/'
                        f'{PROJECT_NAME}/'
                        'scripts/'
                        f'{script3}.py'
                    ),
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
                    'jar_file_uris': [
                        'gs://spark-lib/bigquery/'
                        'spark-3.5-bigquery-0.42.2.jar'
                    ],
                    'args': [
                        '--project_id',
                        dag_env_config['project_id'],
                        '--execution_date',
                        EXECUTION_DATE,
                        '--store_banner',
                        store_banner,
                        '--use',
                        'ELASTICITY',
                    ],
                },
                'runtime_config': {
                    'version': '2.2',
                    'container_image': (
                        'us-east1-docker.pkg.dev/'
                        f'{dag_env_config["project_id"]}/'
                        'dataproc-worker-images/'
                        f"{PROJECT_NAME.replace('_', '-')}:latest"
                    ),
                    'properties': {
                        'spark.executor.instances': '2',
                        'spark.executor.cores': '4',
                        'spark.executor.memory': '4096m',
                        'spark.driver.cores': '4',
                        'spark.driver.memory': '10g',
                    },
                },
                'environment_config': {
                    'execution_config': {
                        'service_account': dag_env_config['g_service_account'],
                        'network_uri': dag_env_config['network'],
                        'subnetwork_uri': dag_env_config['subnetwork'],
                        'ttl': '14400s',
                    },
                },
            },
            batch_id='batch-{{ macros.uuid.uuid4() }}',
            project_id=dag_env_config['project_id'],
        )

        # Dependencia por formato: primero script1, luego script2
        sensibility_task >> processed_data_task >> elasticity_task

        sensibility_tasks.append(sensibility_task)
        processed_data_tasks.append(processed_data_task)
        elasticity_tasks.append(elasticity_task)

