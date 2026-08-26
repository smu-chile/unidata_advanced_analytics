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

# Task 4
script4 = 'balance_matrix_TEMP'



store_banner_list = ['Unimarc', 'Alvi', 'Super 10'] #, 'Alvi', 'Mayorista', 'Super 10']

dag_args = {
    'dag_id': 'balance_matrix_TEMP',
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

    bm_tasks = []

    for store_banner in store_banner_list:
        banner_suffix = store_banner.replace(' ', '_').lower()


        # ---------- Task script4: balance_matrix ----------
        bm_task = DataprocCreateBatchOperator(
            task_id=f'{script4}_{banner_suffix}',
            batch={
                'pyspark_batch': {
                    'main_python_file_uri': (
                        f'gs://{dag_env_config["scripts_gcs"]}/'
                        f'{PROJECT_NAME}/'
                        'scripts/'
                        f'{script4}.py'
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

        bm_tasks.append(bm_task)

