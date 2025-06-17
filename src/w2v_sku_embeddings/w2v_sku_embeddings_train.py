"""Defines the DAG that trains the product embeddings."""
# Default
from datetime import timedelta

# pip
import pendulum
from airflow.models import DAG
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


# Globals
PROJECT_NAME = 'w2v_sku_embeddings'
GCP_PROJECT_ID =  '{{ var.value.develop_smu_unidata_default_project_id }}'

dag_args = {
    'dag_id': 'w2v_sku_embeddings_train',
    'schedule_interval': '30 0 1 * *',
    'dagrun_timeout': None,
    'catchup': True,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [PROJECT_NAME, 'ecastrot'],
    'default_args': {
        'project_id': GCP_PROJECT_ID,
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
    RUN_UUID4 = '{{ macros.uuid.uuid4() }}'

    # Tasks
    train_tasks = [
        DataprocCreateBatchOperator(
            task_id = f"train_embeddings_{store_banner.replace(' ', '_').lower()}",

            batch = {
                'pyspark_batch': {
                    # Main file to run in the dataproc pod
                    'main_python_file_uri': (
                        'gs://{{ var.value.develop_smu_unidata_dataproc_scripts_storage }}/'
                        f'{PROJECT_NAME}/'
                        'scripts/'
                        'sku_embedding_train.py'
                    ),
                    # Common files
                    'python_file_uris': [
                        (
                            'gs://{{ var.value.develop_smu_unidata_dataproc_scripts_storage }}/'
                            'common/'
                        )
                    ],
                    # For Google Big Query read/write
                    'jar_file_uris': ['gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.2.jar'],
                    # Main file arguments
                    'args': [
                        '--uuid', RUN_UUID4,
                        '--project_name', PROJECT_NAME,
                        '--gcp_project', GCP_PROJECT_ID,
                        '--execution_date', "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}",  # noqa: E501
                        '--store_banner', store_banner,
                        '--epochs', "{{ dag_run.conf.get('epochs', 10) }}",
                        '--batch_size', "{{ dag_run.conf.get('batch_size', 5000000) }}",
                        '--sg', "{{ dag_run.conf.get('sg', 1) }}",
                        '--hs', "{{ dag_run.conf.get('hs', 0) }}",
                        '--min_count', "{{ dag_run.conf.get('min_count', 100) }}",
                        '--window_size', "{{ dag_run.conf.get('window_size', 100) }}",
                        '--ns_exponent', "{{ dag_run.conf.get('ns_exponent', -0.5) }}",
                        '--embedding_dim', "{{ dag_run.conf.get('embedding_dim', 100) }}",
                        '--n_negative_samples', "{{ dag_run.conf.get('n_negative_samples', 20) }}",
                        '--cart_lenght', "{{ dag_run.conf.get('cart_lenght', '2 100') }}",
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
                        'subnetwork_uri': '{{ var.value.develop_smu_unidata_dataproc_subnetwork }}',  # noqa: E501
                    },
                },
            },

            # Batch ID
            batch_id = f'batch-{RUN_UUID4}',
            project_id = GCP_PROJECT_ID,
        )

        for store_banner in [
            #'Unimarc',
            #'Mayorista',
            #'Alvi',
            'Super 10'
        ]
    ]

chain(train_tasks)
