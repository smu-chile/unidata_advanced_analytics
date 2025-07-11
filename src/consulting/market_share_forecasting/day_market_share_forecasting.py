# Default
from datetime import timedelta

# pip
import pendulum
from airflow.models import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


# Globals
PROJECT_NAME = 'market_share_forecasting'
GCP_PROJECT_ID =  '{{ var.value.develop_smu_unidata_default_project_id }}'

dag_args = {
    'dag_id': 'day_market_share_forecasting',
    'schedule_interval': '0 1 * * FRI',
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
        'start_date': pendulum.datetime(
            2025, 6, 18,
            tz=pendulum.timezone('America/Santiago')
        ),
        'depends_on_past': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
    }
}

with DAG(**dag_args) as dag:
    EXECUTION_DATE = "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}"  # noqa: E501

    get_costs = DataprocCreateBatchOperator(
        task_id = 'get_costs',

        batch = {
            'pyspark_batch': {
                # Main file to run in the dataproc pod
                'main_python_file_uri': (
                    'gs://{{ var.value.develop_smu_unidata_dataproc_scripts_storage }}/'
                    'consulting/'
                    f'{PROJECT_NAME}/'
                    'scripts/'
                    'day_compute_market_share.py'
                ),
                # Common files
                'python_file_uris': [
                    (
                        'gs://{{ var.value.develop_smu_unidata_dataproc_scripts_storage }}/'
                        'common/'
                    ),
                    (
                        'gs://{{ var.value.develop_smu_unidata_dataproc_scripts_storage }}/'
                        'consulting/'
                        f'{PROJECT_NAME}/'
                        'gbq_objects'
                    )
                ],
                # For Google Big Query read/write
                'jar_file_uris': ['gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.2.jar'],
                # Main file arguments
                'args': [
                    '--project_name', PROJECT_NAME,
                    '--gcp_project', GCP_PROJECT_ID,
                    '--execution_date', EXECUTION_DATE,
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


    export_to_netezza = DataprocCreateBatchOperator(
        task_id = 'export_to_netezza',

        batch = {
            'pyspark_batch': {
                # Main file to run in the dataproc pod
                'main_python_file_uri': (
                    'gs://{{ var.value.develop_smu_unidata_dataproc_scripts_storage }}/'
                    'common/'
                    'scripts/'
                    'netezza_exporter.py'
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
                    '--project_name', PROJECT_NAME,
                    '--gcp_project', GCP_PROJECT_ID,
                    '--query',
                        f"""
                            SELECT *
                            FROM {GCP_PROJECT_ID}.ML_LAB.FACT_DAY_MARKET_SHARE_FORECASTING
                        """,  # noqa: S608
                    '--netezza_table_ref',
                        'DESA_CLIENTES.ML_LAB.FACT_DAY_MARKET_SHARE_FORECASTING',  # noqa: E501
                    '--netezza_columns',
                        """
                            venta_unimarc double,
                            venta_unimarc_proyectado double,
                            venta_unimarc_proyectado_min double,
                            venta_unimarc_proyectado_max double,
                            fin_periodo VARCHAR(10)
                        """,
                    '--if_exists', 'rebuild',
                    'timeout', '60'
                ],
            },

            # Docker image to be used in the dataproc pod
            'runtime_config': {
                'version': '2.2',
                'container_image': (
                    'us-east1-docker.pkg.dev/'
                    f'{GCP_PROJECT_ID}/'
                    'dataproc-worker-images/'
                    'netezza-exporter:latest'
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

get_costs >> export_to_netezza
