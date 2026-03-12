"""Defines the DAG that trains the product embeddings."""
# Default
import json
import platform
import importlib
from datetime import timedelta

# pip
import pendulum
from airflow.models import DAG
from airflow.configuration import conf
from airflow.models.baseoperator import chain


if platform.system() == 'Windows':
    from common.operators.dataproc_create_batch import (
        ExtendedDataprocCreateBatchOperator,
    )
elif platform.system() == 'Linux':
    ExtendedDataprocCreateBatchOperator = (importlib.import_module(
        'BRANCH_PLACEHOLDER.'
        'smu-chile.unidata_advanced_analytics.'
        'src.common.operators.dataproc_create_batch'
    )).ExtendedDataprocCreateBatchOperator
else:
    err_msg = 'Only Linux and Windows are supported.'
    raise NotImplementedError(err_msg)

# Globals
with open(
    f'{conf.get("core", "dags_folder")}/'
    'BRANCH_PLACEHOLDER/'
    'smu-chile/unidata_advanced_analytics/src/common/constants/dag_env_config.json'
) as f:
    dag_env_config = json.load(f)['BRANCH_PLACEHOLDER']

PROJECT_NAME = 'w2v_sku_embeddings'
dag_args = {
    'dag_id': 'w2v_sku_embeddings_train',
    'schedule_interval': '30 0 1 * *',
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 4,
    'tags': [PROJECT_NAME, 'abravom'],
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['abravom@unidata.cl'],
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
    EXECUTION_DATE = "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}"  # noqa: E501
    STORE_BANNERS = [
        'Unimarc',
        'Mayorista',
        'Alvi',
        'Super 10'
    ]

    # ---------------------------------------------------------------------
    # Week forecasting
    # ---------------------------------------------------------------------
    train_tasks = [
        ExtendedDataprocCreateBatchOperator(
            task_id=f"train_product_embeddings_{store_banner.replace(' ', '_').lower()}",
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                'w2v_train_product_embeddings.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=PROJECT_NAME,
            pyspark_batch_args=[
                '--uuid', '{{ macros.uuid.uuid4() }}',
                '--project_name', PROJECT_NAME,
                '--gcp_project', dag_env_config['project_id'],
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
                '--cart_lenght', "{{ dag_run.conf.get('min_cart_lenght', 2) }}", "{{ dag_run.conf.get('max_cart_lenght', 100) }}"  # noqa: E501
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],
            ttl=43200,
            spark_driver_cores=8,
            spark_driver_memory=35,
        )

        for store_banner in STORE_BANNERS
    ]

    predict_tasks = [
        ExtendedDataprocCreateBatchOperator(
            task_id=f"predict_customer_embeddings_{store_banner.replace(' ', '_').lower()}",
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                'w2v_predict_customer_embeddings.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=PROJECT_NAME,
            pyspark_batch_args=[
                '--project_name', PROJECT_NAME,
                '--gcp_project', dag_env_config['project_id'],
                '--execution_date', "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}",  # noqa: E501
                '--store_banner', store_banner,
                '--month_interval', "{{ dag_run.conf.get('month_interval', 12) }}"
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],
            ttl=43200,
        )

        for store_banner in STORE_BANNERS
    ]


chain(train_tasks, predict_tasks)
