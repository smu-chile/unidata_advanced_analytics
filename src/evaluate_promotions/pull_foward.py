# Default
import json
import platform
import importlib
from datetime import timedelta

# pip
import pendulum
from airflow.models import DAG
from airflow.configuration import conf


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

store_banner_list = ['Unimarc','Mayorista','Super 10']

PROJECT_NAME = 'evaluate_promotions'
dag_args = {
    'dag_id': 'pull_foward',
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [PROJECT_NAME, 'abravom'],
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['abravom@unidata.cl'],
        'start_date': pendulum.datetime(
            2025, 1, 1,
            tz=pendulum.timezone('America/Santiago')
        ),
        'depends_on_past': False,
        'catchup': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
    }
}

with DAG(**dag_args) as dag:
    EXECUTION_DATE = "{{ dag_run.conf.get('execution_date',dag.timezone.convert(data_interval_start).strftime('%Y-%m-%d')) }}"  # noqa: E501

    promotions_sharepoint_task = ExtendedDataprocCreateBatchOperator(
        task_id = 'get_promotions_from_sharepoint',
        python_script_path=(
            f'{PROJECT_NAME}/'
            'scripts/'
            'get_promotions_from_sharepoint.py'
        ),
        dag_env_config=dag_env_config,
        docker_image_name=PROJECT_NAME,
        pyspark_batch_args=[
            '--project_id', dag_env_config['project_id'],
            '--execution_date', EXECUTION_DATE,
        ],
        include_paths=[
            'common/',
            f'{PROJECT_NAME}/gbq_objects/'
        ],
    )

    main_tasks = []
    pull_tasks = []

    for store_banner in store_banner_list:
        if store_banner == 'Mayorista':
            lower_banner = 'm10'
        elif store_banner == 'Super 10':
            lower_banner = 's10'
        else:
            lower_banner = store_banner.lower()

        main_promotions_task = ExtendedDataprocCreateBatchOperator(
            task_id = f'get_main_promotions_{lower_banner}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                'build_main_promotion_table.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=PROJECT_NAME,
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
                '--store_banner',store_banner,
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],
        )

        compute_pull_foward = ExtendedDataprocCreateBatchOperator(
            task_id = f'compute_pull_foward_{lower_banner}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                f'compute_pull_foward_{lower_banner}.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=PROJECT_NAME,
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
                '--store_banner',store_banner,
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],
        )

        promotions_sharepoint_task >> main_promotions_task
        main_promotions_task >> compute_pull_foward

        main_tasks.append(main_promotions_task)
        pull_tasks.append(compute_pull_foward)


