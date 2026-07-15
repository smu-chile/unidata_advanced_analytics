# Default
import json
import platform
import importlib
from typing import Any
from datetime import timedelta

# pip
import pendulum
from airflow.models import DAG, Variable
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


##########---------- TASK (i) ----------##########
# Parche 1: nombre de script igual al nombre del archivo .py

script0 = 'computing_customer_segmentation_sophistication_stores_id'
script1 = 'product_sensibility_stores_id'
script2 = 'processed_regression_data_stores_id'
script3 = 'product_elasticity_stores_id'
script4 = 'balance_matrix_stores_id_givenlist'

########## --------------------------##############

PROJECT_NAME      = 'balance_matrix_stores_id' #PARCHE
dag_id            = 'balance_matrix_stores_id_givenlist' #PARCHE
schedule_interval =  None
catchup           =  False
start_date        = [2025, 6, 20]
store_banner_list = ['Unimarc']
store_id_list = json.loads(Variable.get('balance_matrix_store_ids', default_var='[]'))  # noqa: AIR311

dag_args = {
    'dag_id': 'balance_matrix_stores_id_givenlist', #PARCHE
    'schedule_interval': schedule_interval,
    'dagrun_timeout': None,
    'catchup': catchup,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [PROJECT_NAME,'rlagosg'], #PARCHE
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['rlagosg@unidata.cl'], #PARCHE
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
        'retry_delay': timedelta(minutes=5)
    }
}


with DAG(**dag_args) as dag:

    EXECUTION_DATE = "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}"  # noqa: E501


    store_id_list = json.loads(
            Variable.get('balance_matrix_store_ids', default_var='[]')  # noqa: AIR311
        )

    previous_group = None

    for store_banner in store_banner_list:
        banner_suffix = store_banner.replace(' ', '_').lower()

        for store_id in store_id_list:
            sid = str(store_id)

            common: dict[str, Any] = dict(  # noqa: C408
                dag_env_config=dag_env_config,
                docker_image_name=f'{PROJECT_NAME}',
                include_paths=['common/', f'{PROJECT_NAME}/gbq_objects/'],
            )

            base_args = [
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
                '--store_banner', store_banner,
                '--store_id', json.dumps([sid])
            ]

            sophistication_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'{script0}_{banner_suffix}_{sid}',
                python_script_path=f'{PROJECT_NAME}/scripts/{script0}.py',
                pyspark_batch_args=[*base_args],
                **common,
            )

            sensibility_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'{script1}_{banner_suffix}_{sid}',
                python_script_path=f'{PROJECT_NAME}/scripts/{script1}.py',
                pyspark_batch_args=[*base_args],
                **common,
            )

            processed_data_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'{script2}_{banner_suffix}_{sid}',
                python_script_path=f'{PROJECT_NAME}/scripts/{script2}.py',
                pyspark_batch_args=[*base_args, '--use', 'ELASTICITY'],  # noqa: E501
                spark_driver_cores=8,
                spark_driver_memory=40,
                **common,
            )

            elasticity_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'{script3}_{banner_suffix}_{sid}',
                python_script_path=f'{PROJECT_NAME}/scripts/{script3}.py',
                pyspark_batch_args=[*base_args],
                spark_driver_cores=8,
                spark_driver_memory=40,
                **common,
            )

            bm_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'{script4}_{banner_suffix}_{sid}',
                python_script_path=f'{PROJECT_NAME}/scripts/{script4}.py',
                pyspark_batch_args=[*base_args, '--suffix', '{{ dag_run.conf.get("suffix", "") }}'],  # noqa: E501
                **common,
            )

            # Encadenamiento dentro del grupo: secuencial por store
            sophistication_task >> sensibility_task >> processed_data_task >> elasticity_task >> bm_task  # noqa: E501

            if previous_group is not None:
                previous_group >> sophistication_task
            previous_group = bm_task

