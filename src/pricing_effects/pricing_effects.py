# Default
import json  # noqa: I001
import platform
import importlib
from datetime import timedelta
# Pip
import pendulum
from airflow.models import DAG
from airflow.configuration import conf
if platform.system() == 'Windows':
    from common.operators.dataproc_create_batch import (
        ExtendedDataprocCreateBatchOperator,
    )
elif platform.system() == 'Linux':
    ExtendedDataprocCreateBatchOperator = (
        importlib.import_module(
            'BRANCH_PLACEHOLDER.'
            'smu-chile.unidata_advanced_analytics.'
            'src.common.operators.dataproc_create_batch'
        )
    ).ExtendedDataprocCreateBatchOperator
else:
    msg = 'Only Linux and Windows are supported.'
    raise NotImplementedError(msg)
# -------------------------------------------------------------------------
# Configuración ambiente
# -------------------------------------------------------------------------
with open(
    f'{conf.get("core", "dags_folder")}/'
    'BRANCH_PLACEHOLDER/'
    'smu-chile/unidata_advanced_analytics/'
    'src/common/constants/dag_env_config.json'
) as f:
    dag_env_config = json.load(f)['BRANCH_PLACEHOLDER']

PROJECT_NAME = 'pricing_effects'

STORE_BANNER_LIST = [
    'Super 10'
    #, 'Ecommerce Alvi'
    #'Unimarc',
    #'Super 10', 'Alvi','Ecommerce Unimarc', 'Ecommerce Alvi',
]

# =============================================================================
# INTERRUPTOR TEMPORAL -- baseline_panel ya esta validado, y mientras se
# ajusta elasticidad_general (varias iteraciones de prueba esperadas), no
# hace falta volver a correrlo cada vez. Poner en True para reactivarlo
# una vez que elasticidad_general quede validado -- NO borrar baseline_task,
# solo se deja de encadenar/crear mientras este flag este en False.
# =============================================================================
EJECUTAR_BASELINE_PANEL = False

RECURSOS_EXTRA_POR_BANNER = {
    'Unimarc': {
        'spark_driver_cores': 8,
        'spark_driver_memory': 40,
    },
    'Super 10': {
        'spark_driver_cores': 8,
        'spark_driver_memory': 40,
    },
}

dag_args = {
    'dag_id': 'pricing_effects',
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 8,
    'tags': [
        PROJECT_NAME,
        'jsanmartin'
    ],
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['jsanmartin@unidata.cl'],
        'start_date': pendulum.datetime(
            2026,
            1,
            1,
            tz=pendulum.timezone(
                'America/Santiago'
            )
        ),
        'depends_on_past': False,
        'catchup': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(
            minutes=5
        )
    }
}
# -------------------------------------------------------------------------
# DAG
# -------------------------------------------------------------------------
with DAG(**dag_args) as dag:
    EXECUTION_DATE = (
        "{{ dag_run.conf.get("
        "'execution_date', "
        "dag.timezone.convert("
        "data_interval_end"
        ").strftime('%Y-%m-%d')) }}"
    )

    baseline_tasks = []
    elasticidad_general_tasks = []

    for store_banner in STORE_BANNER_LIST:
        banner_suffix = store_banner.replace(' ', '_').lower()
        kwargs_recursos = RECURSOS_EXTRA_POR_BANNER.get(store_banner, {})

        # ---------- Task baseline (condicional al interruptor) ----------
        baseline_task = None
        if EJECUTAR_BASELINE_PANEL:
            baseline_task = (
                ExtendedDataprocCreateBatchOperator(
                    task_id=f'baseline_{banner_suffix}',
                    python_script_path=(
                        f'{PROJECT_NAME}/'
                        'scripts/'
                        'baseline.py'
                    ),
                    dag_env_config=dag_env_config,
                    docker_image_name=PROJECT_NAME,
                    pyspark_batch_args=[
                        '--project_id',
                        dag_env_config['project_id'],
                        '--execution_date',
                        EXECUTION_DATE,
                        '--store_banner',
                        store_banner,
                    ],
                    include_paths=[
                        'common/',
                        f'{PROJECT_NAME}/gbq_objects/'
                    ],
                    **kwargs_recursos,
                )
            )
            baseline_tasks.append(baseline_task)

        # ---------- Task elasticidad_general ----------
        elasticidad_general_task = (
            ExtendedDataprocCreateBatchOperator(
                task_id=f'elasticidad_general_{banner_suffix}',
                python_script_path=(
                    f'{PROJECT_NAME}/'
                    'scripts/'
                    'elasticidad_general.py'
                ),
                dag_env_config=dag_env_config,
                docker_image_name=PROJECT_NAME,
                pyspark_batch_args=[
                    '--project_id',
                    dag_env_config['project_id'],
                    '--execution_date',
                    EXECUTION_DATE,
                    '--store_banner',
                    store_banner,
                ],
                include_paths=[
                    'common/',
                    f'{PROJECT_NAME}/gbq_objects/'
                ],
                **kwargs_recursos,
            )
        )

        # Dependencia por banner: baseline -> elasticidad_general, SOLO
        # si baseline esta activo -- si esta desactivado, elasticidad_general
        # corre directo, sin esperar nada.
        if baseline_task is not None:
            baseline_task >> elasticidad_general_task

        elasticidad_general_tasks.append(elasticidad_general_task)
