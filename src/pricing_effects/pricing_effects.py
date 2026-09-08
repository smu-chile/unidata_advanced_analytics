# Default
"""DAG unificado -- regresion, baseline y elasticidad_general.

Regenera las tablas de regresion (fisica y ecommerce), el baseline
panel, y calcula elasticidad -- para banners fisicos y ecommerce,
todos en el esquema PRECIO_PROMOCIONES. Cada etapa es controlada por
un interruptor True/False independiente.
"""
import json
import platform
import importlib
import itertools
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

# ====================================================================
# INTERRUPTORES -- controlan que etapas se disparan en cada corrida.
# Se puede desactivar cualquier combinacion; elasticidad_general no
# tiene interruptor propio porque es el objetivo final de la cadena --
# siempre corre, encadenada despues de lo que este activo.
# ====================================================================
EJECUTAR_REGRESSION_FISICOS = True
EJECUTAR_REGRESSION_ECOMMERCE = True
EJECUTAR_BASELINE_PANEL = True

# Mismo diccionario que ya usan baseline.py y elasticidad_general.py --
# fuente unica de verdad para decidir que banners son "ecommerce" (y
# que nombre fisico subyacente usar para la regresion), en vez de
# adivinar por texto (ej. .startswith('Ecommerce')).
MAPA_BANNER_REGRESSION_ECOMMERCE = {
    'Ecommerce Unimarc': 'Unimarc',
    'Ecommerce Alvi': 'Alvi',
}

STORE_BANNER_LIST = [
    'Unimarc',
    'Super 10',
    'Alvi',
    'Ecommerce Unimarc',
    'Ecommerce Alvi',
]

RECURSOS_EXTRA_POR_BANNER = {
    'Unimarc': {
        'spark_driver_cores': 8,
        'spark_driver_memory': 40,
    },
    'Super 10': {
        'spark_driver_cores': 8,
        'spark_driver_memory': 40,
    },
    'Alvi': {
            'spark_driver_cores': 8,
            'spark_driver_memory': 40,
        }
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
# DAG -- unificado: regresion (fisica/ecommerce) -> baseline ->
# elasticidad_general, por banner, saltando cualquier etapa cuyo
# interruptor este en False.
# -------------------------------------------------------------------------
with DAG(**dag_args) as dag:
    EXECUTION_DATE = (
        "{{ dag_run.conf.get("
        "'execution_date', "
        "dag.timezone.convert("
        "data_interval_end"
        ").strftime('%Y-%m-%d')) }}"
    )

    regression_tasks = []
    baseline_tasks = []
    elasticidad_general_tasks = []

    for store_banner in STORE_BANNER_LIST:
        banner_suffix = store_banner.replace(' ', '_').lower()
        kwargs_recursos = RECURSOS_EXTRA_POR_BANNER.get(store_banner, {})
        es_ecommerce = store_banner in MAPA_BANNER_REGRESSION_ECOMMERCE

        # ---------- Task regresion (fisica o ecommerce) ----------
        regression_task = None
        if es_ecommerce and EJECUTAR_REGRESSION_ECOMMERCE:
            regression_task = (
                ExtendedDataprocCreateBatchOperator(
                    task_id=f'regression_data_ecommerce_{banner_suffix}',
                    python_script_path=(
                        f'{PROJECT_NAME}/'
                        'scripts/'
                        'ecommerce_processed_regression_data.py'
                    ),
                    dag_env_config=dag_env_config,
                    docker_image_name=PROJECT_NAME,
                    pyspark_batch_args=[
                        '--project_id',
                        dag_env_config['project_id'],
                        '--execution_date',
                        EXECUTION_DATE,
                        '--store_banner',
                        # el script de regresion ecommerce necesita el
                        # nombre SIN el prefijo "Ecommerce "
                        MAPA_BANNER_REGRESSION_ECOMMERCE[store_banner],
                        '--use',
                        'ELASTICITY',
                    ],
                    include_paths=[
                        'common/',
                        f'{PROJECT_NAME}/gbq_objects/'
                    ],
                    **kwargs_recursos,
                )
            )
        elif not es_ecommerce and EJECUTAR_REGRESSION_FISICOS:
            regression_task = (
                ExtendedDataprocCreateBatchOperator(
                    task_id=f'regression_data_fisico_{banner_suffix}',
                    python_script_path=(
                        f'{PROJECT_NAME}/'
                        'scripts/'
                        'processed_regression_data.py'
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
                        '--use',
                        'ELASTICITY',
                    ],
                    include_paths=[
                        'common/',
                        f'{PROJECT_NAME}/gbq_objects/'
                    ],
                    **kwargs_recursos,
                )
            )
        if regression_task is not None:
            regression_tasks.append(regression_task)

        # ---------- Task baseline (condicional) ----------
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

        # ---------- Task elasticidad_general (siempre corre) ----------
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
        elasticidad_general_tasks.append(elasticidad_general_task)

        # ---------- Encadenamiento dinamico ----------
        # Se arma la cadena en orden (regresion -> baseline ->
        # elasticidad),
        # conectando unicamente las etapas que efectivamente se crearon
        # para este banner. Si una etapa esta en False, se salta sin
        # dejar ningun eslabon roto.
        cadena = [t for t in (regression_task, baseline_task) if t is not None]
        cadena.append(elasticidad_general_task)
        for tarea_anterior, tarea_siguiente in itertools.pairwise(cadena):
            tarea_anterior >> tarea_siguiente
