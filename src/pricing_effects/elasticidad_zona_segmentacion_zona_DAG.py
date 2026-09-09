# Default
"""DAG unificado -- regresion y elasticidad por zona comercial (Unimarc).

Resuelve store_id -> zona, arma los tablones de regresion por zona, y
calcula elasticidad -- las 3 etapas encadenadas por zona, cada 1
controlable por separado via interruptor.
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


STORE_BANNER = 'Unimarc'

ZONAS = [
    'Austral',
    'Baja Competencia',
    'Competencia Media',
    'Competencia Regional',
    'Hipercompetitiva',
    'Norte',
    'Premium',
]

# ====================================================================
# INTERRUPTOR -- controla resolver + las 7 tareas de regresion.
# elasticidad_zona NO tiene interruptor propio -- es el objetivo final
# de la cadena, siempre corre (encadenada despues de lo que este
# activo, igual que en pricing_effects.py).
# ====================================================================
EJECUTAR_REGRESSION_ZONA = True

RECURSOS_EXTRA = {
    'spark_driver_cores': 8,
    'spark_driver_memory': 40,
}

dag_args = {
    'dag_id': 'elasticidad_zona_segmentacion_zona_DAG',
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 4,
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
# DAG -- resolver (1x, condicional) -> regresion por zona (7x,
# condicional) -> elasticidad por zona (7x, siempre). Encadenamiento
# dinamico por zona, saltando cualquier etapa en False, mismo patron
# que pricing_effects.py.
# -------------------------------------------------------------------------
with DAG(**dag_args) as dag:
    EXECUTION_DATE = (
        "{{ dag_run.conf.get("
        "'execution_date', "
        "dag.timezone.convert("
        "data_interval_end"
        ").strftime('%Y-%m-%d')) }}"
    )

    resolver_task = None
    if EJECUTAR_REGRESSION_ZONA:
        resolver_task = ExtendedDataprocCreateBatchOperator(
            task_id='resolver_tiendas_zona',
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                'resolver_tiendas_zona.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=PROJECT_NAME,
            pyspark_batch_args=[
                '--project_id',
                dag_env_config['project_id'],
                '--execution_date',
                EXECUTION_DATE,
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],
        )

    regression_tasks = []
    elasticidad_tasks = []

    for zona in ZONAS:
        zona_suffix = zona.replace(' ', '_').lower()

        # ---------- Task regresion (condicional) ----------
        regression_task = None
        if EJECUTAR_REGRESSION_ZONA:
            regression_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'regression_data_zona_{zona_suffix}',
                python_script_path=(
                    f'{PROJECT_NAME}/'
                    'scripts/'
                    'processed_regression_data_zona.py'
                ),
                dag_env_config=dag_env_config,
                docker_image_name=PROJECT_NAME,
                pyspark_batch_args=[
                    '--project_id',
                    dag_env_config['project_id'],
                    '--execution_date',
                    EXECUTION_DATE,
                    '--store_banner',
                    STORE_BANNER,
                    '--use',
                    'ELASTICITY',
                    '--zona',
                    zona,
                ],
                include_paths=[
                    'common/',
                    f'{PROJECT_NAME}/gbq_objects/'
                ],
                **RECURSOS_EXTRA,
            )
            regression_tasks.append(regression_task)

        # ---------- Task elasticidad (siempre corre) ----------
        elasticidad_task = ExtendedDataprocCreateBatchOperator(
            task_id=f'elasticidad_zona_{zona_suffix}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                'scripts/'
                'elasticidad_zona.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=PROJECT_NAME,
            pyspark_batch_args=[
                '--project_id',
                dag_env_config['project_id'],
                '--execution_date',
                EXECUTION_DATE,
                '--store_banner',
                STORE_BANNER,
                '--zona',
                zona,
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/gbq_objects/'
            ],
            **RECURSOS_EXTRA,
        )
        elasticidad_tasks.append(elasticidad_task)

        # ---------- Encadenamiento dinamico ----------
        # resolver -> regresion (si esta activa) -> elasticidad.
        # Si regresion esta apagada, elasticidad corre directo (sigue
        # dependiendo solo del resolver si este llegara a estar
        # activo sin regresion -- caso raro, pero seguro).
        cadena = [t for t in (resolver_task, regression_task, elasticidad_task) if t is not None]
        for tarea_anterior, tarea_siguiente in itertools.pairwise(cadena):
            tarea_anterior >> tarea_siguiente
