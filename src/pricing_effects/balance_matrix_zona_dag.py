# Default
r"""DAG unificado -- regresion, elasticidad, sensibilidad y BM por zona.

Para las 7 zonas comerciales de Unimarc. Cadena por zona:
    regresion? -> elasticidad_zona?  --\
                                   --> balance_matrix_zona?
    sensibilidad_zona? (rama independiente, en paralelo)   --/

Cada etapa tiene su propio interruptor -- si esta apagada, se salta
sin dejar ningun eslabon roto (mismo patron que
pricing_effects_balance_matrix.py, la version por banner completo).
balance_matrix_zona, si esta activo, espera a que terminen AMBAS ramas
para esa zona (si estan activas); si alguna rama esta apagada,
balance_matrix_zona igual corre, leyendo lo que ya exista en
ELASTICITY_ZONA/PRODUCT_SENSIBILITY_ZONA de una corrida anterior.

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

# Exclusivo Unimarc -- SEGMENTACION_ZONAS_BM_PRICING es propia de ese
# banner.
STORE_BANNER = 'Unimarc'

ZONAS = [
    'Austral'#,
    #'Baja Competencia',
    #'Competencia Media',
    #'Competencia Regional',
    #'Hipercompetitiva',
    #'Norte',
    #'Premium',
]

# ====================================================================
# INTERRUPTORES -- 4 en total, cada etapa controlable por separado.
# Ninguna corre "siempre" -- si esta en False, esa tarea simplemente no
# se crea para esa zona, y la cadena salta al siguiente eslabon activo.
# ====================================================================
EJECUTAR_REGRESSION_ZONA = False
EJECUTAR_ELASTICIDAD_ZONA = False
EJECUTAR_SENSIBILIDAD_ZONA = True
EJECUTAR_BALANCE_MATRIX_ZONA = True

# Controla si balance_matrix_zona, cuando corre, tambien sube el Excel
# a Sharepoint (ademas de BigQuery, que siempre se hace).
SUBIR_A_SHAREPOINT = False

RECURSOS_EXTRA = {
    'spark_driver_cores': 8,
    'spark_driver_memory': 40,
}

dag_args = {
    'dag_id': 'balance_matrix_zona',
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

    # ---------- resolver_tiendas_zona (1x, condicional) ----------
    resolver_task = None
    if EJECUTAR_REGRESSION_ZONA:
        resolver_task = ExtendedDataprocCreateBatchOperator(
            task_id='resolver_tiendas_zona',
            python_script_path=(
                f'{PROJECT_NAME}/scripts/resolver_tiendas_zona.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=PROJECT_NAME,
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
            ],
            include_paths=['common/', f'{PROJECT_NAME}/gbq_objects/'],
        )

    regression_tasks = []
    elasticidad_tasks = []
    sensibilidad_tasks = []
    balance_matrix_tasks = []

    for zona in ZONAS:
        zona_suffix = zona.replace(' ', '_').lower()

        # ---------- Rama elasticidad: regresion (condicional) ----------
        regression_task = None
        if EJECUTAR_REGRESSION_ZONA:
            regression_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'regression_data_zona_{zona_suffix}',
                python_script_path=(
                    f'{PROJECT_NAME}/scripts/processed_regression_data_zona.py'
                ),
                dag_env_config=dag_env_config,
                docker_image_name=PROJECT_NAME,
                pyspark_batch_args=[
                    '--project_id', dag_env_config['project_id'],
                    '--execution_date', EXECUTION_DATE,
                    '--store_banner', STORE_BANNER,
                    '--use', 'ELASTICITY',
                    '--zona', zona,
                ],
                include_paths=['common/', f'{PROJECT_NAME}/gbq_objects/'],
                **RECURSOS_EXTRA,
            )
            regression_tasks.append(regression_task)
            if resolver_task is not None:
                resolver_task >> regression_task

        # ---------- Rama elasticidad: elasticidad_zona (condicional) ---
        elasticidad_task = None
        if EJECUTAR_ELASTICIDAD_ZONA:
            elasticidad_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'elasticidad_zona_{zona_suffix}',
                python_script_path=(
                    f'{PROJECT_NAME}/scripts/elasticidad_zona.py'
                ),
                dag_env_config=dag_env_config,
                docker_image_name=PROJECT_NAME,
                pyspark_batch_args=[
                    '--project_id', dag_env_config['project_id'],
                    '--execution_date', EXECUTION_DATE,
                    '--store_banner', STORE_BANNER,
                    '--zona', zona,
                ],
                include_paths=['common/', f'{PROJECT_NAME}/gbq_objects/'],
                **RECURSOS_EXTRA,
            )
            elasticidad_tasks.append(elasticidad_task)

        # Encadenamiento dinamico de la rama elasticidad -- solo entre
        # etapas activas.
        cadena_elasticidad = [
            t for t in (regression_task, elasticidad_task) if t is not None
        ]
        for tarea_anterior, tarea_siguiente in itertools.pairwise(cadena_elasticidad):
            tarea_anterior >> tarea_siguiente

        # ---------- Rama sensibilidad (independiente) ----------
        sensibilidad_task = None
        if EJECUTAR_SENSIBILIDAD_ZONA:
            sensibilidad_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'product_sensibility_zona_{zona_suffix}',
                python_script_path=(
                    f'{PROJECT_NAME}/scripts/product_sensibility_zona.py'
                ),
                dag_env_config=dag_env_config,
                docker_image_name=PROJECT_NAME,
                pyspark_batch_args=[
                    '--project_id', dag_env_config['project_id'],
                    '--execution_date', EXECUTION_DATE,
                    '--store_banner', STORE_BANNER,
                    '--zona', zona,
                ],
                include_paths=['common/', f'{PROJECT_NAME}/gbq_objects/'],
                **RECURSOS_EXTRA,
            )
            sensibilidad_tasks.append(sensibilidad_task)

        # ---------- balance_matrix_zona (converge ambas ramas) ----------
        if EJECUTAR_BALANCE_MATRIX_ZONA:
            balance_matrix_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'balance_matrix_zona_{zona_suffix}',
                python_script_path=(
                    f'{PROJECT_NAME}/scripts/balance_matrix_zona.py'
                ),
                dag_env_config=dag_env_config,
                docker_image_name=PROJECT_NAME,
                pyspark_batch_args=[
                    '--project_id', dag_env_config['project_id'],
                    '--execution_date', EXECUTION_DATE,
                    '--store_banner', STORE_BANNER,
                    '--zona', zona,
                    '--subir_a_sharepoint', str(SUBIR_A_SHAREPOINT),
                ],
                include_paths=['common/', f'{PROJECT_NAME}/gbq_objects/'],
                **RECURSOS_EXTRA,
            )
            balance_matrix_tasks.append(balance_matrix_task)

            if cadena_elasticidad:
                cadena_elasticidad[-1] >> balance_matrix_task
            if sensibilidad_task is not None:
                sensibilidad_task >> balance_matrix_task
