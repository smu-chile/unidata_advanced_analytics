# Default
"""DAG unificado -- regresion, baseline, elasticidad, sensibilidad y BM.

Para banners fisicos (Unimarc, Super 10, Alvi). Cadena por banner:

    regresion? -> baseline? -> elasticidad_general?  --\
                                                   --> balance_matrix?
    sensibilidad? (rama independiente, en paralelo)   --/

Cada etapa tiene su propio interruptor -- si esta apagada, se salta
sin dejar ningun eslabon roto (misma logica ya usada para baseline en
versiones anteriores de este DAG). balance_matrix, si esta activo,
espera a que terminen AMBAS ramas para ese banner (si estan activas);
si alguna rama esta apagada, balance_matrix igual corre, leyendo lo
que ya exista en ELASTICITY_PR/PRODUCT_SENSIBILITY de una corrida
anterior.

NOTA IMPORTANTE: a diferencia de versiones anteriores de este DAG,
este NO incluye banners de ecommerce. Si se necesita elasticidad
de ecommerce, corre por fuera de este DAG.
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

# Solo banners fisicos -- decision confirmada, ver docstring del
# modulo.
STORE_BANNER_LIST = [
    'Unimarc',
    'Super 10',
    'Alvi'
]

# ====================================================================
# INTERRUPTORES -- 6 en total, cada etapa controlable por separado.
# Ninguna etapa corre "siempre" -- si esta en False, esa tarea
# simplemente no se crea para ese banner, y la cadena salta al
# siguiente eslabon activo.
# ====================================================================
EJECUTAR_REGRESSION_FISICOS = True
EJECUTAR_BASELINE_PANEL = True
EJECUTAR_ELASTICIDAD_GENERAL = True
EJECUTAR_SENSIBILIDAD = True
EJECUTAR_BALANCE_MATRIX = True

# Controla si balance_matrix, cuando corre, tambien sube el Excel a
# Sharepoint (ademas de BigQuery, que siempre se hace). Se pasa como
# argumento al script, no como constante fija -- ver
# balance_matrix.py --subir_a_sharepoint.
SUBIR_A_SHAREPOINT = False

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
    'dag_id': 'pricing_effects_balance_matrix',
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

    regression_tasks = []
    baseline_tasks = []
    elasticidad_general_tasks = []
    sensibilidad_tasks = []
    balance_matrix_tasks = []

    for store_banner in STORE_BANNER_LIST:
        banner_suffix = store_banner.replace(' ', '_').lower()
        kwargs_recursos = RECURSOS_EXTRA_POR_BANNER.get(store_banner, {})

        # ---------- Rama elasticidad: regresion (condicional) ----------
        regression_task = None
        if EJECUTAR_REGRESSION_FISICOS:
            regression_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'regression_data_fisico_{banner_suffix}',
                python_script_path=(
                    f'{PROJECT_NAME}/scripts/processed_regression_data.py'
                ),
                dag_env_config=dag_env_config,
                docker_image_name=PROJECT_NAME,
                pyspark_batch_args=[
                    '--project_id', dag_env_config['project_id'],
                    '--execution_date', EXECUTION_DATE,
                    '--store_banner', store_banner,
                    '--use', 'ELASTICITY',
                ],
                include_paths=['common/', f'{PROJECT_NAME}/gbq_objects/'],
                **kwargs_recursos,
            )
            regression_tasks.append(regression_task)

        # ---------- Rama elasticidad: baseline (condicional) ----------
        baseline_task = None
        if EJECUTAR_BASELINE_PANEL:
            baseline_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'baseline_{banner_suffix}',
                python_script_path=f'{PROJECT_NAME}/scripts/baseline.py',
                dag_env_config=dag_env_config,
                docker_image_name=PROJECT_NAME,
                pyspark_batch_args=[
                    '--project_id', dag_env_config['project_id'],
                    '--execution_date', EXECUTION_DATE,
                    '--store_banner', store_banner,
                ],
                include_paths=['common/', f'{PROJECT_NAME}/gbq_objects/'],
                **kwargs_recursos,
            )
            baseline_tasks.append(baseline_task)

        # ---------- Rama elasticidad: elasticidad_general ----------
        elasticidad_general_task = None
        if EJECUTAR_ELASTICIDAD_GENERAL:
            elasticidad_general_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'elasticidad_general_{banner_suffix}',
                python_script_path=(
                    f'{PROJECT_NAME}/scripts/elasticidad_general.py'
                ),
                dag_env_config=dag_env_config,
                docker_image_name=PROJECT_NAME,
                pyspark_batch_args=[
                    '--project_id', dag_env_config['project_id'],
                    '--execution_date', EXECUTION_DATE,
                    '--store_banner', store_banner,
                ],
                include_paths=['common/', f'{PROJECT_NAME}/gbq_objects/'],
                **kwargs_recursos,
            )
            elasticidad_general_tasks.append(elasticidad_general_task)

        # Encadenamiento dinamico de la rama elasticidad -- solo entre
        # etapas activas.
        cadena_elasticidad = [
            t for t in (regression_task, baseline_task, elasticidad_general_task)
            if t is not None
        ]
        for tarea_anterior, tarea_siguiente in itertools.pairwise(cadena_elasticidad):
            tarea_anterior >> tarea_siguiente

        # ---------- Rama sensibilidad (independiente) ----------
        sensibilidad_task = None
        if EJECUTAR_SENSIBILIDAD:
            sensibilidad_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'product_sensibility_{banner_suffix}',
                python_script_path=(
                    f'{PROJECT_NAME}/scripts/product_sensibility.py'
                ),
                dag_env_config=dag_env_config,
                docker_image_name=PROJECT_NAME,
                pyspark_batch_args=[
                    '--project_id', dag_env_config['project_id'],
                    '--execution_date', EXECUTION_DATE,
                    '--store_banner', store_banner,
                ],
                include_paths=['common/', f'{PROJECT_NAME}/gbq_objects/'],
                **kwargs_recursos,
            )
            sensibilidad_tasks.append(sensibilidad_task)

        # ---------- balance_matrix (condicional, converge ambas ramas) ---
        if EJECUTAR_BALANCE_MATRIX:
            balance_matrix_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'balance_matrix_{banner_suffix}',
                python_script_path=(
                    f'{PROJECT_NAME}/scripts/balance_matrix.py'
                ),
                dag_env_config=dag_env_config,
                docker_image_name=PROJECT_NAME,
                pyspark_batch_args=[
                    '--project_id', dag_env_config['project_id'],
                    '--execution_date', EXECUTION_DATE,
                    '--store_banner', store_banner,
                    '--subir_a_sharepoint', str(SUBIR_A_SHAREPOINT),
                ],
                include_paths=['common/', f'{PROJECT_NAME}/gbq_objects/'],
                **kwargs_recursos,
            )
            balance_matrix_tasks.append(balance_matrix_task)

            # balance_matrix espera al ULTIMO eslabon activo de cada
            # rama -- si una rama entera esta apagada, no espera nada
            # de ella (lee lo que ya exista).
            if cadena_elasticidad:
                cadena_elasticidad[-1] >> balance_matrix_task
            if sensibilidad_task is not None:
                sensibilidad_task >> balance_matrix_task
