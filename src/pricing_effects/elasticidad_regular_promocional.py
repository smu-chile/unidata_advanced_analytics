# Default
"""DAG -- elasticidad regular y promocional (sin cascada).

Calcula elasticidad restringida a 1 solo regimen (Regular o
Promocional) por banner fisico, sin cascada de fallback. Vive en el
mismo proyecto que pricing_effects (reutiliza su imagen Docker), pero
como DAG propio y diferenciado -- no se mezcla con el DAG principal.
"""
import json
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

# Mismos scripts/gbq_objects que pricing_effects.py -- misma carpeta,
# misma imagen Docker.
PROJECT_NAME = 'pricing_effects'

# NOTA: solo banners fisicos -- ni el notebook original ni el metodo
# regular/promocional manejan ecommerce (no aparece en ningun lado del
# codigo fuente). Ajustar esta lista si se necesita un subconjunto
# distinto.
STORE_BANNER_LIST = [
    'Unimarc',
    'Super 10',
    'Alvi'
]

# ====================================================================
# INTERRUPTORES -- independientes entre si. Se puede correr solo
# regular, solo promocional, o ambos.
# ====================================================================
EJECUTAR_REGULAR = True
EJECUTAR_PROMOCIONAL = True

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
    'dag_id': 'elasticidad_regular_promocional',
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
# DAG -- regular y promocional corren independientes entre si, y
# ambos son independientes del DAG principal (pricing_effects) --
# ninguno depende de baseline/regresion/elasticidad_general, ni al
# reves. Asume que esas tablas fuente (BASELINE_PANEL,
# TMP_REGRESSION_PROCESSED_DATA_ELASTICITY) ya estan frescas.
# -------------------------------------------------------------------------
with DAG(**dag_args) as dag:
    EXECUTION_DATE = (
        "{{ dag_run.conf.get("
        "'execution_date', "
        "dag.timezone.convert("
        "data_interval_end"
        ").strftime('%Y-%m-%d')) }}"
    )

    regular_tasks = []
    promocional_tasks = []

    for store_banner in STORE_BANNER_LIST:
        banner_suffix = store_banner.replace(' ', '_').lower()
        kwargs_recursos = RECURSOS_EXTRA_POR_BANNER.get(store_banner, {})

        # ---------- Task regular ----------
        if EJECUTAR_REGULAR:
            regular_task = (
                ExtendedDataprocCreateBatchOperator(
                    task_id=f'elasticidad_regular_{banner_suffix}',
                    python_script_path=(
                        f'{PROJECT_NAME}/'
                        'scripts/'
                        'elasticidad_regular.py'
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
            regular_tasks.append(regular_task)

        # ---------- Task promocional ----------
        if EJECUTAR_PROMOCIONAL:
            promocional_task = (
                ExtendedDataprocCreateBatchOperator(
                    task_id=f'elasticidad_promocional_{banner_suffix}',
                    python_script_path=(
                        f'{PROJECT_NAME}/'
                        'scripts/'
                        'elasticidad_promocional.py'
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
            promocional_tasks.append(promocional_task)
