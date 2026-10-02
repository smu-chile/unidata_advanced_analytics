# Default
"""DAG independiente -- tablones de regresion agregados (marca y subcategoria).

Para banners fisicos (Unimarc, Super 10, Alvi). Dispara 2 tareas por
banner, totalmente independientes entre si y sin conexion a ningun otro
DAG (no alimentan elasticidad_general.py ni balance_matrix.py -- son la
base del modelo de elasticidad cruzada, que corre aparte):

    regression_data_marca_{banner}
    regression_data_subcategoria_{banner}

Misma estructura que `pricing_effects_balance_matrix.py` (interruptores
por etapa, recursos por banner, EXECUTION_DATE desde dag_run.conf) --
usado solo como referencia de estilo, no como dependencia.
"""  # noqa: W505
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

PROJECT_NAME = 'pricing_effects'

# Solo banners fisicos -- mismo criterio que el resto del proyecto.
STORE_BANNER_LIST = [
    'Unimarc',
    'Super 10',
    'Alvi',
]

# ====================================================================
# INTERRUPTORES -- 1 por tablon, independientes entre si. Si esta en
# False, esa tarea simplemente no se crea para ningun banner.
# ====================================================================
EJECUTAR_REGRESSION_MARCA = True
EJECUTAR_REGRESSION_SUBCATEGORIA = True

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
    },
}

dag_args = {
    'dag_id': 'pricing_effects_regression_agregada',
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 6,
    'tags': [
        PROJECT_NAME,
        'jsanmartin',
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
            ),
        ),
        'depends_on_past': False,
        'catchup': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(
            minutes=5
        ),
    },
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

    regression_marca_tasks = []
    regression_subcategoria_tasks = []

    for store_banner in STORE_BANNER_LIST:
        banner_suffix = store_banner.replace(' ', '_').lower()
        kwargs_recursos = RECURSOS_EXTRA_POR_BANNER.get(store_banner, {})

        # ---------- Tablon de marca (independiente) ----------
        if EJECUTAR_REGRESSION_MARCA:
            regression_marca_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'regression_data_marca_{banner_suffix}',
                python_script_path=(
                    f'{PROJECT_NAME}/scripts/processed_regression_data_marca.py'
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
            regression_marca_tasks.append(regression_marca_task)

        # ---------- Tablon de subcategoria (independiente) ----------
        if EJECUTAR_REGRESSION_SUBCATEGORIA:
            regression_subcategoria_task = ExtendedDataprocCreateBatchOperator(
                task_id=f'regression_data_subcategoria_{banner_suffix}',
                python_script_path=(
                    f'{PROJECT_NAME}/scripts/processed_regression_data_subcategoria.py'
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
            regression_subcategoria_tasks.append(regression_subcategoria_task)

        # Sin encadenamiento -- marca y subcategoria son independientes
        # entre si, y este DAG no se conecta a ningun otro.
