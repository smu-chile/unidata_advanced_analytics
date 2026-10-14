# Default
"""DAG -- elasticidad cruzada de EAN focales vs sus relacionados.

Por banner fisico: selecciona los EAN focales representativos, estima la
elasticidad cruzada contra su top 10 de sustitutos y top 10 de
complementarios (modelo de SKU) y sube el resultado a
PRECIO_PROMOCIONES.ELASTICITY_CROSS.

Disparo manual (Trigger DAG w/ config), todas las llaves son opcionales:
    {
        "execution_date": "2026-10-14",
        "store_banners": ["Unimarc", "Alvi"],
        "categorias": "ACEITE,CERVEZAS"
    }
  - execution_date: ultimo dia de datos (por defecto, el del DAG run).
  - store_banners: banners a ejecutar (por defecto, todos).
  - categorias: categorias focales separadas por coma (por defecto, todas).
"""
import json  # noqa: I001
import platform
import importlib
from datetime import timedelta

# Pip
import pendulum
from airflow.models import DAG
from airflow.configuration import conf
from airflow.operators.python import ShortCircuitOperator # pyright: ignore[reportMissingImports]


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

# Banners que el DAG sabe ejecutar. El disparo con config elige un
# subconjunto con la llave "store_banners".
STORE_BANNER_LIST = [
    'Unimarc',
    'Super 10',
    'Alvi'
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
    'dag_id': 'elasticidad_cruzada',
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


def banner_habilitado(banner, dag_run=None, **_):  # noqa: ANN001, ANN003, ANN201
    """Indica si el banner fue pedido en la config del disparo."""
    pedidos = ((dag_run.conf or {}) if dag_run else {}).get('store_banners')
    if not pedidos:
        return True
    if isinstance(pedidos, str):
        pedidos = pedidos.split(',')
    pedidos = [p.strip() for p in pedidos if p.strip()]
    desconocidos = set(pedidos) - set(STORE_BANNER_LIST)
    if desconocidos:
        msg = (
            f'store_banners desconocidos: {sorted(desconocidos)}. '
            f'Validos: {STORE_BANNER_LIST}'
        )
        raise ValueError(msg)
    return banner in pedidos


# -------------------------------------------------------------------------
# DAG -- una tarea por banner (en paralelo). Asume que las tablas fuente
# (TMP_REGRESSION_PROCESSED_DATA_ELASTICITY, TMP_PROMOTION_DAILY y los
# modelos de sustitutos y complementarios) ya estan frescas.
# -------------------------------------------------------------------------
with DAG(**dag_args) as dag:
    EXECUTION_DATE = (
        "{{ dag_run.conf.get("
        "'execution_date', "
        "dag.timezone.convert("
        "data_interval_end"
        ").strftime('%Y-%m-%d')) }}"
    )
    CATEGORIAS = "{{ dag_run.conf.get('categorias', '') }}"

    for store_banner in STORE_BANNER_LIST:
        banner_suffix = store_banner.replace(' ', '_').lower()
        kwargs_recursos = RECURSOS_EXTRA_POR_BANNER.get(store_banner, {})

        validar_task = ShortCircuitOperator(
            task_id=f'banner_habilitado_{banner_suffix}',
            python_callable=banner_habilitado,
            op_kwargs={'banner': store_banner},
        )

        elasticidad_cruzada_task = (
            ExtendedDataprocCreateBatchOperator(
                task_id=f'elasticidad_cruzada_{banner_suffix}',
                python_script_path=(
                    f'{PROJECT_NAME}/'
                    'scripts/'
                    'elasticidad_cruzada.py'
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
                    '--categorias',
                    CATEGORIAS,
                ],
                include_paths=[
                    'common/',
                    f'{PROJECT_NAME}/gbq_objects/'
                ],
                **kwargs_recursos,
            )
        )

        validar_task >> elasticidad_cruzada_task
