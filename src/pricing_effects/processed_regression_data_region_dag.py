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

STORE_BANNER_LIST = ['Unimarc', 'Super 10', 'Alvi']

REGIONES = [
    'I Tarapacá', 'II Antofagasta'
    #, 'III Atacama', 'IV Región de Coquimbo',
    #'V Región de Valparaíso', 'VI Región de OHiggins',
    # 'VII Región del Maule',
    #'VIII Región del Bío-Bío', 'IX Región de la Araucanía',
    # 'X Región de Los Lagos',
    #'XI Región de Aysén', 'XII Magallanes', 'XIII Región Metropolitana',
    #'XIV Región de Los Ríos', 'XV Arica y Parinacota',
    # 'XVI Región de Ñuble',
]

# Unimarc es, por lejos, el banner con mas materiales -- el driver
# fallaba con SIGKILL (exit 137, presion de memoria) usando los
# recursos por defecto. Se le da mas poder solo a este banner; el
# resto sigue con la configuracion default del operador.
RECURSOS_EXTRA_POR_BANNER = {
    'Unimarc': {
        'spark_driver_cores': 8,
        'spark_driver_memory': 40,
    },
}

dag_args = {
    'dag_id': 'processed_regression_data_region',
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    # El DAG original (givenlist.py) usa concurrency=1 -- nunca corre 2
    # consultas pesadas contra VW_SALES_ITEM al mismo tiempo. 8 aca
    # generaba contencion real de slots de BigQuery (varias corridas
    # pesadas compitiendo a la vez), explicando el cuelgue observado.
    # Se deja en 2 como punto intermedio conservador -- ajustar si se
    # confirma que sigue habiendo contencion.
    'concurrency': 2,
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

    # ---------- Tarea resolver -- corre 1 sola vez, deja la tabla
    # TMP_TIENDAS_ACTIVAS_POR_REGION lista para que las 48 tareas de
    # region solo lean (barato), en vez de resolver cada una por su
    # cuenta contra las tablas de transacciones (caro, x48).
    resolver_task = ExtendedDataprocCreateBatchOperator(
        task_id='resolver_tiendas_activas_region',
        python_script_path=(
            f'{PROJECT_NAME}/'
            'scripts/'
            'resolver_tiendas_activas_region.py'
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

    for store_banner in STORE_BANNER_LIST:
        banner_suffix = store_banner.replace(' ', '_').lower()
        kwargs_recursos = RECURSOS_EXTRA_POR_BANNER.get(store_banner, {})

        for region in REGIONES:
            # sufijo de tarea legible y estable -- sin tildes ni espacios
            region_suffix = (
                region.lower().replace(' ', '_').replace('í', 'i')
                .replace('ó', 'o').replace('é', 'e').replace('á', 'a')
                .replace('ñ', 'n').replace('ú', 'u')
            )

            regression_data_task = (
                ExtendedDataprocCreateBatchOperator(
                    task_id=f'regression_data_{banner_suffix}_{region_suffix}',
                    python_script_path=(
                        f'{PROJECT_NAME}/'
                        'scripts/'
                        'processed_regression_data_region.py'
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
                        '--region',
                        region,
                    ],
                    include_paths=[
                        'common/',
                        f'{PROJECT_NAME}/gbq_objects/'
                    ],
                    **kwargs_recursos,
                )
            )

            resolver_task >> regression_data_task
