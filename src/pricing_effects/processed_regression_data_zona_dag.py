# Default
"""DAG -- tablones de regresion por zona comercial (Unimarc).

Resuelve la tabla store_id -> zona (SEGMENTACION_ZONAS_BM_PRICING,
liviano, sin tocar transacciones) y arma el tablon de regresion para
cada una de las 7 zonas comerciales. Fuente madre para el futuro
calculo de elasticidad por zona --.
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

PROJECT_NAME = 'pricing_effects'

# Exclusivo Unimarc -- SEGMENTACION_ZONAS_BM_PRICING es una
# segmentacion propia de ese banner.
STORE_BANNER = 'Unimarc'

ZONAS = [
    'Austral'
    #,
    #'Baja Competencia',
    #'Competencia Media',
    #'Competencia Regional',
    #'Hipercompetitiva',
    #'Norte',
    #'Premium',
]

# Mismo ajuste de recursos que el resto de los DAGs de este proyecto --
# Unimarc es el banner con mas materiales/transacciones.
RECURSOS_EXTRA = {
    'spark_driver_cores': 8,
    'spark_driver_memory': 40,
}

dag_args = {
    'dag_id': 'processed_regression_data_zona',
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    # Mismo criterio que processed_regression_data_region_dag.py --
    # consultas pesadas contra VW_SALES_ITEM, se evita contencion de
    # slots de BigQuery corriendo pocas en simultaneo.
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
# DAG -- resolver corre 1 sola vez, deja TMP_TIENDAS_ACTIVAS_POR_ZONA
# lista para que las 7 tareas de zona solo lean (barato), en vez de
# resolver cada una por su cuenta.
# -------------------------------------------------------------------------
with DAG(**dag_args) as dag:
    EXECUTION_DATE = (
        "{{ dag_run.conf.get("
        "'execution_date', "
        "dag.timezone.convert("
        "data_interval_end"
        ").strftime('%Y-%m-%d')) }}"
    )

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

    zona_tasks = []
    for zona in ZONAS:
        zona_suffix = zona.replace(' ', '_').lower()

        zona_task = ExtendedDataprocCreateBatchOperator(
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

        resolver_task >> zona_task
        zona_tasks.append(zona_task)
