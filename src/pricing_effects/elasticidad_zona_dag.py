# Default
"""DAG -- elasticidad por zona comercial (Unimarc).

Calcula elasticidad para cada 1 de las 7 zonas comerciales de Unimarc
(ver SEGMENTACION_ZONAS_BM_PRICING), con cascada de fallback completa
DENTRO de cada zona. Depende de que
processed_regression_data_zona_dag.py ya haya corrido -- no lo dispara
automaticamente, cada DAG se controla por separado.
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

# Exclusivo Unimarc -- misma segmentacion que
# processed_regression_data_zona_dag.py.
STORE_BANNER = 'Unimarc'

ZONAS = [
    'Austral'
    #,'Baja Competencia',
    #'Competencia Media',
    #'Competencia Regional',
    #'Hipercompetitiva',
    #'Norte',
    #'Premium',
]

# Mismo ajuste de recursos que el resto de los DAGs de este proyecto.
RECURSOS_EXTRA = {
    'spark_driver_cores': 8,
    'spark_driver_memory': 40,
}

dag_args = {
    'dag_id': 'elasticidad_zona',
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
# DAG -- 1 tarea por zona, todas independientes entre si. No depende
# del DAG de regresion por zona -- asume que TMP_REGRESSION_
# PROCESSED_DATA_ELASTICITY_ZONA ya esta fresca cuando esto corre.
# -------------------------------------------------------------------------
with DAG(**dag_args) as dag:
    EXECUTION_DATE = (
        "{{ dag_run.conf.get("
        "'execution_date', "
        "dag.timezone.convert("
        "data_interval_end"
        ").strftime('%Y-%m-%d')) }}"
    )

    zona_tasks = []
    for zona in ZONAS:
        zona_suffix = zona.replace(' ', '_').lower()

        zona_task = ExtendedDataprocCreateBatchOperator(
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

        zona_tasks.append(zona_task)
