# Default
"""DAG unificado -- regresion, elasticidad, sensibilidad y BM por zona.

Para las 7 zonas comerciales de Unimarc, con FASES secuenciales (no
las 4 etapas compitiendo a la vez por el mismo pool de slots de
BigQuery -- causa mas probable de la falla observada al correr todo
junto):

    FASE 1: Regresion (7 zonas)
        |  (barrera -- TODAS deben terminar)
    FASE 2: Elasticidad (7 zonas, depende de Regresion)
        |  (barrera -- TODAS deben terminar)
    FASE 3: Sensibilidad (7 zonas, independiente de datos, pero
        |   secuenciada igual para no competir por recursos)
        |  (barrera -- TODAS deben terminar)
    FASE 4: Balance Matrix (7 zonas, usa Elasticidad + Sensibilidad)

Dentro de cada fase activa, el 'concurrency' del DAG sigue limitando
cuantas de las 7 tareas corren en simultaneo -- las barreras evitan
que fases DISTINTAS se crucen entre si, no reemplazan ese control.

Cada fase tiene su propio interruptor -- si esta apagada, sus tareas
no se crean, y la barrera de la fase activa siguiente se conecta
directo a la ultima fase activa anterior (sin dejar ningun eslabon
roto).

Reemplaza a balance_matrix_zona_dag.py (la version anterior, con las
4 etapas corriendo sin secuenciar entre si).
"""
import json
import platform
import importlib
from datetime import timedelta

# Pip
import pendulum
from airflow.models import DAG
from airflow.configuration import conf
from airflow.operators.empty import EmptyOperator


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
# INTERRUPTORES -- 4 en total, 1 por fase. Si una fase esta en False,
# sus tareas no se crean, y la barrera se conecta directo entre las
# fases activas vecinas.
# ====================================================================
EJECUTAR_REGRESSION_ZONA = False
EJECUTAR_ELASTICIDAD_ZONA = True
EJECUTAR_SENSIBILIDAD_ZONA = True
EJECUTAR_BALANCE_MATRIX_ZONA = True

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
    # Limita cuantas tareas corren a la vez DENTRO de una fase activa
    # -- las barreras entre fases son una proteccion complementaria,
    # no un reemplazo de este limite.
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

    # punto_enganche: lista de tareas de la ULTIMA fase activa vista
    # hasta ahora -- la siguiente fase activa se conecta desde aca.
    # Empieza vacia (None = nada de que depender todavia).
    punto_enganche = None

    # ---------- FASE 1: Regresion ----------
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
        for zona in ZONAS:
            zona_suffix = zona.replace(' ', '_').lower()
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
            resolver_task >> regression_task
            regression_tasks.append(regression_task)

        fin_fase_regresion = EmptyOperator(task_id='fin_fase_regresion')
        regression_tasks >> fin_fase_regresion
        punto_enganche = [fin_fase_regresion]

    # ---------- FASE 2: Elasticidad ----------
    if EJECUTAR_ELASTICIDAD_ZONA:
        elasticidad_tasks = []
        for zona in ZONAS:
            zona_suffix = zona.replace(' ', '_').lower()
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
            if punto_enganche is not None:
                punto_enganche >> elasticidad_task
            elasticidad_tasks.append(elasticidad_task)

        fin_fase_elasticidad = EmptyOperator(task_id='fin_fase_elasticidad')
        elasticidad_tasks >> fin_fase_elasticidad
        punto_enganche = [fin_fase_elasticidad]

    # ---------- FASE 3: Sensibilidad ----------
    if EJECUTAR_SENSIBILIDAD_ZONA:
        sensibilidad_tasks = []
        for zona in ZONAS:
            zona_suffix = zona.replace(' ', '_').lower()
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
            if punto_enganche is not None:
                punto_enganche >> sensibilidad_task
            sensibilidad_tasks.append(sensibilidad_task)

        fin_fase_sensibilidad = EmptyOperator(task_id='fin_fase_sensibilidad')
        sensibilidad_tasks >> fin_fase_sensibilidad
        punto_enganche = [fin_fase_sensibilidad]

    # ---------- FASE 4: Balance Matrix ----------
    if EJECUTAR_BALANCE_MATRIX_ZONA:
        for zona in ZONAS:
            zona_suffix = zona.replace(' ', '_').lower()
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
            if punto_enganche is not None:
                punto_enganche >> balance_matrix_task
