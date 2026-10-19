# Default
"""DAG unificado -- regresion, elasticidad, sensibilidad y BM por zona.

Sirve para Unimarc y Alvi en un solo DAG. El banner se elige al
disparar (Trigger DAG w/ config):

    {"execution_date": "2026-07-02", "store_banner": "Alvi"}

Si no se indica "store_banner", corre BANNER_POR_DEFECTO. Se ejecuta UN
banner por corrida: el otro queda en "skipped". Para correr ambos,
disparar dos veces (max_active_runs=1 deja la 2da en cola, asi nunca se
cruzan sobre las mismas tablas de BigQuery).

Cada banner tiene sus zonas (ZONAS_POR_BANNER) y 4 FASES secuenciales:

    FASE 1: Regresion (1 zona tras otra, nunca 2 al mismo tiempo)
        |  (barrera -- TODAS deben terminar)
    FASE 2: Elasticidad (hasta 'concurrency' zonas en simultaneo)
        |  (barrera)
    FASE 3: Sensibilidad (idem)
        |  (barrera)
    FASE 4: Balance Matrix (1 zona tras otra, nunca 2 al mismo tiempo)

Antes de la regresion, el resolvedor de tiendas corre solo para los
banners de RESOLVER_TIENDAS (Unimarc). Alvi usa la tabla
TMP_TIENDAS_ACTIVAS_POR_ZONA ya cargada.

Cada fase tiene su propio interruptor -- si esta apagada, sus tareas
no se crean, y la barrera de la fase activa siguiente se conecta
directo a la ultima fase activa anterior.
"""
import json  # noqa: I001
import platform
import importlib
import itertools
from datetime import timedelta

# Pip
import pendulum
from airflow.models import DAG
from airflow.configuration import conf
from airflow.operators.empty import EmptyOperator
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

PROJECT_NAME = 'pricing_effects'

# Banner que corre si el disparo no trae "store_banner".
BANNER_POR_DEFECTO = 'Unimarc'

# Zonas de cada banner (nombres tal como estan en
# TMP_TIENDAS_ACTIVAS_POR_ZONA).
ZONAS_POR_BANNER = {
    'Unimarc': [
        'Austral',
        'Baja Competencia',
        'Competencia Media',
        'Competencia Regional',
        'Hipercompetitiva',
        'Norte',
        'Premium',
    ],
    'Alvi': [
        'Baja Competencia',
        'Competencia Media',
        'Hipercompetitiva',
        'Retail sin Mayorista',
    ],
}

# Banners cuya tabla de tiendas por zona se arma con
# resolver_tiendas_zona.py (reemplaza solo las filas de Unimarc). Los
# demas usan la tabla ya cargada.
RESOLVER_TIENDAS = ['Unimarc']

# ====================================================================
# INTERRUPTORES -- 4 en total, 1 por fase. Si una fase esta en False,
# sus tareas no se crean, y la barrera se conecta directo entre las
# fases activas vecinas.
# ====================================================================
EJECUTAR_REGRESSION_ZONA = True
EJECUTAR_ELASTICIDAD_ZONA = True
EJECUTAR_SENSIBILIDAD_ZONA = True
EJECUTAR_BALANCE_MATRIX_ZONA = True

SUBIR_A_SHAREPOINT = True

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


def banner_seleccionado(banner, dag_run=None, **_):
    """Indica si el banner es el pedido en la config del disparo."""
    config_disparo = (dag_run.conf or {}) if dag_run else {}
    pedido = str(config_disparo.get('store_banner', BANNER_POR_DEFECTO)).strip()
    validos = {b.lower(): b for b in ZONAS_POR_BANNER}
    if pedido.lower() not in validos:
        msg = f'store_banner desconocido: {pedido!r}. Validos: {sorted(ZONAS_POR_BANNER)}'
        raise ValueError(msg)
    return validos[pedido.lower()] == banner


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

    def crear_tarea(task_id, script, argumentos, recursos=None):
        """Crea una tarea Dataproc con los argumentos comunes."""
        return ExtendedDataprocCreateBatchOperator(
            task_id=task_id,
            python_script_path=f'{PROJECT_NAME}/scripts/{script}',
            dag_env_config=dag_env_config,
            docker_image_name=PROJECT_NAME,
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
                *argumentos,
            ],
            include_paths=['common/', f'{PROJECT_NAME}/gbq_objects/'],
            **(recursos or {}),
        )

    for store_banner, zonas in ZONAS_POR_BANNER.items():
        banner_suffix = store_banner.replace(' ', '_').lower()

        # Compuerta: si el banner no es el pedido, se salta todo lo
        # que cuelga de ella (las 4 fases de este banner).
        compuerta = ShortCircuitOperator(
            task_id=f'banner_{banner_suffix}',
            python_callable=banner_seleccionado,
            op_kwargs={'banner': store_banner},
        )

        # punto_enganche: lista de tareas de la ULTIMA fase activa vista
        # hasta ahora -- la siguiente fase activa se conecta desde aca.
        punto_enganche = [compuerta]

        # ---------- FASE 1: Regresion ----------
        if EJECUTAR_REGRESSION_ZONA:
            regression_tasks = []
            for zona in zonas:
                zona_suffix = zona.replace(' ', '_').lower()
                regression_tasks.append(crear_tarea(
                    f'regression_data_zona_{banner_suffix}_{zona_suffix}',
                    'processed_regression_data_zona.py',
                    ['--store_banner', store_banner,
                     '--use', 'ELASTICITY',
                     '--zona', zona],
                    RECURSOS_EXTRA,
                ))

            # Secuencial estricta -- 1 zona tras otra, nunca 2 al mismo
            # tiempo, sin importar el 'concurrency' del DAG.
            if store_banner in RESOLVER_TIENDAS:
                resolver_task = crear_tarea(
                    f'resolver_tiendas_zona_{banner_suffix}',
                    'resolver_tiendas_zona.py',
                    [],
                )
                punto_enganche >> resolver_task >> regression_tasks[0]
            else:
                punto_enganche >> regression_tasks[0]
            for tarea_anterior, tarea_siguiente in itertools.pairwise(regression_tasks):
                tarea_anterior >> tarea_siguiente

            fin_fase_regresion = EmptyOperator(
                task_id=f'fin_fase_regresion_{banner_suffix}'
            )
            regression_tasks[-1] >> fin_fase_regresion
            punto_enganche = [fin_fase_regresion]

        # ---------- FASE 2: Elasticidad ----------
        if EJECUTAR_ELASTICIDAD_ZONA:
            elasticidad_tasks = []
            for zona in zonas:
                zona_suffix = zona.replace(' ', '_').lower()
                elasticidad_task = crear_tarea(
                    f'elasticidad_zona_{banner_suffix}_{zona_suffix}',
                    'elasticidad_zona.py',
                    ['--store_banner', store_banner, '--zona', zona],
                    RECURSOS_EXTRA,
                )
                punto_enganche >> elasticidad_task
                elasticidad_tasks.append(elasticidad_task)

            fin_fase_elasticidad = EmptyOperator(
                task_id=f'fin_fase_elasticidad_{banner_suffix}'
            )
            elasticidad_tasks >> fin_fase_elasticidad
            punto_enganche = [fin_fase_elasticidad]

        # ---------- FASE 3: Sensibilidad ----------
        if EJECUTAR_SENSIBILIDAD_ZONA:
            sensibilidad_tasks = []
            for zona in zonas:
                zona_suffix = zona.replace(' ', '_').lower()
                sensibilidad_task = crear_tarea(
                    f'product_sensibility_zona_{banner_suffix}_{zona_suffix}',
                    'product_sensibility_zona.py',
                    ['--store_banner', store_banner, '--zona', zona],
                    RECURSOS_EXTRA,
                )
                punto_enganche >> sensibilidad_task
                sensibilidad_tasks.append(sensibilidad_task)

            fin_fase_sensibilidad = EmptyOperator(
                task_id=f'fin_fase_sensibilidad_{banner_suffix}'
            )
            sensibilidad_tasks >> fin_fase_sensibilidad
            punto_enganche = [fin_fase_sensibilidad]

        # ---------- FASE 4: Balance Matrix ----------
        if EJECUTAR_BALANCE_MATRIX_ZONA:
            balance_matrix_tasks = []
            for zona in zonas:
                zona_suffix = zona.replace(' ', '_').lower()
                balance_matrix_tasks.append(crear_tarea(
                    f'balance_matrix_zona_{banner_suffix}_{zona_suffix}',
                    'balance_matrix_zona.py',
                    ['--store_banner', store_banner,
                     '--zona', zona,
                     '--subir_a_sharepoint', str(SUBIR_A_SHAREPOINT)],
                    RECURSOS_EXTRA,
                ))

            punto_enganche >> balance_matrix_tasks[0]
            for tarea_anterior, tarea_siguiente in itertools.pairwise(balance_matrix_tasks):
                tarea_anterior >> tarea_siguiente
