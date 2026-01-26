"""Declares DAG that moves various complete tables from RDS to GBQ."""
# Default
import json
import platform
import importlib
from datetime import timedelta

# pip
import pendulum
from airflow.models import DAG
from airflow.configuration import conf
from airflow.models.baseoperator import chain


if platform.system() == 'Windows':
    from common.operators.dataproc_create_batch import (
        ExtendedDataprocCreateBatchOperator,
    )
elif platform.system() == 'Linux':
    ExtendedDataprocCreateBatchOperator = (importlib.import_module(
        'BRANCH_PLACEHOLDER.'
        'smu-chile.unidata_advanced_analytics.'
        'src.common.operators.dataproc_create_batch'
    )).ExtendedDataprocCreateBatchOperator
else:
    err_msg = 'Only Linux and Windows are supported.'
    raise NotImplementedError(err_msg)

# Globals
with open(
    f'{conf.get("core", "dags_folder")}/'
    'BRANCH_PLACEHOLDER/'
    'smu-chile/unidata_advanced_analytics/src/common/constants/dag_env_config.json'
) as f:
    dag_env_config = json.load(f)['BRANCH_PLACEHOLDER']

PROJECT_NAME = 'ingest'
SUBPROJECT_NAME = 'rds'
dag_args = {
    'dag_id': 'ingest_full_tables_ecommerce',
    'schedule_interval': '0 9 * * SUN',
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 3,
    'tags': [PROJECT_NAME, SUBPROJECT_NAME, 'ecastrot'],
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['ecastrot@unidata.cl'],
        'start_date': pendulum.datetime(
            2025, 6, 25,
            tz=pendulum.timezone('America/Santiago')
        ),
        'depends_on_past': False,
        'catchup': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
    }
}

with DAG(**dag_args) as dag:
    QUERIES = {
        'dim_valid_ecommerce_stores': """
        (
            SELECT id, nombre_tienda_janis
            FROM ecommdata.tiendas
            WHERE status = 1
        ) UNION ALL (
            SELECT id, nombre_tienda_janis
            FROM ecommdata_alvi.tiendas
            WHERE
                id LIKE '3%%'
                AND status = 1
        )
        """,

        'dim_ok_to_shop': (
        """
        SELECT
            CAST(product_ean AS BIGINT) AS ean,
            brand_name AS brand,
            description,
            flavor,
            CAST(CAST(REPLACE(drained_size_value, ',', '.') AS FLOAT) AS INT) AS drained_size_value,
            CAST(CAST(REPLACE(num_portions, ',', '.') AS FLOAT) AS INT) AS num_portions,
            basic_unit,
        """  # noqa: E501

        + ''.join([
            f"\tCAST(REPLACE({col + suffix}, ',', '.') AS FLOAT) AS {col + suffix},\n"
            if suffix == '_value'
            else f'\t{col + suffix},\n'
            for col in [
                'size', 'portion', 'energy', 'protein', 'fat_total', 'fat_sat',
                'fat_mono', 'fat_poli', 'fat_trans', 'fat_cholesterol', 'carb',
                'sugars', 'fiber', 'sodium'
            ]
            for suffix in [
                '_value', '_unit'
            ]
        ])

        + ''.join([
            f'\tCAST({col} AS INTEGER) AS {col},\n' for col in [
                'minsal_cl_high_sugar', 'minsal_cl_high_saturated_fat', 'minsal_cl_high_sodium',
                'minsal_cl_high_calories', 'aplv_suitable', 'gluten_free', 'lactose_free',
                'kosher', 'vegan', 'vegetarian', 'diabetes_suitable', 'soy_free',
                'egg_free', 'fish_free', 'seafood_free', 'peanut_free', 'nuts_free',
                'walnuts_free', 'sulphite_free', 'wheat_free',
            ]
        ])

        + '\talcohol_by_volume,\n'
        + '\talcohol_proof\n'
        + 'FROM catalogo.ok_to_shop_v2'
        ),

        'plan_venta_unimarc': """
        SELECT
            fecha AS date,
            id_tienda AS store_id,
            plan_venta,
            plan_ticket_promedio,
            plan_ordenes,
            forecast_venta,
            forecast,
            forecast_operacional,
            forecast_operacional_creado,
            sesiones_esperadas
        FROM forecast_and_planning.plan_venta_unimarc
        """,

        'plan_venta_alvi': """
        SELECT
            fecha AS date,
            id_tienda AS store_id,
            plan_venta,
            plan_ticket_promedio,
            plan_ordenes,
            forecast_venta
        FROM forecast_and_planning.plan_venta_alvi
        """,

        'plan_venta_membresia': """
        SELECT
            fecha AS date,
            id_tienda AS store_id,
            tipo_membresia,
            estado_membresia,
            tipo_facturacion_membresia,
            membresias,
            ingresos_item_membresia,
            facturacion_membresia,
            ordenes_membresia
        FROM forecast_and_planning.plan_venta_membresia_unimarc
        """,

        'plan_venta_last_millers': """
        SELECT
            fecha AS date,
            nombre_last_miller,
            plan_venta,
            plan_ticket_promedio,
            plan_ordenes,
            forecast_venta
        FROM forecast_and_planning.plan_venta_lastmillers
        """,
    }
    EXECUTION_DATE = "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}"  # noqa: E501

    ingest_data = [
        ExtendedDataprocCreateBatchOperator(
            task_id = f'ingest_{query_name}',
            python_script_path=(
                f'{PROJECT_NAME}/'
                f'{SUBPROJECT_NAME}/'
                'scripts/'
                'ingest_full_table_generic.py'
            ),
            dag_env_config=dag_env_config,
            docker_image_name=f'{PROJECT_NAME}-{SUBPROJECT_NAME}',
            pyspark_batch_args=[
                '--project_id', dag_env_config['project_id'],
                '--execution_date', EXECUTION_DATE,
                '--query', QUERIES[query_name],
                '--table_ddl_filename', query_name
            ],
            include_paths=[
                'common/',
                f'{PROJECT_NAME}/{SUBPROJECT_NAME}/gbq_objects/'
            ],
        )

        for query_name in QUERIES
    ]


chain(ingest_data)
