"""Contains the script that loads the found rate table from ecommerce."""
import os
import logging
import argparse
from logging import config

import pendulum
from google.cloud import bigquery  # noqa: F401
from google.cloud.bigquery import Client

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.databases.postgresql import readPostgresQuery
from common.gcp_extended.bigquery import uploadFrame, deleteFromTable
from common.gcp_extended.secretsmanager import getSecret


# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
# Logging config
config.dictConfig(LOGGING_CONFIG)
# Parser config
parser = argparse.ArgumentParser()
parser.add_argument(
    '--project_id', type=str,
    help='GCP project in which the script will be executed')
parser.add_argument(
    '--execution_date', type=str,
    help='DAG execution date')

# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
'extract_data':
"""
SELECT
    fecha_facturacion,
    ref_id,
    orden,
    id_tienda AS store_id,
    CASE
        WHEN ref_id = '000000000000651953-UN' THEN '7807975004117'
        ELSE ean_primario
    END AS ean,
    unidades_completadas AS ordenes_completadas,
    unidades_solicitadas AS ordenes_solicitadas
FROM (
    SELECT
        fecha_facturacion,
        id_tienda,
        ref_id,
        orden,
        SUM(CASE WHEN estado_foundrate = 3 THEN 1
        ELSE 0 END) AS unidades_completadas,
        SUM(CASE WHEN producto_substituto = false THEN 1
        ELSE 0 END) AS unidades_solicitadas
    FROM operaciones_unimarc.found_rate_productos
    WHERE fecha_facturacion
        >= '${execution_date}'::timestamp - '1 month'::interval
    GROUP BY 1,2,3, 4) found_rate_productos
LEFT JOIN ecommdata.skus USING(ref_id)
WHERE unidades_solicitadas > 0 AND length(ean_primario) < 19
""",
})

# -------------------------------------------------------------------------
#  Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    user = 'ingest-rds'  # noqa: F841
    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: pendulum.Date = pendulum.date(
        *list(map(int, args['execution_date'].split('-'))))
    gcp_project_id: str = args['project_id']
    logging.info(f'execution_date: {execution_date}')

    # Static variables
    gbq_client = Client()

    # Get data
    logging.info('Sending query...')
    data = readPostgresQuery(
        query=SQL_QUERIES['extract_data'].substitute(
            execution_date=execution_date.isoformat(),
        ),
        credentials_dict=getSecret(
            secret_name='ecommerce_postgres_credentials',  # noqa: S106
            project=gcp_project_id,
        )
    ).astype({
        'ean': 'Int64',
    })
    logging.info('Data collected!')

    if data.isna().sum().sum():
        err_msg = 'Missing values!'
        raise Exception(err_msg)

    # ---------------------------------------------------------------------
    # Load TMP table
    # ---------------------------------------------------------------------
    logging.info('Cargando tabla TMP...')

    deleteFromTable(
        table_ref=os.path.join(
            'gbq_objects',
            'found_rate_tmp.json'
        ),
        project=gcp_project_id,
        where_clause='1=1',
        gbq_client=gbq_client,
        if_not_exists='ignore'
    )

    uploadFrame(
        data,
        table_ddl_json_path=os.path.join(
            'gbq_objects',
            'found_rate_tmp.json'
        ),
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='append',
    )

    logging.info('Tabla TMP cargada correctamente.')


    # ---------------------------------------------------------------------
    # Delete target data
    # ---------------------------------------------------------------------
    logging.info('Eliminando datos de tabla final...')

    deleteFromTable(
        table_ref=os.path.join(
            'gbq_objects',
            'found_rate_new.json'
        ),
        project=gcp_project_id,
        where_clause=(
            f'fecha_facturacion >= '
            f'"{execution_date.add(months=-1).isoformat()}"'
        ),
        gbq_client=gbq_client,
        if_not_exists='ignore'
    )

    logging.info('Datos anteriores eliminados.')


    # ---------------------------------------------------------------------
    # Insert enriched data
    # ---------------------------------------------------------------------
    logging.info('Agrega datos de columna STORE_BANNER...')

    sql = f"""
    INSERT INTO
    `{gcp_project_id}.ECOMMERCE.FOUND_RATE`
    (
        fecha_facturacion,
        store_banner,
        store_id,
        orden,
        ref_id,
        ean,
        ordenes_completadas,
        ordenes_solicitadas
    )
    SELECT
        A.fecha_facturacion,
        B.store_banner,
        A.store_id,
        A.orden,
        A.ref_id,
        A.ean,
        A.ordenes_completadas,
        A.ordenes_solicitadas
    FROM
    `{gcp_project_id}.TMP.FOUND_RATE_TMP` A
    LEFT JOIN
    `{gcp_project_id}.CDA_VISTAS.VW_DIM_STORE` B
        ON SAFE_CAST(A.store_id AS INT64) = SAFE_CAST(B.store_id AS INT64)
    """  # noqa: S608

    job = gbq_client.query(sql)
    job.result()

    logging.info('Tabla final (FOUND_RATE) cargada correctamente.')


if __name__ == '__main__':
    main()
