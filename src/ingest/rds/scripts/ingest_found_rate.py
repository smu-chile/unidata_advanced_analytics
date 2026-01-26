import os
import logging
import argparse
from logging import config

import pendulum
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
    help='GCP project in which the script will be executed'
)
parser.add_argument(
    '--execution_date', type=str,
    help='DAG execution date'
)


# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'extract_data':
    """
    SELECT
        fecha_facturacion,
        ref_id,
        id_tienda AS store_id,
        CASE
            WHEN ref_id = '000000000000651953-UN' THEN '7807975004117'
            ELSE ean_primario
        END AS ean,
        unidades_completadas AS ordenes_completadas,
        unidades_solicitadas AS ordenes_solicitadas,
        found_rate
    FROM (
        SELECT
            fecha_facturacion,
            id_tienda,
            ref_id,

            SUM(CASE
                WHEN estado_foundrate = 3 THEN 1
                ELSE 0
            END) AS unidades_completadas,

            SUM(CASE
                WHEN producto_substituto = false THEN 1
                ELSE 0
            END) AS unidades_solicitadas,

            SUM(CASE
                WHEN estado_foundrate = 3 THEN 1
                ELSE 0
            END)::numeric * 1.0 / SUM(CASE
                WHEN producto_substituto = false THEN 1
                ELSE 0
            END)::numeric AS found_rate

        FROM operaciones_unimarc.found_rate_productos
        WHERE fecha_facturacion >= '${execution_date}'::timestamp - '1 month'::interval
        GROUP BY 1,2,3
    ) found_rate_productos
    LEFT JOIN ecommdata.skus USING(ref_id)
    WHERE
        unidades_solicitadas > 0
        AND length(ean_primario) < 19
    """,
})



# -------------------------------------------------------------------------
#  Main function
# -------------------------------------------------------------------------
def main() -> None:
    user = 'ingest-rds'  # noqa: F841
    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: pendulum.Date = pendulum.date(
        *list(map(int, args['execution_date'].split('-')))
    )
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

    # Deleting past data if exist
    logging.info('Deleting past run data if exists...')
    deleteFromTable(
        table_ref=os.path.join('gbq_objects', 'found_rate_product_store_date.json'),
        project=gcp_project_id,
        where_clause=f'fecha_facturacion >= "{execution_date.add(months=-1).isoformat()}"',
        gbq_client=gbq_client,
        if_not_exists='ignore'
    )


    # Upload data to the table
    logging.info('Uploading frame to GBQ...')
    uploadFrame(
        data,
        table_ddl_json_path=os.path.join('gbq_objects', 'found_rate_product_store_date.json'),
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='append',
    )
    logging.info('Done!')


if __name__ == '__main__':
    main()
