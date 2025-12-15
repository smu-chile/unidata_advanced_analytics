import os
import logging
import argparse
from logging import config

from google.cloud.bigquery import Client

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.databases.postgresql import readPostgresQuery
from common.gcp_extended.bigquery import uploadFrame
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
    'extract_substitutes':
    """
    SELECT
        fecha_entrega,
        id_orden,
        vtex_ref_id_og,
        vtex_ref_id_sub,
        ean_og,
        ean_sub

    FROM (
        SELECT
            id_orden,
            fecha_entrega
        FROM operaciones_unimarc.cumplimiento_despacho
        WHERE
            fecha_entrega >= '${execution_date}'::date - '1 month'::interval
            AND fecha_entrega < '${execution_date}'
    ) interest_orders

    INNER JOIN (
        SELECT
            id_orden,
            id_producto_substituido AS id,
            ref_id AS vtex_ref_id_og,
            ean AS ean_og,
            descripcion AS descripcion_og
        FROM ecommdata.orden_productos
    ) originals
    USING (id_orden)

    INNER JOIN (
        SELECT
            id,
            ref_id AS vtex_ref_id_sub,
            ean AS ean_sub,
            descripcion AS descripcion_sub
        FROM ecommdata.orden_productos
    ) substitued
    USING (id)
    """,
})



# -------------------------------------------------------------------------
#  Main function
# -------------------------------------------------------------------------
def main() -> None:
    user = 'ingest-rds'  # noqa: F841
    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    gcp_project_id: str = args['project_id']
    logging.info(f'execution_date: {execution_date}')

    # Static variables
    gbq_client = Client()

    # Get data
    logging.info('Sending query...')
    data = readPostgresQuery(
        query=SQL_QUERIES['extract_substitutes'].substitute(
            execution_date=execution_date,
        ),
        credentials_dict=getSecret(
            secret_name='ecommerce_postgres_credentials',  # noqa: S106
            project=gcp_project_id,
        )
    )
    logging.info('Data collected!')

    # Upload data to the table
    logging.info('Uploading frame to GBQ...')
    uploadFrame(
        data,
        table_ddl_json_path=os.path.join('gbq_objects', 'market_product_substitutions.json'),
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='append',
    )
    logging.info('Done!')


if __name__ == '__main__':
    main()
