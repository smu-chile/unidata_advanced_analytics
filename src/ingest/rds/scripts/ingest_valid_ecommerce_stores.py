"""Moves valid ecommerce stores from RDS to GBQ."""
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
    'get_data':
    """
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
})


# -------------------------------------------------------------------------
#  Main function
# -------------------------------------------------------------------------
def main() -> None:
    user = 'ingest-ecommerce'  # noqa: F841
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
        query=SQL_QUERIES['get_data'].substitute(),
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
        table_ddl_json_path=os.path.join('gbq_objects', 'dim_valid_ecommerce_stores.json'),
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='replace'
    )
    logging.info('Done!')


if __name__ == '__main__':
    main()
