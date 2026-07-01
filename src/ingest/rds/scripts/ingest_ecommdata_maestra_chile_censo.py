"""Carga completa de ecommdata.maestra_chile_censo hacia BigQuery."""

# -------------------------------------------------------------------------
# Imports
# -------------------------------------------------------------------------
import os
import logging
import argparse
from logging import config

import pendulum
from google.cloud.bigquery import Client

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.databases.postgresql import readPostgresQuery
from common.gcp_extended.bigquery import uploadFrame
from common.gcp_extended.secretsmanager import getSecret


# -------------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------------
config.dictConfig(LOGGING_CONFIG)

# -------------------------------------------------------------------------
# Parser
# -------------------------------------------------------------------------
parser = argparse.ArgumentParser()

parser.add_argument(
    '--project_id',
    required=True,
    type=str,
    help='GCP Project ID',
)

parser.add_argument(
    '--execution_date',
    required=True,
    type=str,
    help='Execution date YYYY-MM-DD',
)

# -------------------------------------------------------------------------
# SQL
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'extract':
    """
    SELECT DISTINCT
        cut,
        cod_region,
        region,
        cod_provincia,
        provincia,
        comuna,
        area_c,
        cod_localidad,
        localidad,
        id_localidad,
        nombre_localidad_interno
    FROM ecommdata.maestra_chile_censo
    """
})

# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103

    # ---------------------------------------------------------------------
    # Parse arguments
    # ---------------------------------------------------------------------
    args = vars(parser.parse_args())

    execution_date = pendulum.date(
        *list(map(int, args['execution_date'].split('-'))))

    project_id = args['project_id']

    logging.info(f'Execution date: {execution_date}')

    # ---------------------------------------------------------------------
    # BigQuery client
    # ---------------------------------------------------------------------
    gbq_client = Client()

    # ---------------------------------------------------------------------
    # Get data from PostgreSQL
    # ---------------------------------------------------------------------

    logging.info('Leyendo datos desde PostgreSQL...')

    df_a = readPostgresQuery(
        query=SQL_QUERIES['extract'].substitute(),
        credentials_dict=getSecret(
            secret_name='ecommerce_postgres_credentials',  # noqa: S106
            project=project_id,
        ),
    )
    logging.info(f'Registros obtenidos: {len(df_a)}')

    # ---------------------------------------------------------------------
    # Upload to BigQuery
    # ---------------------------------------------------------------------
    logging.info('Uploading data to BigQuery')

    uploadFrame(
        df_a,
        table_ddl_json_path=os.path.join(
            'gbq_objects',
            'maestra_chile_censo.json'
        ),
        project=project_id,
        gbq_client=gbq_client,
        if_exists='replace',
    )

    logging.info('Process completed successfully')

# -------------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------------
if __name__ == '__main__':
    main()
