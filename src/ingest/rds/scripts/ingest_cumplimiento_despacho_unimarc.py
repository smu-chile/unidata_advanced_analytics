"""Load cumplimiento despacho unimarc from PostgreSQL to BigQuery."""

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
# Logging config
# -------------------------------------------------------------------------
config.dictConfig(LOGGING_CONFIG)

# -------------------------------------------------------------------------
# Parser config
# -------------------------------------------------------------------------
parser = argparse.ArgumentParser()

parser.add_argument(
    '--project_id',
    type=str,
    required=True,
    help='GCP project id'
)

parser.add_argument(
    '--execution_date',
    type=str,
    required=True,
    help='Execution date YYYY-MM-DD'
)


# -------------------------------------------------------------------------
# SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'extract_data':
    """
    SELECT
        id_orden,
        fecha_facturacion,
        tipo_despacho,
        id_tienda,
        glosa_tienda,
        id_transportadora,
        nombre_transportadora,
        fecha_despacho,
        hora_despacho,
        fecha_entrega,
        hora_entrega,
        inicio_ventana,
        termino_ventana,
        comuna,
        cumplimiento_ondate,
        cumplimiento_ontime
    FROM operaciones_unimarc.cumplimiento_despacho
    """
})


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main() -> None:
    """Main process."""  # noqa: D401
    # ---------------------------------------------------------------------
    # Parse arguments
    # ---------------------------------------------------------------------
    args = vars(parser.parse_args())

    execution_date: pendulum.Date = pendulum.date(
        *list(map(int, args['execution_date'].split('-')))
    )

    gcp_project_id: str = args['project_id']

    logging.info(
        f'Execution date: {execution_date}'
    )

    # ---------------------------------------------------------------------
    # BigQuery client
    # ---------------------------------------------------------------------
    gbq_client = Client()

    # ---------------------------------------------------------------------
    # Get data from PostgreSQL
    # ---------------------------------------------------------------------
    logging.info(
        'Extracting data from PostgreSQL'
    )

    data = readPostgresQuery(
        query=SQL_QUERIES['extract_data'],
        credentials_dict=getSecret(
            secret_name='ecommerce_postgres_credentials',  # noqa: S106
            project=gcp_project_id,
        )
    )

    logging.info(
        f'Data collected: {data.shape}'
    )

    # ---------------------------------------------------------------------
    # Upload to BigQuery
    # ---------------------------------------------------------------------
    logging.info(
        'Uploading data to BigQuery'
    )

    uploadFrame(
        data,
        table_ddl_json_path=os.path.join(
            'gbq_objects',
            'cumplimiento_despacho_unimarc.json'
        ),
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='replace',
    )

    logging.info(
        'Process completed successfully'
    )


# -------------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------------
if __name__ == '__main__':
    main()
