"""Load orden_comuna_localidad from PostgreSQL to BigQuery."""

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
        id_localidad,
        comuna_de_la_orden,
        localidad
    FROM ecommdata.orden_comuna_localidad
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
        *list(map(int, args['execution_date'].split('-'))))

    gcp_project_id: str = args['project_id']

    logging.info(f'Execution date: {execution_date}')

    # ---------------------------------------------------------------------
    # BigQuery client
    # ---------------------------------------------------------------------
    gbq_client = Client()

    # ---------------------------------------------------------------------
    # Get data from PostgreSQL
    # ---------------------------------------------------------------------
    logging.info(
        f'Extracting from PostgreSQL: ecommdata.orden_comuna_localidad')  # noqa: F541

    data = readPostgresQuery(
        query=SQL_QUERIES['extract_data'].substitute(),
        credentials_dict=getSecret(
            secret_name='ecommerce_postgres_credentials',  # noqa: S106
            project=gcp_project_id,
        )
    )

    logging.info(
    f'Data collected: {data.shape} - Filas: {data.shape[0]}, Columnas: {data.shape[1]}')

    # Verificar que hay datos
    if data.empty:
        logging.warning('⚠️ No se encontraron datos en la tabla origen')
    else:
        logging.info(f'Primeras 5 filas:\n{data.head()}')

    # ---------------------------------------------------------------------
    # Upload to BigQuery
    # ---------------------------------------------------------------------
    logging.info('Uploading data to BigQuery with REPLACE strategy')

    # Verificar que el archivo JSON existe
    json_path = os.path.join('gbq_objects', 'orden_comuna_localidad.json')

    uploadFrame(
        data,
        table_ddl_json_path=json_path,
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='replace',
    )

    logging.info('✅ Process completed successfully')

    # Mostrar resumen final
    print('\n' + '='*60)  # noqa: T201
    print('📊 CARGA COMPLETADA EXITOSAMENTE')  # noqa: T201
    print('='*60)  # noqa: T201
    print(f'📁 Tabla origen (PostgresSQL): ecommdata.orden_comuna_localidad')  # noqa: F541, T201
    print(f'📁 Tabla destino (BQ): ECOMMERCE.ORDEN_COMUNA_LOCALIDAD')  # noqa: F541, T201
    print(f'📈 Registros cargados: {data.shape[0]:,}')  # noqa: T201
    print(f'📊 Columnas: {data.shape[1]}')  # noqa: T201
    print('='*60 + '\n')  # noqa: T201

# -------------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------------
if __name__ == '__main__':
    main()
