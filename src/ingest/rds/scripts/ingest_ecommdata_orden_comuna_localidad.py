"""Contar registros de tabla orden_comuna_localidad."""

# -------------------------------------------------------------------------
# Imports
# -------------------------------------------------------------------------
import logging
import argparse
from logging import config

import pendulum
from google.cloud.bigquery import Client  # noqa: F401

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.databases.postgresql import readPostgresQuery
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
    'count_records':
    """
    SELECT COUNT(*) as total_registros
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
    logging.info(f'Iniciando')  # noqa: F541

    args = vars(parser.parse_args())

    execution_date: pendulum.Date = pendulum.date(
        *list(map(int, args['execution_date'].split('-')))
    )

    gcp_project_id: str = args['project_id']

    logging.info(f'Execution date: {execution_date}')

    # ---------------------------------------------------------------------
    # Get count from PostgreSQL
    # ---------------------------------------------------------------------
    logging.info('Contando registros en ecommdata.orden_comuna_localidad')

    data = readPostgresQuery(
        query=SQL_QUERIES['count_records'].substitute(),
        credentials_dict=getSecret(
            secret_name='ecommerce_postgres_credentials',  # noqa: S106
            project=gcp_project_id,
        )
    )

    total_records = data['total_registros'].iloc[0] if not data.empty else 0

    # Resultado en consola
    print('\n' + '='*50)  # noqa: T201
    print(f'📊 CONTEO DE REGISTROS')  # noqa: F541, T201
    print('='*50)  # noqa: T201
    print(f'📁 Tabla: ecommdata.orden_comuna_localidad')  # noqa: F541, T201
    print(f'📈 Total: {total_records:,} registros')  # noqa: T201
    print('='*50 + '\n')  # noqa: T201

    logging.info(f'Proceso completado. Total: {total_records} registros')


# -------------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------------
if __name__ == '__main__':
    main()
