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
    SELECT DISTINCT
        CAST(SUBSTR(id_membresia, 1, 10) AS DATE) AS transaction_date,
        CAST(rut AS TEXT) AS rut,
        estado,
        CAST(fecha_inicio AS DATE) AS fecha_inicio,
        CAST(fecha_fin AS DATE) AS fecha_fin,
        tipo_membresia,
        motivo_inactivacion,
        CAST(fecha_inactivacion AS DATE) AS fecha_inactivacion,
        motivo_cancelacion
    FROM power_bi.membresias_por_user_profile_id
    WHERE SUBSTR(id_membresia, 1, 10)::date >= '${execution_date}'::date - '7 month'::interval
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
    )
    logging.info('Data collected!')

    # Deleting past data if exist
    logging.info('Deleting past run data if exists...')
    deleteFromTable(
        table_ref=os.path.join('gbq_objects', 'market_diamond_memberships.json'),
        project=gcp_project_id,
        where_clause=f'transaction_date >= "{execution_date.add(months=-7).isoformat()}"',
        gbq_client=gbq_client,
        if_not_exists='ignore'
    )


    # Upload data to the table
    logging.info('Uploading frame to GBQ...')
    uploadFrame(
        data,
        table_ddl_json_path=os.path.join('gbq_objects', 'market_diamond_memberships.json'),
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='append',
    )
    logging.info('Done!')


if __name__ == '__main__':
    main()
