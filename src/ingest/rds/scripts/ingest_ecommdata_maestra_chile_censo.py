"""Carga completa de ecommdata.maestra_chile_censo hacia BigQuery."""

# -------------------------------------------------------------------------
# Imports
# -------------------------------------------------------------------------
import json
import logging
import argparse
from logging import config
from pathlib import Path

import pandas as pd  # noqa: TC002
import pendulum
from google.cloud import bigquery

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.databases.postgresql import readPostgresQuery
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

    args = vars(parser.parse_args())

    execution_date = pendulum.date(
        *list(map(int, args['execution_date'].split('-')))
    )

    project_id = args['project_id']

    logging.info(f'Execution date: {execution_date}')

    # ---------------------------------------------------------------------
    # Leer JSON
    # ---------------------------------------------------------------------
    json_path = (
        Path(__file__).resolve().parents[1]
        / 'gbq_objects'
        / 'maestra_chile_censo.json'
    )

    with open(json_path, encoding='utf-8') as f:
        metadata = json.load(f)

    dataset = metadata['schema']
    table = metadata['table']

    destination_table = f'{project_id}.{dataset}.{table}'

    # ---------------------------------------------------------------------
    # Leer PostgreSQL
    # ---------------------------------------------------------------------
    logging.info('Leyendo datos desde PostgreSQL...')

    df: pd.DataFrame = readPostgresQuery(
        query=SQL_QUERIES['extract'].substitute(),
        credentials_dict=getSecret(
            secret_name='ecommerce_postgres_credentials',  # noqa: S106
            project=project_id,
        ),
    )

    logging.info(f'Registros obtenidos: {len(df)}')

    # ---------------------------------------------------------------------
    # BigQuery Client
    # ---------------------------------------------------------------------
    client = bigquery.Client(project=project_id)

    # ---------------------------------------------------------------------
    # Construcción del Schema desde JSON
    # ---------------------------------------------------------------------
    schema = []

    for column in metadata['columns']:

        schema.append(  # noqa: PERF401
            bigquery.SchemaField(
                name=column['name'],
                field_type=column['field_type'],
                mode=column['mode'],
            )
        )

    # ---------------------------------------------------------------------
    # Load Job
    # ---------------------------------------------------------------------
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    logging.info(f'Cargando tabla {destination_table}')

    job = client.load_table_from_dataframe(
        dataframe=df,
        destination=destination_table,
        job_config=job_config,
    )

    job.result()

    logging.info('Carga finalizada correctamente.')

    table_info = client.get_table(destination_table)

    logging.info(
    f'Tabla {destination_table} contiene {table_info.num_rows} registros.')


# -------------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------------
if __name__ == '__main__':
    main()
