"""Contains script that loads the catalog inweb table from ecommerce."""
import os
import logging
import argparse
from logging import config

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
        fecha,
        ref_id,
        (CASE
            WHEN ref_id = '000000000000651953-UN' THEN '7807975004117'
            ELSE ean_primario
        END)::FLOAT::BIGINT AS ean,
        store_id,
        store_banner,
        inweb

    FROM (
        SELECT
            fecha_hora::date fecha,
            ref_id,
            id_tienda AS store_id,
            'Unimarc' AS store_banner,
            AVG(disponible_web::INT) AS inweb
        FROM ecommdata.publicacion_catalogo
        WHERE
            fecha_hora::date = '${execution_date}'::date
        GROUP BY 1, 2, 3
        UNION ALL
        SELECT
            fecha_hora::date fecha,
            ref_id,
            id_tienda AS store_id,
            'Alvi' AS store_banner,
            AVG(disponible_web::INT) AS inweb
        FROM ecommdata_alvi.publicacion_catalogo
        WHERE fecha_hora::date = '${execution_date}'::date
        GROUP BY 1, 2, 3
    ) inweb_product

    LEFT JOIN ecommdata.skus USING(ref_id)
    WHERE length(ean_primario) < 19
    """,
})



# -------------------------------------------------------------------------
#  Main function
# -------------------------------------------------------------------------
def main() -> None:
    """Load catalog inweb table from ecommerce to BigQuery.

    Extracts data from PostgreSQL ecommerce database, validates for missing
    values, deletes any existing data for the execution date, and uploads
    the new data to BigQuery.
    """
    user = 'ingest-rds'  # noqa: F841
    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str =  args['execution_date']
    gcp_project_id: str = args['project_id']
    logging.info(f'execution_date: {execution_date}')

    # Static variables
    gbq_client = Client()

    # Get data
    logging.info('Sending query...')
    data = readPostgresQuery(
        query=SQL_QUERIES['extract_data'].substitute(
            execution_date=execution_date,
        ),
        credentials_dict=getSecret(
            secret_name='ecommerce_postgres_credentials',  # noqa: S106
            project=gcp_project_id,
        )
    )
    logging.info('Data collected!')

    if data.isna().sum().sum():
        err_msg = 'Missing values!'
        raise Exception(err_msg)

    # Deleting past data if exist
    logging.info('Deleting past run data if exists...')
    deleteFromTable(
        table_ref=os.path.join('gbq_objects', 'catalog_inweb.json'),
        project=gcp_project_id,
        where_clause=f'fecha = "{execution_date}"',
        gbq_client=gbq_client,
        if_not_exists='ignore'
    )


    # Upload data to the table
    logging.info('Uploading frame to GBQ...')
    uploadFrame(
        data,
        table_ddl_json_path=os.path.join('gbq_objects', 'catalog_inweb.json'),
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='append',
    )
    logging.info('Done!')


if __name__ == '__main__':
    main()
