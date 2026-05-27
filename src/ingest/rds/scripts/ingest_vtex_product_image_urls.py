import os
import logging
import argparse
from logging import config

import pendulum  # noqa: F401
from tqdm import tqdm  # noqa: F401
from google.cloud.bigquery import Client

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.databases.postgresql import readPostgresQuery
from common.gcp_extended.bigquery import uploadFrame
from common.gcp_extended.secretsmanager import getSecret


config.dictConfig(LOGGING_CONFIG)

parser = argparse.ArgumentParser()
parser.add_argument('--project_id', type=str, help='GCP project')
parser.add_argument('--execution_date', type=str, help='DAG date')

SQL_QUERIES = QueryDict({
    'get_image_data': """
    SELECT
        CAST(regexp_replace(ref_id, '-.*', '', 'gi') AS BIGINT) AS sku,
        CAST(ean_primario AS BIGINT) AS ean,
        nombre_producto AS name,
        'https://unimarc.vteximg.com.br' || imagen AS url,
        LOWER(etiqueta) AS etiqueta,
        orden
    FROM ecommdata.imagenes_sku
    INNER JOIN ecommdata.skus USING (ref_id)
    WHERE LENGTH(ean_primario) < LENGTH('9223372036854775807')
    AND 'https://unimarc.vteximg.com.br' || imagen NOT LIKE '%PREPARACION%'
    AND nombre_producto <> 'PRUEBA'
    GROUP BY 1,2,3,4,5,6
    """,
})


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main() -> None:
    args = vars(parser.parse_args())
    gcp_project_id = args['project_id']

    gbq_client = Client()

    logging.info('Extrayendo datos de Postgres...')
    image_urls = readPostgresQuery(
        query=SQL_QUERIES['get_image_data'].substitute(),
        credentials_dict=getSecret(
            secret_name='ecommerce_postgres_credentials',  # noqa: S106
            project=gcp_project_id,
        )
    )

    total_urls = len(image_urls)
    logging.info(f'Total de URLs a procesar: {total_urls:,}')

    logging.info('Subiendo resultados a BigQuery...')
    uploadFrame(
        image_urls,
        table_ddl_json_path=os.path.join('gbq_objects', 'dim_vtex_product_image_urls.json'),
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='replace'
    )
    logging.info('Done!')

if __name__ == '__main__':
    main()
