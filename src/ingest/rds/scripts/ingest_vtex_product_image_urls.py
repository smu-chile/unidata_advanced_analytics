# Default
import os
import logging
import argparse
from io import BytesIO
from logging import config
from concurrent.futures import ThreadPoolExecutor

# pip
import pendulum
from PIL import Image
from tqdm import tqdm
from requests import get
from google.cloud.bigquery import Client

# Own
from common.constants import LOGGING_CONFIG
from common.utils.requests import safeGet
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
parser.add_argument(
    '--max_workers', default=8, type=int,
    help='Number of CPU threads to be used'
)


# -------------------------------------------------------------------------
# SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'get_image_data':
    """
    SELECT
        CAST(regexp_replace(ref_id, '-.*', '', 'gi') AS BIGINT) AS sku,
        CAST(ean_primario AS BIGINT) AS ean,
        nombre_producto AS name,
        'https://unimarc.vteximg.com.br' || imagen AS url,
        LOWER(etiqueta) AS etiqueta,
        orden
    FROM ecommdata.imagenes_sku
    INNER JOIN ecommdata.skus
    USING (ref_id)
    WHERE
        LENGTH(ean_primario) < LENGTH('9223372036854775807')
    GROUP BY 1,2,3,4,5,6
    """,
})


# -------------------------------------------------------------------------
# Functions and classes
# -------------------------------------------------------------------------
def request_function(url, **kwargs):
    """Get image url and verify its contents"""
    # Get url
    requested_url = get(url, **kwargs)  # noqa: S113
    # PIL image verification
    try:
        Image.open(BytesIO(requested_url.content)).verify()
        return requested_url.status_code
    except:  # noqa: E722
        return 410

def personalizedSafeGet(url):
    return safeGet(
        url=url,
        request_function=request_function,
        error_handling='silent'
    )



# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    user = 'ingest-ecommerce'  # noqa: F841
    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    execution_date = pendulum.date(*map(int, args['execution_date'].split('-')))
    max_workers = args['max_workers']
    logging.info(f'execution_date: {execution_date}')
    logging.info(f'max_workers: {max_workers}')

    # Static variables
    gbq_client = Client()

    # Get image data
    logging.info('Sending query...')
    image_urls = readPostgresQuery(
        query=SQL_QUERIES['get_image_data'].substitute(),
        credentials_dict=getSecret(
            secret_name='ecommerce_postgres_credentials',  # noqa: S106
            project=gcp_project_id,
        )
    )
    logging.info(f'#(retrieved VTEX image links): {image_urls.shape[0]:,}')

    # Multithreaded url verification
    logging.info('Starting image verification')
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        image_urls['http_status'] = list(
            tqdm(
                executor.map(personalizedSafeGet, image_urls['url'].to_list()),
                total=len(image_urls),
                desc='Reviewing image urls',
                unit='urls',
            )
        )
    logging.info(f"There are {(image_urls['http_status'] == 200).sum():,} usable images")

    logging.info('Uploading frame to GBQ...')
    uploadFrame(
        image_urls[
            image_urls['http_status'] == 200
        ][
            ['sku', 'ean', 'name', 'url', 'etiqueta', 'orden']
        ],
        table_ddl_json_path=os.path.join('gbq_objects', 'dim_vtex_product_image_urls.json'),
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='replace'
    )
    logging.info('Done!')


if __name__ == '__main__':
    main()
