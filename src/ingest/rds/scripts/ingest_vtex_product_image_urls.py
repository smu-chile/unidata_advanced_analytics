import os
import asyncio
import logging
import argparse
from io import BytesIO
from logging import config

import aiohttp
import pendulum  # noqa: F401
from PIL import Image
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
parser.add_argument('--max_workers', default=100, type=int, help='Concurrent requests')

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
    GROUP BY 1,2,3,4,5,6
    """,
})

# -------------------------------------------------------------------------
# Lógica de Verificación de Integridad
# -------------------------------------------------------------------------

async def verify_image_integrity(session, url, semaphore):
    """Descarga y verifica que el archivo sea una imagen válida"""
    async with semaphore:
        try:
            # Usamos GET porque para verify()
            # necesitamos el contenido binario
            async with session.get(url, timeout=15) as response:
                if response.status == 200:
                    content = await response.read()
                    # PIL abre la imagen en memoria y la verifica
                    with Image.open(BytesIO(content)) as img:
                        img.verify()
                    return 200
                return response.status
        except Exception:  # noqa: BLE001
            # 415: Unsupported Media Type o 410: Gone (corrupta/error)
            return 415

async def process_batch(urls, max_concurrent):
    """Procesa un subconjunto de URLs de forma asíncrona"""
    semaphore = asyncio.Semaphore(max_concurrent)
    connector = aiohttp.TCPConnector(limit_per_host=20)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [verify_image_integrity(session, url, semaphore) for url in urls]
        # Usamos gather para mantener el orden exacto de la lista original
        return await asyncio.gather(*tasks)

# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main() -> None:
    args = vars(parser.parse_args())
    gcp_project_id = args['project_id']
    max_workers = args['max_workers']

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

    # Procesamiento por Batches
    all_statuses = []
    batch_size = 5000

    for i in range(0, total_urls, batch_size):
        batch_df = image_urls.iloc[i : i + batch_size]
        batch_list = batch_df['url'].to_list()

        logging.info(f'Procesando batch {i//batch_size + 1} (URLs {i} a {i+len(batch_list)})')

        # Ejecutar el batch asíncronamente
        batch_results = asyncio.run(process_batch(batch_list, max_workers))
        all_statuses.extend(batch_results)

        # Pequeña pausa opcional para dejar respirar
        # al worker de Composer/Red
        # asyncio.run(asyncio.sleep(1))  # noqa: ERA001

    image_urls['http_status'] = all_statuses

    # Filtrado final: solo las que pasaron la prueba de PIL (200)
    df_final = image_urls[image_urls['http_status'] == 200][
        ['sku', 'ean', 'name', 'url', 'etiqueta', 'orden']
    ]

    logging.info(f'Imágenes válidas encontradas: {len(df_final):,}')

    logging.info('Subiendo resultados a BigQuery...')
    uploadFrame(
        df_final,
        table_ddl_json_path=os.path.join('gbq_objects', 'dim_vtex_product_image_urls.json'),
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='replace'
    )
    logging.info('Done!')

if __name__ == '__main__':
    main()
