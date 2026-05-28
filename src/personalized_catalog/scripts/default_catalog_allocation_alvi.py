# Default
from __future__ import annotations

import io  # noqa: F401
import os
import logging
import argparse
from logging import config

import numpy as np  # noqa: F401

# Pip
import pandas as pd
import pendulum  # noqa: F401

# Own
from google.cloud.bigquery import Client
from sklearn.metrics.pairwise import cosine_distances

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    uploadFrame,
    readBigQuery,
    deleteFromTable,
)


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
    '--store_banner', type=str,
    help='Store banner'
)
parser.add_argument(
    '--top_n', default=10, type=int,
    help='Number of top offers to assign to each customer'
)
parser.add_argument(
    '--sku_per_category', default=1, type=int,
    help='Number of products per category to allocate'
)

# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------

SQL_QUERIES = QueryDict({
    'next_catalog_info':
    """
    SELECT
        DISTINCT nombre_promocion,
        fecha_inicio_de_promocion,
        fecha_fin_de_promocion
    FROM `${gcp_project}.CDA_VISTAS.VW_FACT_WORKFLOW`
    WHERE fecha_inicio_de_promocion > CAST('${execution_date}' AS DATE)
        AND registro_valido = 'X'
        AND descripcion_mecanica='CICLO ALVI'
        AND descripcion_evento_promocional = 'ALVI CICLO'
    ORDER BY fecha_inicio_de_promocion, nombre_promocion
    LIMIT 1;
    """,

    'build_generic_rubro_embedding':
    ('SELECT customer_rubro.rubro,\n'
     + ',\n'.join([f'AVG(dim_{i}) AS dim_{i}' for i in range(100)])
     + '\nFROM `cl-bigdata-analytics-preprod.ML_LAB.W2V_CUSTOMER_EMBEDDINGS` customer_emb'
     + '\nINNER JOIN ( SELECT *'
     + '\nFROM `cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_PROFILE_VF_II`' # noqa: E501
     + '\nWHERE MONTH_ID = ${monthid}) customer_rubro'
     + '\nON customer_emb.customer_key = customer_rubro.customer_id'
     + '\nGROUP BY rubro'
    ),

    'product_embedding_matrix':
    # Gets the embedding representation of the products inside the catalog
    # and their category
    """
    SELECT sku_pool.categoria, sku_pool.ean, sku_emb.*
    FROM (
        SELECT DISTINCT
            workflow.material,
            workflow.ean,
            workflow.categoria
        FROM `${gcp_project}.CDA_VISTAS.VW_FACT_WORKFLOW` workflow

        INNER JOIN (
            SELECT DISTINCT ean from `${gcp_project}.ECOMMERCE.DIM_VTEX_PRODUCT_IMAGE_URLS`
        ) image_urls
        ON CAST(workflow.ean AS BIGINT) = image_urls.ean

        INNER JOIN (
            SELECT material
            FROM `${gcp_project}.PRECIO_PROMOCIONES.BALANCE_MATRIX`
            WHERE (segmento_bm = 'Low-Lower' OR segmento_bm = 'Hi-Lo')
            AND STORE_BANNER = '${store_banner}'
        ) bm
        ON workflow.material = CAST(bm.material AS INT)

        WHERE
            registro_valido = 'X'
            AND descripcion_mecanica='CICLO ALVI'
            AND descripcion_evento_promocional='ALVI CICLO'
            AND fecha_inicio_de_promocion >= CAST('${start_date}' AS DATE)
            AND fecha_fin_de_promocion <= CAST('${end_date}' AS DATE)
            AND nombre_promocion = '${campaign_name}'
    ) sku_pool

    INNER JOIN `${gcp_project}.ML_LAB.W2V_SKU_EMBEDDINGS` sku_emb
    ON sku_pool.material = sku_emb.sku

    WHERE sku_emb.date = CAST('${fecha_sku_emb}' AS DATE)
    AND sku_emb.store_banner = '${store_banner}'
    """,
})

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    gcp_project: str = args['project_id']
    store_banner: str = args['store_banner']
    top_n: int = args['top_n']
    sku_per_category: int = args['sku_per_category']

    upper_store_banner = store_banner.upper()
    lower_store_banner = store_banner.lower()

    logging.info(f'execution_date: {execution_date}')
    logging.info(f'gcp_project: {gcp_project}')
    logging.info(f'store_banner: {store_banner}')
    logging.info(f'top_n: {top_n}')
    logging.info(f'sku_per_category: {sku_per_category}')


    # Set gbq client for all subsequent queries
    gbq_client = Client()

    # Usuario
    usuario = 'default_catalog'

    # ---------------------------------------------------------------------
    # Get catalog campaing name and start/end dates
    # ---------------------------------------------------------------------

    logging.info('Get catalog campaing name and start/end dates')

    campaign_name, start_date, end_date = readBigQuery(SQL_QUERIES['next_catalog_info'].substitute(
        execution_date = execution_date,
        gcp_project = gcp_project
        ),
    user = usuario,
    gbq_client = gbq_client
    ).iloc[0].to_numpy()

    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')

    logging.info(f'Starting allocation of {campaign_name} campaign')
    logging.info(f'start_date = {start_date}')
    logging.info(f'end_date = {end_date}')

    fecha_sku_emb = start_date[:-2] + '01'

    logging.info(f'fecha_sku_emb = {fecha_sku_emb}')

    # ---------------------------------------------------------------------
    # Get the embedded catalog SKUs
    # ---------------------------------------------------------------------
    # Get the embeddings for the SKUs in the catalog

    logging.info('Get the embeddings for the SKUs in the catalog')

    sku_emb = readBigQuery(SQL_QUERIES['product_embedding_matrix'].substitute(
        campaign_name=campaign_name,
        start_date=start_date,
        end_date=end_date,
        fecha_sku_emb = fecha_sku_emb,
        store_banner = store_banner,
        gcp_project = gcp_project
        ),
    user = usuario,
    gbq_client = gbq_client
    ).rename(columns={'sku': 'material', 'categoria': 'category'})

    logging.info(f'#(products in the catalog with embedding): {sku_emb.shape[0]:,}')

    # Construct the sku -> ean relationship table
    sku_ean = sku_emb[['material', 'ean']].copy().drop_duplicates()
    sku_ean['ean'] = sku_ean['ean'].astype('Int64')

    # Drop the ean column from the embeddings table
    sku_emb = sku_emb.drop(columns='ean').drop_duplicates('material')

    # ---------------------------------------------------------------------
    # Get the embedded rubro embeddings
    # ---------------------------------------------------------------------

    logging.info('Get the embedded rubro embeddings')

    rubro_embeddings = readBigQuery(SQL_QUERIES['build_generic_rubro_embedding'].substitute(
        start_date=start_date,
        store_banner = store_banner,
        gcp_project = gcp_project
        ),
    user = usuario,
    gbq_client = gbq_client
    )

    # ---------------------------------------------------------------------
    # Main allocation process
    # ---------------------------------------------------------------------

    logging.info('Main allocation process')

    distances = pd.DataFrame(
        data=cosine_distances(
            rubro_embeddings[[f'dim_{i}' for i in range(100)]],
            sku_emb[[f'dim_{i}' for i in range(100)]]
        ),
        index=rubro_embeddings['rubro'],
        columns=sku_emb['material']
    )

    logging.info(f'OK: Cosine distance matrix. Dimension {distances.shape}')

    # "Flattens" the rectangular matrix into three columns:
    # [ rubro | material | cosine distance ]
    # So now the distances matrix have dim:
    # (n_customer x n_product) x 3
    distances = pd.melt(
        distances.reset_index(),
        id_vars='rubro'
    ).rename(
        columns={'value': 'cosine_distance'}
    )

    logging.info(f'OK: Melt matrix. Dimension {distances.shape}')

    # Remove vectors that are have 90 or more degrees between them
    distances = distances[distances['cosine_distance'] < 1]

    logging.info(f'OK: Filter vectors with +90°. New dim {distances.shape}')

    # Filter by quantity of sku's in every category
    distances = distances.merge(
        sku_emb[['category', 'material']],
        on='material',
        how='inner'
    # Minimizing cosine value...
    ).sort_values(
        'cosine_distance', ascending=True
    # for every customer and category...
    ).groupby(
        ['rubro', 'category'], sort=False,
    # get only the top sku_per_category elements...
    ).head(
        sku_per_category
    # in this columns:
    )[
        ['rubro', 'material', 'cosine_distance']
    ]

    logging.info(f'OK: Filter by max {sku_per_category} products in each category. New dim {distances.shape}')  # noqa: E501

    # Get top n offers per customer minimizing cosine distance
    distances = distances.sort_values(
        'cosine_distance', ascending=True
    ).groupby('rubro').head(top_n)
    logging.info(f'OK: Filter top {top_n} offers. New dim {distances.shape}')

    # Rank best offers (minimizes cosine distance)
    distances['relevance'] = distances.groupby(
        'rubro'
    )['cosine_distance'].rank(
        ascending=True, method='first'
    )
    logging.info(f'OK: Rank top {top_n}')

    # Add campaign name column
    distances['campaign_name'] = campaign_name

    # Change sku to ean
    distances = distances.merge(sku_ean, how='inner', on='material')

    distances = distances[
        ['rubro', 'ean', 'relevance', 'campaign_name']
    # Removes eans with duplicated relevance (can occur when a sku have
    # two eans) in the catalog
    ].drop_duplicates(
        ['rubro', 'relevance'],
        keep='first'
    )

    distances['fecha'] = start_date

    deleteFromTable(
    table_ref = f'{gcp_project}.PERSONALIZED_CATALOG.RUBRO_DEFAULT_CATALOG_{upper_store_banner}',
    where_clause = f"FECHA = '{start_date}'",
    gbq_client = gbq_client,
    )

    uploadFrame(
        distances,
        table_ddl_json_path = os.path.join('gbq_objects',f'default_catalog_allocation_{lower_store_banner}.json'),  # noqa: E501
        project = gcp_project,
        gbq_client = gbq_client,
        if_exists = 'append'
    )

if __name__ == '__main__':
    main()
