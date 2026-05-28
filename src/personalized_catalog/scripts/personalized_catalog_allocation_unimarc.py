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
    setTableExpiration,
    createTableAsSelect,
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
    '--month_interval', default=12, type=int,
    help='Number of months of past transactions from the execution date to view'
)
parser.add_argument(
    '--sku_per_category', default=1, type=int,
    help='Number of products per category to allocate'
)
parser.add_argument(
    '--batch_size', type=int,
    help='Batch size for the allocation execution'
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
    WHERE
        fecha_inicio_de_promocion > CAST('${execution_date}' AS DATE)
        AND registro_valido = 'X'
        AND descripcion_mecanica='CATALOGO UNIMARC'
        AND descripcion_evento_promocional = 'UNI CATALOGO'
        --AND nombre_promocion LIKE 'PRECIO OFERTA%'
    ORDER BY fecha_inicio_de_promocion, nombre_promocion
    LIMIT 1
    """,

    'product_embedding_matrix':
    """
    SELECT
        sku_pool.categoria,
        sku_pool.ean,
        sku_emb.*
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
            AND descripcion_mecanica='CATALOGO UNIMARC'
            AND fecha_inicio_de_promocion >= CAST('${start_date}' AS DATE)
            AND fecha_fin_de_promocion <= CAST('${end_date}' AS DATE)
            AND nombre_promocion = '${campaign_name}'
    ) sku_pool

    INNER JOIN `${gcp_project}.ML_LAB.W2V_SKU_EMBEDDINGS` sku_emb
    ON sku_pool.material = sku_emb.sku

    WHERE
        sku_emb.date = CAST('${fecha_sku_emb}' AS DATE)
        AND sku_emb.store_banner = '${store_banner}'
    """,

    'customer_embedding_matrix':
    """
    SELECT *
    FROM `${gcp_project}.ML_LAB.W2V_CUSTOMER_EMBEDDINGS` customer_emb

    INNER JOIN `${gcp_project}.SEMANTIC_BASKET_SEGMENTATION.SEMANTIC_CUSTOMER_TOPIC` customer_topic
    USING (customer_key)

    WHERE
        customer_emb.date = DATE_TRUNC(DATE '${start_date}', MONTH)
        AND customer_topic.fecha_carga = DATE_TRUNC(DATE '${start_date}', MONTH)
        AND customer_emb.store_banner = '${store_banner}'
    """, # noqa: E501

    'contactable_customer_pool':
    """
    SELECT DISTINCT customer_pool.customer_key
    FROM (
        SELECT DISTINCT CUSTOMER_KEY
        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_BASKET` A

        INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE` D
        ON A.STORE_ID = D.STORE_ID

        WHERE
            TRANSACTION_DATE >= DATE_SUB(DATE '${start_date}', INTERVAL ${month_interval} MONTH)
            AND TRANSACTION_DATE <= DATE '${end_date}'
            AND FNC_DOC_TP_DSC IN ('F','B','NC','FX','NE','BX','BE','FE')
            AND ITM_TXN_FCN_TP_DSC = 'V'
            AND MARKET_BASKET_KEY NOT IN (
                SELECT MARKET_BASKET_KEY
                FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
                WHERE CANAL_VENTA IN ('PEDIDOS YA','UBER EATS','RAPPI','RAPPI TURBO')
            )
            AND D.STORE_BANNER in ('${store_banner}')
            AND CUSTOMER_KEY <> MD5('CST^CL^-1')
    ) customer_pool

    INNER JOIN `cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_TYC` TYC
    ON
        customer_pool.CUSTOMER_KEY = TYC.CUSTOMER_KEY
        and TYC.TYC = 1

    INNER JOIN `cl-cda-unidata-prod.DS_UNIDATA_CRM.CONTACTABILITY_USR` USR
    ON
        customer_pool.CUSTOMER_KEY = USR.CUSTOMER_ID
        AND USR.ORGANIZATION_ID = upper('${store_banner}')
        AND USR.EMAIL IS NOT NULL

    INNER JOIN `cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CUSTOMER_BASE_WCA_FINAL` UNS
    ON
        customer_pool.CUSTOMER_KEY = UNS.CUSTOMER_ID
        AND UNS.UNSUBSCRIBE_EMAIL_UNIMARC IS NULL

    LEFT JOIN `cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_OUTLIER_MART` OLR
    ON
        customer_pool.CUSTOMER_KEY = OLR.CUSTOMER_KEY
        AND OLR.ORG_IP_ID = '${organization_id}'

    WHERE OLR.CUSTOMER_KEY IS NULL
    """, # noqa: E501

    'last_n_month_transactions':
    """
    WITH last_n_month_transactions AS (
        SELECT DISTINCT
            A.customer_key,
            CAST(A.sku_product AS BIGINT) AS material
        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM` A

        INNER JOIN `${gcp_project}.CDA_VISTAS.VW_FACT_WORKFLOW` B
        ON CAST(A.sku_product AS BIGINT) = B.material

        WHERE
            TRANSACTION_DATE > DATE_SUB(DATE '${start_date}', INTERVAL ${month_interval} MONTH)
            AND TRANSACTION_DATE <= DATE '${end_date}'
            AND SKU_PRODUCT <> 'None'
            AND TRANSACTION_TYPE IN ('NE','FX','BX','B ','BE','FE','F ','NC','TN','TF')
            AND ITM_TXN_FCN_TP_DSC = 'V'
            AND MARKET_BASKET_KEY NOT IN (
                SELECT MARKET_BASKET_KEY
                FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
                WHERE CANAL_VENTA IN ('PEDIDOS YA','UBER EATS','RAPPI','RAPPI TURBO')
            )
            AND registro_valido = 'X'
            AND descripcion_mecanica='CATALOGO UNIMARC'
            AND nombre_promocion = '${campaign_name}'
    )

    SELECT
        A.customer_key,
        A.material,
        B.customer_key_index
    FROM last_n_month_transactions A
    INNER JOIN (
        SELECT
            customer_key,
            DENSE_RANK() OVER (ORDER BY customer_key) AS customer_key_index
        FROM (
            SELECT DISTINCT customer_key
            FROM last_n_month_transactions
        )
    ) B
    ON A.customer_key = B.customer_key
    """, # noqa: E501

    'n_clients_with_transactions':
    # Get the total number of clients with transactions to allow printing
    # of the total batch count
    """
    SELECT MAX(customer_key_index) AS max_customers
    FROM `${gcp_project}.TMP.TMP_LAST_N_MONTH_TRANSACTIONS_${upper_store_banner}`
    """,

    'query_transactions':
    # Queries a batch of customer transactions associated to batch_size
    # customer ids
    """
    SELECT customer_key, material
    FROM `${gcp_project}.TMP.TMP_LAST_N_MONTH_TRANSACTIONS_${upper_store_banner}`
    WHERE
        customer_key_index >= ${start_idx}
        AND customer_key_index < ${end_idx}
    """,

    'topic_catalog_alloc':
    """
    SELECT *
    FROM `${gcp_project}.PERSONALIZED_CATALOG.TOPIC_DEFAULT_CATALOG_${upper_store_banner}`
    WHERE FECHA = '${start_date}'
    """,  # noqa: E501

})

# -------------------------------------------------------------------------
# Functions and Classes
# -------------------------------------------------------------------------

def filterSKUperCategory(ordered_ranked_alloc: pd.DataFrame,
                         sku_ean_category: pd.DataFrame,
                         ranking_col_name: str,
                         sku_per_category: int) -> pd.DataFrame:

    # Define if the products are identified by ean or material
    if (('ean' in ordered_ranked_alloc.columns)
        and ('ean' in sku_ean_category.columns)):
        prod_id_name = 'ean'
    elif (('material' in ordered_ranked_alloc.columns)
          and ('material' in sku_ean_category.columns)):
        prod_id_name = 'material'
    else:
        err_msg = ('Neither material or ean simultaneously in both DataFrames '
                   'passed.')
        raise KeyError(err_msg)

    # Get other col names in the original df
    other_cols = ordered_ranked_alloc.columns.drop(
        ['customer_key', ranking_col_name, prod_id_name]
    )

    # Filter by quantity of sku's in every category
    return ordered_ranked_alloc.merge(
        sku_ean_category[['category', prod_id_name]],
        on=prod_id_name,
        how='inner'
    # Minimizing ranking column...
    ).sort_values(
        ranking_col_name, ascending=True
    # for every customer and category...
    ).groupby(
        ['customer_key', 'category'], sort=False,
    # get only the top sku_per_category elements...
    ).head(
        sku_per_category
    # and return this columns:
    )[
        ['customer_key', prod_id_name, ranking_col_name, *other_cols]
    ]

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
    month_interval: int = args['month_interval']
    sku_per_category: int = args['sku_per_category']
    batch_size: int = args['batch_size']

    upper_store_banner = store_banner.upper()
    lower_store_banner = store_banner.lower()

    if store_banner == 'Unimarc':
        organization_id = '01'

    logging.info(f'gcp_project: {gcp_project}')
    logging.info(f'execution_date: {execution_date}')
    logging.info(f'store_banner: {store_banner}')
    logging.info(f'organization_id: {organization_id}')
    logging.info(f'top_n: {top_n}')
    logging.info(f'month_interval: {month_interval}')
    logging.info(f'sku_per_category: {sku_per_category}')
    logging.info(f'batch_size: {batch_size}')


    # Set gbq client for all subsequent queries
    gbq_client = Client()

    # Usuario
    usuario = 'personalized_catalog'

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
    # Get the embedded customer pool
    # ---------------------------------------------------------------------
    # Get customer pool

    logging.info('Get the embedded customer pool')
    logging.info('Get customer pool')

    customer_pool = readBigQuery(SQL_QUERIES['contactable_customer_pool'].substitute(
        start_date=start_date,
        end_date=end_date,
        month_interval=month_interval,
        store_banner = store_banner,
        organization_id = organization_id,
        gcp_project = gcp_project
        ),
    user = usuario,
    gbq_client = gbq_client
    )

    logging.info(f'#(filtered last {month_interval} months customers): {customer_pool.shape[0]:,}')

    # Get customer embeddings
    logging.info('Get customer embeddings')

    customer_emb = readBigQuery(SQL_QUERIES['customer_embedding_matrix'].substitute(
        start_date=start_date,
        store_banner = store_banner,
        gcp_project = gcp_project
        ),
    user = usuario,
    gbq_client = gbq_client
    )

    customer_emb.columns = customer_emb.columns.str.lower()

    # Construct embedded customer pool
    logging.info('Construct embedded customer pool')

    customer_pool_emb = customer_emb.merge(
        customer_pool,
        on='customer_key',
        how='inner'
    )

    del customer_emb
    del customer_pool
    logging.info(f'#(filtered last {month_interval} months customers with embedding): {customer_pool_emb.shape[0]:,}')  # noqa: E501

    # ---------------------------------------------------------------------
    # Get the default recommendations per topic
    # ---------------------------------------------------------------------

    logging.info('Get the default recommendations per topic')

    topic_default_alloc = readBigQuery(SQL_QUERIES['topic_catalog_alloc'].substitute(
        start_date = start_date,
        upper_store_banner = upper_store_banner,
        gcp_project = gcp_project
        ),
    user = usuario,
    gbq_client = gbq_client
    )

    topic_default_alloc.columns = topic_default_alloc.columns.str.lower()


    logging.info('Rebuilding tmp_last_year_transactions table...')

    createTableAsSelect(
        query=SQL_QUERIES['last_n_month_transactions'].substitute(
            month_interval=month_interval,
            campaign_name=campaign_name,
            start_date=start_date,
            end_date=end_date,
            gcp_project = gcp_project
        ),
        table_ref=f'{gcp_project}.TMP.TMP_LAST_N_MONTH_TRANSACTIONS_{upper_store_banner}',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )

    now = pendulum.now()
    expiration = now.add(minutes=1440)

    setTableExpiration(
        table_ref = f'{gcp_project}.TMP.TMP_LAST_N_MONTH_TRANSACTIONS_{upper_store_banner}',
        expiration = expiration,
        gbq_client= gbq_client
    )

    # ---------------------------------------------------------------------
    # Main allocation process
    # ---------------------------------------------------------------------
    # Get the max number of clients with transactions

    logging.info('Main allocation process')
    logging.info('Get the max number of clients with transactions')

    max_n_customers = readBigQuery(SQL_QUERIES['n_clients_with_transactions'].substitute(
        upper_store_banner = upper_store_banner,
        gcp_project = gcp_project
        ),
    user = usuario,
    gbq_client = gbq_client
    )['max_customers'].iloc[0]

    # Total number of batches
    total_batches = int(np.ceil(max_n_customers / batch_size))
    logging.info(f'total_batches: {total_batches}')

    deleteFromTable(
        table_ref=f'{gcp_project}.PERSONALIZED_CATALOG.PERSONALIZED_CATALOG_{upper_store_banner}',
        where_clause=f"FECHA = '{start_date}'",
        gbq_client=gbq_client,
    )

    for n_batch in range(total_batches):
        logging.info('--------------------------------------------------------')
        logging.info(f'Batch {n_batch+1} of {total_batches}')
        logging.info(f'Indexes: [{n_batch*batch_size}, {(n_batch + 1)*batch_size}[')
        logging.info('--------------------------------------------------------')

        # Get all the transactions for the first batch_size clients
        transactions = readBigQuery(SQL_QUERIES['query_transactions'].substitute(
            upper_store_banner = upper_store_banner,
            start_idx=n_batch*batch_size,
            end_idx=(n_batch + 1)*batch_size,
            gcp_project = gcp_project
            ),
        user = usuario,
        gbq_client = gbq_client
        )
        logging.info(f'OK: Retrieved transactions. Dimension: {transactions.shape}')

        # Get the customer embeddings for this batch
        customer_pool_emb_batch = customer_pool_emb.merge(
            pd.DataFrame(
                transactions['customer_key'].unique(), columns=['customer_key']
            ),
            on='customer_key',
            how='inner'
        )
        logging.info(f'OK: Appended customer embeddings, New dim {customer_pool_emb_batch.shape}')

        # Calculate the cosine distance between all the catalog offers
        # vectors and the customer vectors
        distances = pd.DataFrame(
            data=cosine_distances(
                customer_pool_emb_batch[[f'dim_{i}' for i in range(100)]],
                sku_emb[[f'dim_{i}' for i in range(100)]]
            ),
            index=customer_pool_emb_batch['customer_key'],
            columns=sku_emb['material']
        )
        logging.info(f'OK: Cosine distance matrix. Dimension {distances.shape}')

        # "Flattens" the rectangular matrix into three columns:
        # customer_id | material | cosine distance
        # So now the df_cosines_dist matrix have dim:
        # (n_customer x n_product) x 3
        distances = pd.melt(
            distances.reset_index(),
            id_vars='customer_key'
        ).rename(
            columns={'value': 'cosine_distance'}
        )
        logging.info(f'OK: Melt matrix. Dimension {distances.shape}')

        # Remove vectors that are have 90 or more degrees between them
        distances = distances[distances['cosine_distance'] < 1]
        logging.info(f'OK: Filter vectors with +90°. New dim {distances.shape}')

        # Filter by products that the customer buyed in the last n_months
        distances = distances.merge(
            transactions,
            how='inner',
            on=['customer_key', 'material']
        )
        logging.info(f'OK: Filter by buyed products. New dim {distances.shape}')

        # Filter sku per category
        distances = filterSKUperCategory(
            ordered_ranked_alloc=distances,
            sku_ean_category=sku_emb[['category', 'material']],
            ranking_col_name='cosine_distance',
            sku_per_category=sku_per_category
        )
        logging.info(f'OK: Filter by max {sku_per_category} products in each category. New dim {distances.shape}')  # noqa: E501

        # Get top n offers per customer minimizing cosine distance
        distances = distances.sort_values(
            'cosine_distance', ascending=True
        ).groupby('customer_key').head(top_n)
        logging.info(f'OK: Filter top {top_n} offers. New dim {distances.shape}')

        # Rank best offers (minimizes cosine distance)
        distances['relevance'] = distances.groupby(
            'customer_key'
        )['cosine_distance'].rank(
            ascending=True, method='first'
        )
        logging.info(f'OK: Rank top {top_n}')

        # Add campaign name column
        distances['campaign_name'] = campaign_name

        # Change sku to ean
        distances = distances.merge(sku_ean, how='inner', on='material')

        # Get only the interest columns
        distances = distances[
            ['customer_key', 'ean', 'relevance', 'campaign_name']
        # Removes eans with duplicated relevance (can occur when a sku have
        # two eans) in the catalog
        ].drop_duplicates(
            ['customer_key', 'relevance'],
            keep='first'
        )

        # -----------------------------------------------------------------
        # Fill with default offers taken from customer topics
        # -----------------------------------------------------------------
        logging.info('Filling up missing offers...')
        # Join every customer in the batch with the default offers using
        # their semantic topic
        default_offers: pd.DataFrame = topic_default_alloc.merge(
            customer_pool_emb_batch[['customer_key', 'topic']].drop_duplicates(),
            on='topic',
            how='inner'
        )[
        # Get only the relevant columns
            ['customer_key', 'ean', 'relevance', 'campaign_name']
        # Add a column with the count of personalizedf offers by every
        # customer
        ].merge(
            distances.groupby('customer_key')['relevance'].max().to_frame().rename(
                columns={'relevance':'n_missing'}
            ),
            on='customer_key',
            how='outer'
        )

        # Change the relevance of the default offers so now starts after
        # the personalized ones (e.g. if personalized offers of client A
        # are 5 and top_n is 10, deafult offers start on 6)
        default_offers['relevance'] = (default_offers['relevance']
                                        + default_offers['n_missing'].fillna(0))

        # Join personalized and default offers
        distances = pd.concat([
            distances,
            default_offers[['customer_key', 'ean', 'relevance', 'campaign_name']]
        # Sort by customer_id and relevance to mantain personalized offers
        # on the top
        ]).sort_values(
            ['customer_key', 'relevance'],
            ascending=True
        # Remove possible duplicated eans when one of them is part of the
        # default and personalized offers for a client
        ).drop_duplicates(
            ['customer_key', 'ean'], keep='first'
        )

        # Filter by sku per category
        distances = filterSKUperCategory(
            ordered_ranked_alloc=distances,
            sku_ean_category=sku_ean.merge(
                sku_emb[['category', 'material']],
                on='material',
                how='inner'
            )[['ean', 'category']],
            ranking_col_name='relevance',
            sku_per_category=sku_per_category
        )

        # Create the new raking bc some rows can be droped in the previous
        # step
        distances['final_rank'] = distances.groupby(
            'customer_key'
        )['relevance'].rank(ascending=True)

        # Build final df
        distances = distances[
        # Remove relevance column as its no longer used
            ['customer_key', 'ean', 'final_rank', 'campaign_name']
        # Get only top_n offers
        ].sort_values(
            by=['customer_key', 'final_rank'],
            ascending=True
        ).groupby('customer_key').head(top_n)

        distances['fecha'] = start_date

        uploadFrame(
            distances,
            table_ddl_json_path=os.path.join('gbq_objects',f'personalized_catalog_allocation_{lower_store_banner}.json'),
            project = gcp_project,
            gbq_client = gbq_client,
            if_exists = 'append'
        )

if __name__ == '__main__':
    main()
