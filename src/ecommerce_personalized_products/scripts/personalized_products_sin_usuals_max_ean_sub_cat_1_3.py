"""Flujo Personalized Prodcuts."""
# Default
from __future__ import annotations

import gc
import io  # noqa: F401
import os
import logging
import argparse
from logging import config

import numpy as np

# Pip
import pandas as pd
import pendulum

# Own
from google.cloud.bigquery import Client
from sklearn.metrics.pairwise import cosine_distances  # noqa: F401

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    uploadFrame,
    readBigQuery,
    deleteFromTable,
    setTableExpiration,
    createTableAsSelect,
    createTableFromJSON,
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
    '--top_n', default=35, type=int,
    help='Number of top offers to assign to each customer'
)
parser.add_argument(
    '--month_interval', default=6, type=int,
    help='Number of months of past transactions from the execution date to view'
)
parser.add_argument(
    '--ean_per_subcategory', default=1, type=int,
    help='Number of products per category to allocate'
)
parser.add_argument(
    '--batch_size', default = 50000, type=int,
    help='Batch size for the allocation execution'
)

# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------

SQL_QUERIES = QueryDict({
    'customer_embeddings':
    """
    SELECT
        customer_emb.*,
        customer_index.customer_key_index
    FROM (
        SELECT *
        FROM`${gcp_project}.ML_LAB.W2V_CUSTOMER_EMBEDDINGS`
        WHERE date = '${fecha_emb}'
        AND store_banner = '${store_banner}'
        AND customer_key IN (
            FROM_BASE64('aSg9tFRjxdgT12sSbnG7LA=='),
            FROM_BASE64('eYPo7nMndFtbrPoMzXExdA=='),
            FROM_BASE64('DQaqL1qALLNUlXl0HICjDQ'),
            FROM_BASE64('emQwxVeJ34weHj3P+omSFg=='),
            FROM_BASE64('eRzgTneP0tGKsnMZ9U2xQQ=='),
            FROM_BASE64('iSFrmN+kHVbHRQqBaaRdQQ=='),
            FROM_BASE64('P5uD9Q6vg6/Z9pAuRas65g=='),
            FROM_BASE64('eomAD4slCiWWkUDz/zwwLQ=='),
            FROM_BASE64('5lvjTzd2D7/4Hzx9OOb3fg=='),
            FROM_BASE64('QA+qP4odVW3QOOCyXfArgQ=='),
            FROM_BASE64('G3eG4XSovD/2EsL5eAP2wQ=='),
            FROM_BASE64('zSQdiN4QvKm39gwe5zqxUA=='),
            FROM_BASE64('UA+lolcjjhcXXdbjN9aB8w=='),
            FROM_BASE64('Srko2maBU6R9BrmQ8jHw5A=='),
            FROM_BASE64('7K3bbBkZLsufdmbDUngOSQ==')
        )
    ) customer_emb

    INNER JOIN (
        SELECT customer_key
        FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_APP_USERS`
        WHERE UNIMARC_LOGGED = 1
    ) customer_app
    USING (customer_key)

    INNER JOIN (
        SELECT DISTINCT customer_key, customer_key_index
        FROM `${gcp_project}.TMP.TMP_LAST_N_MONTH_TRANSACTIONS_PERSONALIZED_PRODUCTS_${upper_store_banner}`
    ) customer_index
    USING(customer_key)
    """, #noqa: E501

    'sku_embeddings':
    """
        SELECT *
        FROM `${gcp_project}.ML_LAB.W2V_SKU_EMBEDDINGS`
        WHERE date = '${fecha_emb}'
        AND store_banner = '${store_banner}'
    """,

    'productos_promocion':
    """
    SELECT
        t.NOMBRE_PROMOCION,
        t.DESCRIPCION_EVENTO_PROMOCIONAL,
        t.FECHA_INICIO_DE_PROMOCION,
        t.FECHA_FIN_DE_PROMOCION,
        t.MATERIAL,
        t.EAN,
        t.DESC_MATERIAL,
        dim_prod.GRUPO_DSC,
        dim_prod.CAT_DSC,
        dim_prod.BRAND_DESC
    FROM (
        SELECT
            NOMBRE_PROMOCION,
            DESCRIPCION_EVENTO_PROMOCIONAL,
            MATERIAL,
            EAN,
            DESC_MATERIAL,
            FECHA_INICIO_DE_PROMOCION,
            FECHA_FIN_DE_PROMOCION,
            ROW_NUMBER() OVER (PARTITION BY EAN ORDER BY FECHA_INICIO_DE_PROMOCION ASC) AS RW
    FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_WORKFLOW`
    WHERE
        organizacion_ventas = '1000'
        AND canal_distribucion IN ('10','70')
        AND registro_valido = 'X'
        AND FECHA_INICIO_DE_PROMOCION <= '${fecha_ini_prom2}'
        AND FECHA_FIN_DE_PROMOCION >= '${fecha_ini_prom3}'
        AND EXTRACT(YEAR FROM FECHA_FIN_DE_PROMOCION) < 2100
        AND REGEXP_CONTAINS(
            UPPER(DESCRIPCION_EVENTO_PROMOCIONAL),
            r'CATALOGO|CICLO|VENTA ESPECIAL|APOTEOSICO|OFERTA QUINCENAL|MASIVAS'
        )
        AND NOMBRE_PROMOCION NOT LIKE 'LUNES%'
        AND NOMBRE_PROMOCION NOT LIKE 'MARTES%'
        AND NOMBRE_PROMOCION NOT LIKE 'MIERCOLES%'
        AND NOMBRE_PROMOCION NOT LIKE 'JUEVES%'
        AND NOMBRE_PROMOCION NOT LIKE 'VIERNES%'
        AND NOMBRE_PROMOCION NOT LIKE 'SAB%'
        AND NOMBRE_PROMOCION NOT LIKE '%UNIPAY%'
        AND NOMBRE_PROMOCION NOT LIKE '%CUMPLEANOS%'
        AND NOMBRE_PROMOCION NOT LIKE '%LOCALES%'
        AND NOMBRE_PROMOCION NOT LIKE '%REGION%'
        AND NOMBRE_PROMOCION NOT LIKE '%CAFE%'
        AND NOMBRE_PROMOCION NOT LIKE '%NON FOOD%'
    ) t

    INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT` AS dim_prod
    ON t.EAN = dim_prod.EAN

    WHERE t.RW = 1
    """,

    'last_n_month_transactions':
    """
    WITH last_n_month_transactions AS (
        SELECT
            A.customer_key,
            A.transaction_date,
            A.ean,
            CAST(A.sku_product AS BIGINT) AS material,
            C.grupo_dsc,
            C.cat_dsc,
            D.ean as ean_usuals
        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM` A

        INNER JOIN (
            SELECT customer_key
            FROM`${gcp_project}.ML_LAB.W2V_CUSTOMER_EMBEDDINGS`
            WHERE
                date = '${fecha_emb}'
                AND store_banner = '${store_banner}'
        ) B
        ON A.customer_key = B.customer_key

        INNER JOIN (
            SELECT DISTINCT
                CAST(ean AS INT) AS ean,
                grupo_dsc,
                cat_dsc
            FROM `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT`
        ) C
        ON CAST(A.ean AS INT) = C.ean

        LEFT JOIN (
            SELECT customer_key,ean
            FROM `${gcp_project}.ECOMMERCE.MY_USUALS`
            WHERE date = '${fecha_my_usuals}'
            AND store_banner = 1
        ) D
        ON A.customer_key = D.customer_key
        AND CAST(A.ean AS INT) = D.ean

        WHERE
            TRANSACTION_DATE >= DATE_TRUNC(DATE_SUB(DATE '${execution_date}', INTERVAL ${month_interval} MONTH), MONTH)
            AND TRANSACTION_DATE < DATE '${execution_date}'
            AND SKU_PRODUCT <> 'None'
            AND TRANSACTION_TYPE IN ('NE','FX','BX','B ','BE','FE','F ','NC','TN','TF')
            AND ITM_TXN_FCN_TP_DSC = 'V'
            AND VALUE > 0
            AND MARKET_BASKET_KEY NOT IN (
                SELECT MARKET_BASKET_KEY
                FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
                WHERE CANAL_VENTA IN ('PEDIDOS YA','UBER EATS','RAPPI','RAPPI TURBO')
            )
            AND A.customer_key IN (
                FROM_BASE64('aSg9tFRjxdgT12sSbnG7LA=='),
                FROM_BASE64('eYPo7nMndFtbrPoMzXExdA=='),
                FROM_BASE64('DQaqL1qALLNUlXl0HICjDQ'),
                FROM_BASE64('emQwxVeJ34weHj3P+omSFg=='),
                FROM_BASE64('eRzgTneP0tGKsnMZ9U2xQQ=='),
                FROM_BASE64('iSFrmN+kHVbHRQqBaaRdQQ=='),
                FROM_BASE64('P5uD9Q6vg6/Z9pAuRas65g=='),
                FROM_BASE64('eomAD4slCiWWkUDz/zwwLQ=='),
                FROM_BASE64('5lvjTzd2D7/4Hzx9OOb3fg=='),
                FROM_BASE64('QA+qP4odVW3QOOCyXfArgQ=='),
                FROM_BASE64('G3eG4XSovD/2EsL5eAP2wQ=='),
                FROM_BASE64('zSQdiN4QvKm39gwe5zqxUA=='),
                FROM_BASE64('UA+lolcjjhcXXdbjN9aB8w=='),
                FROM_BASE64('Srko2maBU6R9BrmQ8jHw5A=='),
                FROM_BASE64('7K3bbBkZLsufdmbDUngOSQ==')
            )
    )

    SELECT
        A.customer_key,
        A.transaction_date,
        A.ean,
        A.material,
        A.grupo_dsc,
        A.cat_dsc,
        B.customer_key_index,
        CASE
            WHEN A.ean_usuals IS NOT NULL THEN 'SI' ELSE 'NO'
        END AS my_usuals
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
    """,  # noqa: E501

    'get_batch':
    """
    SELECT
        customer_key,
        transaction_date,
        ean,
        material,
        grupo_dsc,
        cat_dsc
    FROM `${gcp_project}.TMP.TMP_LAST_N_MONTH_TRANSACTIONS_PERSONALIZED_PRODUCTS_${upper_store_banner}`
    WHERE
        customer_key_index >= ${start_idx}
        AND customer_key_index < ${end_idx}
        AND my_usuals = 'NO'
    """,  # noqa: E501

    'max_customer_index':
    """
    SELECT MAX(customer_key_index) AS max_customers
    FROM `${gcp_project}.TMP.TMP_LAST_N_MONTH_TRANSACTIONS_PERSONALIZED_PRODUCTS_${upper_store_banner}`
    """,  # noqa: E501

    'transactions_ean':
    """
    SELECT
        customer_key,
        ean,
        DENSE_RANK() OVER(PARTITION BY customer_key ORDER BY COUNT(*) DESC) AS relevance_ean,
        count(distinct transaction_date) AS n_compras_ean
    FROM `${gcp_project}.TMP.TMP_LAST_N_MONTH_TRANSACTIONS_PERSONALIZED_PRODUCTS_${upper_store_banner}`
    WHERE
        customer_key_index >= ${start_idx}
        AND customer_key_index < ${end_idx}
        AND my_usuals = 'NO'
    GROUP BY customer_key, ean
    """,  # noqa: E501

    'transactions_grupo':
    """
    SELECT
        customer_key,
        grupo_dsc,
        DENSE_RANK() OVER(PARTITION BY customer_key ORDER BY COUNT(*) DESC) AS relevance_grupo,
        count(*) AS n_compras_grupo
    FROM `${gcp_project}.TMP.TMP_LAST_N_MONTH_TRANSACTIONS_PERSONALIZED_PRODUCTS_${upper_store_banner}`
    WHERE
        customer_key_index >= ${start_idx}
        AND customer_key_index < ${end_idx}
        AND my_usuals = 'NO'
    GROUP BY customer_key, grupo_dsc
    """,  # noqa: E501

    'distancia_coseno':
    """
    WITH DISTANCIAS AS (
    SELECT
        c.customer_key,
        c.customer_key_index,
        s.material,
        ML.DISTANCE(c.embedding, s.embedding, 'COSINE') AS cosine_distance
    FROM (
        SELECT
            customer_key,
            customer_key_index,
            [${dims_columns}] AS embedding
        FROM `${gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_CUSTOMER_EMBEDDINGS`
        WHERE
            customer_key_index >= ${start_idx}
            AND customer_key_index < ${end_idx}
    ) c
    CROSS JOIN (
        SELECT
            material,
            [${dims_columns}] AS embedding
        FROM `${gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_SKU_EMBEDDINGS`
    ) s
    WHERE ML.DISTANCE(c.embedding, s.embedding, 'COSINE') < 1
    ),

    DISTANCIAS_2 AS (
        SELECT
            d.*,
            p.EAN AS ean,
            p.GRUPO_DSC AS grupo_dsc,
            p.CAT_DSC AS cat_dsc
        FROM DISTANCIAS d

        INNER JOIN `${gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_PROD_PROMOCION` p
        ON d.MATERIAL = p.MATERIAL

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY d.customer_key, d.material
            ORDER BY d.customer_key
        ) = 1
    ),

    DISTANCES_3 AS (
        SELECT A.*,B.ean as ean_usuals
        FROM DISTANCIAS_2 A

        LEFT JOIN (
            SELECT customer_key,ean
            FROM `${gcp_project}.ECOMMERCE.MY_USUALS`
            WHERE date = '${fecha_my_usuals}'
            AND store_banner = 1
        ) B
        ON A.customer_key = B.customer_key
        AND CAST(A.ean AS INT) = B.ean
    )

    SELECT *
    FROM DISTANCES_3
    WHERE ean_usuals IS NULL
    """,

    'prod_promocion':
    """
    SELECT *
    FROM `${gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_PROD_PROMOCION`
    """,

    'distances_ean':
    """
    SELECT
        distance.*,
        ean.n_compras_ean,
        ean.relevance_ean
    FROM `${gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_DISTANCE_COSINE` distance

    INNER JOIN `${gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_TRANSACTIONS_EAN` ean
    ON distance.customer_key = ean.customer_key
    AND distance.ean = ean.ean
    """,

    'distances_grupo':
    """
    SELECT
        distance.*,
        grupo.n_compras_grupo,
        grupo.relevance_grupo
    FROM `${gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_DISTANCE_COSINE` distance

    INNER JOIN `${gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_TRANSACTIONS_GRUPO` grupo
    ON distance.customer_key = grupo.customer_key
    AND distance.grupo_dsc = grupo.grupo_dsc
    """,

    'distances_fill':
    """
    WITH CUSTOMER_FILL_COUNTS AS (
        SELECT
            customer_key,
            cat_dsc,
            COUNT(DISTINCT ean) AS ean_count_cat
        FROM `${gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_CUSTOMER_FILL`
        GROUP BY customer_key, cat_dsc
    ),

    DISTANCES_FILL_PASO_1 AS (
        SELECT d.*, n.prod_fill
        FROM `${gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_DISTANCE_COSINE` d
        INNER JOIN `${gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_NEEDS_FILL` n
        ON d.customer_key = n.customer_key
        WHERE d.ean_usuals IS NULL
    ),

    DISTANCES_FILL_PASO_2 AS (
        SELECT
            d.*,
            LEAST(1, 3 - COALESCE(c_cat.ean_count_cat, 0)) AS ean_disponible_cat
        FROM DISTANCES_FILL_PASO_1 d
        LEFT JOIN `${gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_CUSTOMER_FILL` c_grupo
            ON d.customer_key = c_grupo.customer_key
            AND d.grupo_dsc = c_grupo.grupo_dsc
        LEFT JOIN CUSTOMER_FILL_COUNTS c_cat
            ON d.customer_key = c_cat.customer_key
            AND d.cat_dsc = c_cat.cat_dsc
        WHERE c_grupo.customer_key IS NULL -- No repetir grupo
          AND COALESCE(c_cat.ean_count_cat, 0) < 3
    ),

    DISTANCES_FILL_PASO_3 AS (
        SELECT *
        FROM DISTANCES_FILL_PASO_2
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY customer_key, grupo_dsc
            ORDER BY cosine_distance
        ) = 1
        AND ROW_NUMBER() OVER (
            PARTITION BY customer_key, cat_dsc
            ORDER BY cosine_distance
        ) <= ean_disponible_cat
    )

    SELECT * EXCEPT (prod_fill, ean_disponible_cat), 2 AS origen
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY customer_key
                ORDER BY cosine_distance
            ) AS relevance_fill
        FROM DISTANCES_FILL_PASO_3
    )
    WHERE relevance_fill <= prod_fill
    """,

    'allocation_personalized_products':
    """
    SELECT
        date,
        customer_key,
        hash_string,
        CAST(sku_product AS INT64) AS sku_product,
        ean,
        relevance,
        CASE
            WHEN store_banner = 'Unimarc' THEN 1
            WHEN store_banner = 'Mayorista' THEN 4
            WHEN store_banner = 'Alvi' THEN 5
            WHEN store_banner = 'Super 10' THEN 15
        END AS store_banner,
        CASE
            WHEN unidad_de_medida LIKE '%ST%' THEN LPAD(sku_product, 18, '0') || '-' || 'UN'
            ELSE LPAD(sku_product, 18, '0') || '-' || unidad_de_medida
        END AS vtexrefid

    FROM `${gcp_project}.TMP.BASE_PERSONALIZED_PRODUCTS`

    INNER JOIN  (
        SELECT *
        FROM (
            SELECT
                CAST(ean AS INT64) AS ean,
                sku_product,
                unidad_de_medida,
                ROW_NUMBER() OVER (PARTITION BY ean) AS ean_index
            FROM `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT`
        )
        WHERE ean_index = 1
    )
    USING (ean)

    INNER JOIN (
        SELECT
            customer_key,
            pda_customer_key AS customer_id
        FROM `${gcp_project_cda}.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID`
    )
    USING (customer_key)

    INNER JOIN (
        SELECT
            customer_id,
            hash_string
        FROM `${gcp_project_cda}.DS_PROD_CLIENTES_IC.CL_HASH`
    )
    USING (customer_id)
    """
})

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------

def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project: str = args['project_id']
    store_banner: str = args['store_banner']
    top_n: int = args['top_n']
    month_interval: int = args['month_interval']
    ean_per_subcategory: int = args['ean_per_subcategory']
    batch_size: int = args['batch_size']
    execution_date: pendulum.Date = pendulum.date(
        *list(map(int, args['execution_date'].split('-')))
    )

    gcp_project_cda = 'cl-cda-unidata-prod'

    date_table = execution_date.add(days=1)
    fecha_prom = execution_date.add(days=4)
    fecha_emb = execution_date.replace(day=1)
    fin_mes_n1 = execution_date.add(months=1).end_of('month')

    if execution_date.day_of_week == pendulum.WEDNESDAY:
        fecha_my_usuals = execution_date
    elif execution_date.day_of_week == pendulum.SUNDAY:
        fecha_my_usuals = execution_date.previous(pendulum.THURSDAY).to_date_string()

    upper_store_banner = store_banner.upper()

    dims_columns = [f'dim_{i}' for i in range(100)]
    dims_columns = ', '.join(dims_columns)

    logging.info(f'gcp_project: {gcp_project}')
    logging.info(f'execution_date: {execution_date}')
    logging.info(f'fecha_emb: {fecha_emb}')
    logging.info(f'fin_mes_n1: {fin_mes_n1}')
    logging.info(f'fecha_my_usuals: {fecha_my_usuals}')
    logging.info(f'fecha_prom: {fecha_prom}')
    logging.info(f'store_banner: {store_banner}')
    logging.info(f'top_n: {top_n}')
    logging.info(f'month_interval: {month_interval}')
    logging.info(f'ean_per_subcategory: {ean_per_subcategory}')
    logging.info(f'batch_size: {batch_size}')

    # Set gbq client for all subsequent queries
    gbq_client = Client()

    # Usuario
    usuario = 'personalized_products'

    # Create table using DDL JSON
    logging.info('Creating table schema if needed')
    createTableFromJSON(
        table_ddl_json_path=os.path.join('gbq_objects', 'personalized_products.json'),
        project=gcp_project,
        gbq_client=gbq_client,
        if_exists='ignore',
    )

    logging.info('Creacion tabla transacciones clientes')
    createTableAsSelect(
        query=SQL_QUERIES['last_n_month_transactions'].substitute(
            gcp_project = gcp_project,
            fecha_emb = fecha_emb,
            fecha_my_usuals = fecha_my_usuals,
            execution_date = date_table,
            month_interval = month_interval,
            store_banner = store_banner
        ),
        table_ref=f'{gcp_project}.TMP.TMP_LAST_N_MONTH_TRANSACTIONS_PERSONALIZED_PRODUCTS_{upper_store_banner}',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )

    now = pendulum.now()
    expiration = now.add(minutes=1440)

    setTableExpiration(
        table_ref = f'{gcp_project}.TMP.TMP_LAST_N_MONTH_TRANSACTIONS_PERSONALIZED_PRODUCTS_{upper_store_banner}',  # noqa: E501
        expiration = expiration,
        gbq_client= gbq_client
    )

    logging.info('Creacion tabla customer embeddings')
    createTableAsSelect(
        query=SQL_QUERIES['customer_embeddings'].substitute(
            gcp_project = gcp_project,
            fecha_emb = fecha_emb,
            store_banner = store_banner,
            upper_store_banner = upper_store_banner
        ),
        table_ref=f'{gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_CUSTOMER_EMBEDDINGS',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )

    setTableExpiration(
        table_ref = f'{gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_CUSTOMER_EMBEDDINGS',
        expiration = expiration,
        gbq_client= gbq_client
    )

    logging.info('Creacion tabla sku embeddings')
    sku_emb = readBigQuery(SQL_QUERIES['sku_embeddings'].substitute(
        gcp_project = gcp_project,
        fecha_emb = fecha_emb,
        store_banner = store_banner
        ),
    user = usuario,
    gbq_client = gbq_client
    )
    sku_emb = sku_emb.drop(columns=['date','store_banner'])

    logging.info('Creacion tabla productos en promocion')
    createTableAsSelect(
    query=SQL_QUERIES['productos_promocion'].substitute(
        gcp_project = gcp_project,
        fecha_ini_prom2 = date_table,
        fecha_ini_prom3 = fecha_prom,
        ),
        table_ref=f'{gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_PROD_PROMOCION',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )

    setTableExpiration(
        table_ref = f'{gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_PROD_PROMOCION',
        expiration = expiration,
        gbq_client= gbq_client
    )

    prod_prom = readBigQuery(SQL_QUERIES['prod_promocion'].substitute(
        gcp_project = gcp_project
        ),
    user = usuario,
    gbq_client = gbq_client
    )
    prod_prom.columns = prod_prom.columns.str.lower()

    sku_emb = sku_emb.merge(
        prod_prom[['material']],
        left_on='sku',
        right_on='material',
        how='inner'
    )
    sku_emb = sku_emb.drop_duplicates(subset=['material'])
    sku_emb = sku_emb.drop(columns=['sku'])

    uploadFrame(
        sku_emb,
        table_ddl_json_path=os.path.join('gbq_objects','sku_emb.json'),
        project = gcp_project,
        gbq_client = gbq_client,
        if_exists = 'replace'
    )

    setTableExpiration(
        table_ref = f'{gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_SKU_EMBEDDINGS',
        expiration = expiration,
        gbq_client= gbq_client
    )

    logging.info(f'#skus en promocion con embeddings: {sku_emb.shape[0]:,}')

    del sku_emb
    gc.collect()

    max_n_customers = readBigQuery(SQL_QUERIES['max_customer_index'].substitute(
        upper_store_banner = upper_store_banner,
        gcp_project = gcp_project
        ),
    user = usuario,
    gbq_client = gbq_client
    )['max_customers'].iloc[0]

    total_batches = int(np.ceil(max_n_customers / batch_size))
    logging.info(f'total barches: {total_batches}')


    logging.info('Inicio Proceso Productos Personalizados')

    for n_batch in range(total_batches):
        logging.info('--------------------------------------------------------')
        logging.info(f'Batch {n_batch+1} of {total_batches}')
        logging.info(f'Indexes: [{n_batch*batch_size}, {(n_batch + 1)*batch_size}[')
        logging.info('--------------------------------------------------------')

        createTableAsSelect(
            query=SQL_QUERIES['transactions_ean'].substitute(
                gcp_project = gcp_project,
                upper_store_banner = upper_store_banner,
                start_idx=n_batch*batch_size,
                end_idx=(n_batch + 1)*batch_size,
            ),
            table_ref=f'{gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_TRANSACTIONS_EAN',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            use_legacy_sql=False,
            gbq_client=gbq_client,
        )

        createTableAsSelect(
            query=SQL_QUERIES['transactions_grupo'].substitute(
                gcp_project = gcp_project,
                upper_store_banner = upper_store_banner,
                start_idx=n_batch*batch_size,
                end_idx=(n_batch + 1)*batch_size,
            ),
            table_ref=f'{gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_TRANSACTIONS_GRUPO',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            use_legacy_sql=False,
            gbq_client=gbq_client,
        )

        createTableAsSelect(
            query=SQL_QUERIES['distancia_coseno'].substitute(
                gcp_project = gcp_project,
                dims_columns = dims_columns,
                start_idx=n_batch*batch_size,
                end_idx=(n_batch + 1)*batch_size,
                fecha_my_usuals = fecha_my_usuals
            ),
            table_ref=f'{gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_DISTANCE_COSINE',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            clustering_fields = ['customer_key_index'],
            use_legacy_sql=False,
            gbq_client=gbq_client,
        )

        distances_ean = readBigQuery(SQL_QUERIES['distances_ean'].substitute(
            gcp_project = gcp_project
            ),
        user = usuario,
        gbq_client = gbq_client
        )

        distances_ean = distances_ean.sort_values(
            by=['customer_key','relevance_ean','cosine_distance'],
            ascending=[True, True, True]
        ).reset_index(drop=True)

        distances_ean_final = distances_ean.loc[
            (distances_ean['n_compras_ean'] > 1)
        ].copy()

        distances_ean_final['rank_subcat'] = (
            distances_ean_final
            .groupby(['customer_key', 'grupo_dsc'])
            .cumcount() + 1
        )

        distances_ean_final = distances_ean_final.loc[
            distances_ean_final['rank_subcat'] <= ean_per_subcategory
        ]

        distances_ean_final['rank_cat'] = (
            distances_ean_final
            .groupby(['customer_key', 'cat_dsc'])
            .cumcount() + 1
        )

        distances_ean_final = distances_ean_final.loc[
            distances_ean_final['rank_cat'] <= 3
        ]

        distances_ean_final['rank_total_cliente'] = (
            distances_ean_final
            .groupby('customer_key')
            .cumcount() + 1
        )

        distances_ean_final = distances_ean_final.loc[
            distances_ean_final['rank_total_cliente'] <= top_n
        ]

        distances_ean_final = distances_ean_final.drop(
            columns=['n_compras_ean','rank_subcat','rank_total_cliente']
        ).reset_index(drop=True)

        del distances_ean
        gc.collect()

        distances_grupo = readBigQuery(SQL_QUERIES['distances_grupo'].substitute(
            gcp_project = gcp_project
            ),
        user = usuario,
        gbq_client = gbq_client
        )

        conteo_grupo = distances_ean_final.groupby(['customer_key', 'grupo_dsc'])[
            'ean'
        ].nunique()
        conteo_cat = distances_ean_final.groupby(['customer_key', 'cat_dsc'])[
            'ean'
        ].nunique()

        grupos_saturados = conteo_grupo[conteo_grupo >= ean_per_subcategory]
        disponibilidad_grupo = (ean_per_subcategory - conteo_grupo).clip(lower=0)

        cat_saturadas = conteo_cat[conteo_cat >= 3]
        disponibilidad_cat = (3 - conteo_cat).clip(lower=0)

        idx_grupo = pd.MultiIndex.from_frame(
            distances_grupo[['customer_key', 'grupo_dsc']]
        )
        idx_cat = pd.MultiIndex.from_frame(
            distances_grupo[['customer_key', 'cat_dsc']]
        )

        mask_saturados = ~idx_grupo.isin(grupos_saturados.index) & ~idx_cat.isin(
            cat_saturadas.index
        )
        distances_cat_filtrado = distances_grupo[mask_saturados].copy()


        idx_filas_grupo = pd.MultiIndex.from_frame(
            distances_cat_filtrado[['customer_key', 'grupo_dsc']]
        )
        idx_filas_cat = pd.MultiIndex.from_frame(
            distances_cat_filtrado[['customer_key', 'cat_dsc']]
        )
        disp_g_series = idx_filas_grupo.map(disponibilidad_grupo).fillna(ean_per_subcategory)
        disp_c_series = idx_filas_cat.map(disponibilidad_cat).fillna(3)

        distances_cat_filtrado['ean_disponible'] = (
            np.minimum(disp_g_series.to_numpy(), disp_c_series.to_numpy())
            .astype('int8')
        )

        idx_cat_ean = pd.MultiIndex.from_frame(
            distances_cat_filtrado[['customer_key', 'ean']]
        )
        idx_ean_final = pd.MultiIndex.from_frame(
            distances_ean_final[['customer_key', 'ean']]
        )

        mask_no_asignados = ~idx_cat_ean.isin(idx_ean_final)
        distances_cat_filtrado = distances_cat_filtrado[mask_no_asignados]

        distances_cat_filtrado = distances_cat_filtrado.sort_values(
            by=['customer_key', 'relevance_grupo', 'cosine_distance'],
            ascending=[True, True, True],
        )

        distances_cat_filtrado['rank_grupo'] = distances_cat_filtrado.groupby(
            ['customer_key', 'grupo_dsc']
        ).cumcount()
        distances_cat_filtrado['rank_cat'] = distances_cat_filtrado.groupby(
            ['customer_key', 'cat_dsc']
        ).cumcount()

        distances_cat_filtrado = (
            distances_cat_filtrado[
                (
                    distances_cat_filtrado['rank_grupo']
                    < distances_cat_filtrado['ean_disponible']
                )
                & (distances_cat_filtrado['rank_cat'] < 3)
            ]
            .drop(
                columns=[
                    'n_compras_grupo',
                    'ean_disponible',
                    'rank_grupo',
                    'rank_cat',
                ]
            )
            .reset_index(drop=True)
        )

        del distances_grupo
        gc.collect()

        distances_all = pd.concat(
            [distances_ean_final, distances_cat_filtrado], ignore_index=False
        )

        distances_all['origen'] = np.where(
            distances_all['relevance_ean'].notna(),0,1
        ).astype('int8')

        distances_all = distances_all.sort_values(
            ['customer_key','relevance_grupo','origen'],
            ascending=[True, True, False]
        )

        distances_all['relevance_add'] = distances_all.groupby(
            ['customer_key', 'origen']
        ).cumcount()

        mask_fijos = distances_all['relevance_ean'].notna()
        fijos_count = mask_fijos.groupby(distances_all['customer_key']).transform('sum')
        distances_all['cupos'] = (top_n - fijos_count).clip(lower=0)

        mask_keep = (
            mask_fijos |
            (
                (distances_all['origen'] == 1) &
                (distances_all['relevance_add'] < distances_all['cupos'])
            )
        )

        distances_all_final = distances_all[mask_keep].copy()
        counts = distances_all_final.groupby('customer_key').size().rename('n_ean')
        needs_fill = counts[counts < top_n].reset_index()
        needs_fill['prod_fill'] = top_n - needs_fill['n_ean']

        customer_fill = distances_all_final[
            distances_all_final['customer_key'].isin(needs_fill['customer_key'])
        ]

        customer_fill = customer_fill[[
            'customer_key','material','cosine_distance',
            'ean','grupo_dsc','cat_dsc','origen'
        ]]

        uploadFrame(
            needs_fill,
            table_ddl_json_path=os.path.join('gbq_objects','needs_fill.json'),
            project = gcp_project,
            gbq_client = gbq_client,
            if_exists = 'replace'
        )

        uploadFrame(
            customer_fill,
            table_ddl_json_path=os.path.join('gbq_objects','customer_fill.json'),
            project = gcp_project,
            gbq_client = gbq_client,
            if_exists = 'replace'
        )

        del needs_fill
        del customer_fill
        gc.collect()

        distances_fill = readBigQuery(SQL_QUERIES['distances_fill'].substitute(
            gcp_project = gcp_project
            ),
        user = usuario,
        gbq_client = gbq_client
        )

        distances_all_final = pd.concat(
            [distances_all_final, distances_fill], ignore_index=False
        )

        del distances_fill
        gc.collect()

        distances_all_final = distances_all_final.sort_values(
        [
            'customer_key',
            'origen',
            'relevance_ean',
            'relevance_grupo',
            'relevance_fill'
        ],
        ascending=[True, True, True, True, True]
        ).reset_index(drop=True)

        distances_all_final['relevance'] = (
            distances_all_final
            .groupby('customer_key')
            .cumcount()
            .add(1)
            .astype('int64')
        )

        distances_all_final['store_banner'] = store_banner
        distances_all_final['date'] = date_table

        distances_all_final['ean'] = distances_all_final['ean'].astype('int64')
        distances_all_final['relevance'] = distances_all_final['relevance'].astype('int64')
        distances_all_final['store_banner'] = distances_all_final['store_banner'].astype(str)
        distances_all_final['date'] = pd.to_datetime(distances_all_final['date']).dt.date


        uploadFrame(
            distances_all_final[[
                'date',
                'customer_key',
                'ean',
                'relevance',
                'store_banner'
                ]],
            table_ddl_json_path=os.path.join('gbq_objects','base_personalized_products.json'),
            project = gcp_project,
            gbq_client = gbq_client,
            if_exists = 'append'
        )

        setTableExpiration(
            table_ref = f'{gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_TRANSACTIONS_EAN',
            expiration = expiration,
            gbq_client= gbq_client
        )

        setTableExpiration(
            table_ref = f'{gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_TRANSACTIONS_GRUPO',
            expiration = expiration,
            gbq_client= gbq_client
        )

        setTableExpiration(
            table_ref = f'{gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_DISTANCE_COSINE',
            expiration = expiration,
            gbq_client= gbq_client
        )

        setTableExpiration(
            table_ref = f'{gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_NEEDS_FILL',
            expiration = expiration,
            gbq_client= gbq_client
        )

        setTableExpiration(
            table_ref = f'{gcp_project}.TMP.TMP_PERSONALIZED_PRODUCTS_CUSTOMER_FILL',
            expiration = expiration,
            gbq_client= gbq_client
        )

        setTableExpiration(
            table_ref = f'{gcp_project}.TMP.BASE_PERSONALIZED_PRODUCTS',
            expiration = expiration,
            gbq_client= gbq_client
        )

    deleteFromTable(
        table_ref=f'{gcp_project}.ECOMMERCE.PERSONALIZED_PRODUCTS',
        where_clause=f"date = '{date_table}'",
        gbq_client=gbq_client,
    )

    createTableAsSelect(
        query=SQL_QUERIES['allocation_personalized_products'].substitute(
            gcp_project = gcp_project,
            gcp_project_cda = gcp_project_cda
        ),
        table_ref=f'{gcp_project}.ECOMMERCE.PERSONALIZED_PRODUCTS',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )

if __name__ == '__main__':
    main()

