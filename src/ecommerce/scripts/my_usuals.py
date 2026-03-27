from __future__ import annotations

# Default
import os
import logging
import argparse
from logging import config

# pip
import pendulum
from google.cloud.bigquery import Client

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    deleteFromTable,
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
    '--project_name', type=str, required=True,
    help='Name fo the Advanced Analytics project executed'
)
parser.add_argument(
    '--gcp_project', type=str, required=True,
    help='Name of the GCP project billed. Used to differenciate dev from prod'
)
parser.add_argument(
    '--execution_date', type=str, required=True,
    help='DAG execution date'
)
parser.add_argument(
    '--store_banner', type=str, required=True,
    choices=['Unimarc', 'Alvi', 'Super 10', 'Mayorista'],
    help='SMU subsidiary for which the allocation will be made'
)
parser.add_argument(
    '--rollback_months', default=6, type=int,
    help='Number of months of transactions from the execution date taken'
)
parser.add_argument(
    '--rollback_months_filter', default=12, type=int,
    help='Number of months of transactions from the execution date taken'
)
parser.add_argument(
    '--min_transacted_months', default=3, type=int,
    help='Min number of different months with purchases for each user'
)
parser.add_argument(
    '--top_n', default=100, type=int,
    help='Max number of my usuals to be assigned to each user'
)


# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'base_my_usuals':
    """
    WITH max_usuals_per_customer AS (
        SELECT
            customer_key,
            CASE
                WHEN (lt_50 + lt_30) = 0 THEN ${top_n}
                WHEN (lt_50 + lt_30) = 1 THEN 50
                ELSE 30
            END AS max_usuals

        FROM (
            SELECT
                customer_key,
                MAX(basket_quantity) AS max_items,
                CASE
                    WHEN MAX(basket_quantity) <= 30 THEN 1
                    ELSE 0
                END AS lt_30,
                CASE
                    WHEN MAX(basket_quantity) <= 50 THEN 1
                    ELSE 0
                END AS lt_50
            FROM `${gcp_project}.CDA_VISTAS.VW_SALES_BASKET` sales_basket


            INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE` dim_store
            USING (store_id)

            INNER JOIN `${gcp_project}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
            USING (market_basket_key)

            WHERE
                transaction_date >= DATE('${execution_date}') - INTERVAL ${rollback_months_filter} MONTH
                AND transaction_date < DATE('${execution_date}')
                AND canal_venta = 'E-COMMERCE'
                AND store_banner = '${store_banner}'
                AND fnc_doc_tp_dsc IN ('NE','FX','BX','B ','BE','FE','F ','NC','TN','TF')
                AND itm_txn_fcn_tp_dsc = 'V'
                AND total_value > 0
            GROUP BY 1
        )
    ),

    max_usuals_filter AS (
        SELECT
            customer_key,
            30 AS max_usuals
        FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_APP_USERS`
        LEFT JOIN (
            SELECT
                customer_key,
                1 AS in_filter
            FROM max_usuals_per_customer
        )
        USING (customer_key)
        WHERE in_filter IS NULL
            AND unimarc_logged = 1
        GROUP BY 1

        UNION ALL

        SELECT *
        FROM max_usuals_per_customer
    ),

    transactions AS (
        SELECT
            customer_key AS customer_id,
            CAST(ean AS BIGINT) AS ean,
            txn_key,
            -- I'm using log2() inspired by DCG@K
            LOG(2,
                -- Must add bc starts from 0
                2 + DATE_DIFF(
                    DATE('${execution_date}'),
                    transaction_date,
                    ISOWEEK
                )
            ) AS backwards_date_week_diff,
            FORMAT_DATE('%m%Y', DATE(transaction_date)) AS transaction_month

        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM` sales_item

        INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE` dim_store
        USING (store_id)

        LEFT JOIN (
            SELECT
                market_basket_key,
                TRUE AS from_other_ecommerce
            FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
            WHERE canal_venta IN ('PEDIDOS YA','UBER EATS','RAPPI','RAPPI TURBO')
        ) external_ecommerce_filter
        USING (market_basket_key)

        WHERE
            transaction_date >= DATE('${execution_date}') - INTERVAL ${rollback_months} MONTH
            AND transaction_date < DATE('${execution_date}')
            AND store_banner = '${store_banner}'
            AND sku_product <> 'None'
            AND transaction_type IN ('NE','FX','BX','B ','BE','FE','F ','NC','TN','TF')
            AND itm_txn_fcn_tp_dsc = 'V'
            AND from_other_ecommerce IS NULL
    ),

    week_weighted_transactions AS (
        SELECT
            customer_id,
            ean,
            CAST(COUNT(DISTINCT txn_key) AS FLOAT64) / backwards_date_week_diff AS weighted_week_frequency
        FROM transactions
        INNER JOIN (
            SELECT customer_id
            FROM transactions
            -- Removes customers with less than min_transacted_months transactions in diferent months
            GROUP BY customer_id
            HAVING COUNT(DISTINCT transaction_month) >= ${min_transacted_months}
        ) filtered_transactions
        USING (customer_id)
        GROUP BY customer_id, ean, backwards_date_week_diff
    ),

    ranked_transactions AS (
        SELECT
            customer_id AS customer_key,
            ean,
            ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY SUM(weighted_week_frequency) DESC) AS relevance
        FROM week_weighted_transactions
        GROUP BY customer_id, ean
    )

    SELECT
        DATE('${execution_date}') AS date,
        customer_key,
        ean,
        relevance,
        '${store_banner}' AS store_banner
    FROM ranked_transactions
    INNER JOIN max_usuals_filter
    USING (customer_key)
    WHERE relevance <= max_usuals
    """, # noqa: E501

    'marcas_propias':
    """
    WITH MARCAS_PROPIAS AS (
        SELECT
            B.NM,
            ltrim(B.SKU_PRODUCT, '0') AS MATERIAL,
            B.EAN,
            B.GRUPO_DSC,B.BRAND_DESC
        FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT_HIERARCHY` B

        LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_ATTR` SKU
        on B.SKU_KEY = SKU.SKU_KEY

        WHERE SKU.TIPO_MARCA IN('1','3')
        ORDER BY B.GRUPO_ID,MATERIAL
    ),

    MARCAS_PROPIAS_VENTAS AS (
        SELECT DISTINCT EAN
        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM` A

        INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE` B
        USING(STORE_ID)

        WHERE
            TRANSACTION_DATE >= '${execution_date_3n}'
            AND TRANSACTION_DATE < '${execution_date}'
            AND EAN IN (SELECT EAN FROM MARCAS_PROPIAS)
            AND ITM_TXN_FCN_TP_DSC = 'V'
            AND TRANSACTION_TYPE IN ('TN','TF','BX','B','BE','F','NC','NE','FX','FE')
            AND VALUE > 0
            AND MARKET_BASKET_KEY  IN (
                SELECT MARKET_BASKET_KEY
                FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
                WHERE CANAL_VENTA = 'E-COMMERCE'
            )
            AND STORE_BANNER = '${store_banner}'
    )

    SELECT *
    FROM MARCAS_PROPIAS
    WHERE EAN IN (SELECT EAN FROM MARCAS_PROPIAS_VENTAS)
    """,

    'base_my_usuals_adj':
    """
    WITH SUB_CAT_MP AS (
        SELECT DISTINCT GRUPO_DSC
        FROM `${gcp_project}.TMP.TMP_MARCAS_PROPIAS_${upper_store_banner}`
    ),

    BASE_USUALS_1 AS (
        SELECT
            A.date,
            A.customer_key,
            A.ean,
            A.relevance,
            A.store_banner,
            B.GRUPO_DSC,
            B.SKU_PRODUCT,
            B.NM
        FROM `${gcp_project}.TMP.TMP_BASE_MY_USUALS` A

        INNER JOIN  (
            SELECT *
            FROM (
                SELECT
                    CAST(ean AS INT64) AS ean,
                    sku_product,
                    GRUPO_DSC,
                    NM,
                    ROW_NUMBER() OVER (PARTITION BY ean ORDER BY CAST(LTRIM(sku_product, '0') AS INT64)) AS ean_index
                FROM `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT`
                )
            WHERE ean_index = 1
        ) B
        USING (EAN)

        WHERE A.store_banner = '${store_banner}'
    ),

    BASE_USUALS_2 AS (
        SELECT
            A.*,
            CASE
                WHEN B.EAN IS NOT NULL THEN 'ES MARCA PROPIA'
            END AS MARCA_PROPIA,
            CASE
                WHEN C.GRUPO_DSC IS NOT NULL THEN 'SI' ELSE 'NO'
            END AS TIENE_MARCA_PROPIA
        FROM BASE_USUALS_1 A

        LEFT JOIN `${gcp_project}.TMP.TMP_MARCAS_PROPIAS_${upper_store_banner}` B
        ON CAST(A.EAN AS STRING) = B.EAN

        LEFT JOIN SUB_CAT_MP C
        ON A.GRUPO_DSC = C.GRUPO_DSC
    ),

    SUSTITUTOS AS (
        SELECT
            A.*,
            B.GRUPO_DSC AS GRUPO_DSC_SKU,
            C.GRUPO_DSC AS GRUPO_DSC_SUBSTITUTE
        FROM `${gcp_project}.ML_LAB.SKU_SUBSTITUTES_BY_CATEGORY` A

        INNER JOIN (
        SELECT DISTINCT SKU_PRODUCT,GRUPO_DSC
        FROM `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT`
        ) B
        ON A.sku = CAST(ltrim(b.SKU_PRODUCT, '0') AS INT)

        INNER JOIN (
        SELECT DISTINCT SKU_PRODUCT,GRUPO_DSC
        FROM `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT`
        ) C
        ON A.substitute = CAST(ltrim(c.SKU_PRODUCT, '0') AS INT)

        WHERE
            date = '${inicio_mes}'
            AND store_banner = 'Unimarc'
    ),

    SUSTITUTOS_ADJ AS (
        SELECT *
        FROM SUSTITUTOS
        WHERE GRUPO_DSC_SKU = GRUPO_DSC_SUBSTITUTE
    ),

    PROD_MP AS (
        SELECT
            sku,
            substitute as material_mp,
            substitution_score,
            EAN as ean_mp,
            NM AS nm_mp
        FROM (
            SELECT *,
            row_number() over (PARTITION BY sku ORDER BY substitution_rank asc) as rw
            FROM SUSTITUTOS_ADJ
            WHERE sku IN (
                SELECT CAST(ltrim(SKU_PRODUCT, '0') AS INT)
                FROM BASE_USUALS_2
                WHERE MARCA_PROPIA IS NULL
                AND TIENE_MARCA_PROPIA = 'SI'
            )
            AND substitute IN (
                SELECT CAST(MATERIAL AS INT)
                FROM `${gcp_project}.TMP.TMP_MARCAS_PROPIAS_${upper_store_banner}`
            )
        ) t

        INNER JOIN `${gcp_project}.TMP.TMP_MARCAS_PROPIAS_${upper_store_banner}` b
        ON CAST(t.substitute AS INT) = CAST(b.MATERIAL AS INT)
        WHERE rw = 1
    ),

    BASE_USUALS_3 AS (
        SELECT
            date,
            customer_key,
            ean,
            relevance,
            store_banner,
            GRUPO_DSC,
            SKU_PRODUCT,
            NM,MARCA_PROPIA,
            TIENE_MARCA_PROPIA,
            sku,
            material_mp,
            ean_mp,
            nm_mp
        FROM (
            SELECT *,
            row_number() over (PARTITION BY customer_key,ean ORDER BY ean_mp asc) as rw
            FROM BASE_USUALS_2 A
            LEFT JOIN PROD_MP B
            ON CAST(ltrim(A.SKU_PRODUCT, '0') AS INT) = CAST(B.sku AS INT)
        ) t
        WHERE rw = 1
    ),

    BASE_USUALS_4 AS (
        SELECT
            date,
            customer_key,
            ean,
            relevance,
            store_banner,
            GRUPO_DSC,
            SKU_PRODUCT,
            NM,
            MARCA_PROPIA,
            TIENE_MARCA_PROPIA,
            CASE WHEN rw = 1 THEN sku ELSE NULL END AS sku,
            CASE WHEN rw = 1 THEN material_mp ELSE NULL END AS material_mp,
            CASE WHEN rw = 1 THEN ean_mp ELSE NULL END AS ean_mp,
            CASE WHEN rw = 1 THEN nm_mp ELSE NULL END AS nm_mp
        FROM (
            SELECT *,
                ROW_NUMBER() OVER(PARTITION BY customer_key,GRUPO_DSC,MATERIAL_MP ORDER BY RELEVANCE ASC) AS rw
            FROM BASE_USUALS_3
        )
    ),

    ean_por_cliente as (
        SELECT DISTINCT customer_key,ean
        FROM BASE_USUALS_4
    ),

    BASE_USUALS_5 AS (
        SELECT
            A.*,
            IF(B.ean IS NOT NULL, 1, 0) AS aux_mp
        FROM BASE_USUALS_4 AS A

        LEFT JOIN ean_por_cliente AS B
        ON
            A.customer_key = B.customer_key
            AND CAST(A.ean_mp AS INT) = B.ean
    ),

    USUALS AS (
    SELECT
        date,
        customer_key,
        CAST(ean AS INT) AS ean_aux,
        relevance,
        store_banner
    FROM BASE_USUALS_5

    UNION ALL

    SELECT
        date,
        customer_key,
        CAST(EAN_MP AS INT) AS ean_aux,
        relevance + 0.5 AS relevance,
        store_banner
    FROM BASE_USUALS_5
    WHERE
        MARCA_PROPIA IS NULL
        AND TIENE_MARCA_PROPIA = 'SI'
        AND EAN_MP IS NOT NULL
        AND aux_mp = 0
    ),

    USUALS_AUX AS (
        SELECT *,
            IF(relevance != FLOOR(relevance), 1, 0) AS marca_propia
        FROM USUALS
    ),

    USUALS_FILTRADO AS (
        SELECT *,
            IF(marca_propia = 1,ROW_NUMBER() OVER(PARTITION BY customer_key,marca_propia ORDER BY relevance ASC), 1) AS rw
        FROM USUALS_AUX
    ),

    USUALS_ADJ AS (
        SELECT
            date,
            customer_key,
            ean_aux,
            relevance,
            store_banner
        FROM USUALS_FILTRADO
        WHERE (marca_propia = 0 OR (marca_propia = 1 AND rw <= 5))
    )

    SELECT
        date,
        customer_key,
        ean_aux as ean,
        DENSE_RANK() OVER (PARTITION BY date,customer_key ORDER BY relevance ASC) AS relevance,
        store_banner
    FROM USUALS_ADJ
    ORDER BY CUSTOMER_KEY,RELEVANCE ASC
    """,# noqa: E501

    'usuals_partition':
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

    FROM `${gcp_project}.TMP.TMP_BASE_MY_USUALS_ADJ`

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
    USING (EAN)

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

    WHERE store_banner = '${store_banner}'
    """,

    'compute_usuals_partition_alvi':
    """
    WITH max_usuals_per_customer AS (
        SELECT
            customer_key,
            CASE
                WHEN (lt_50 + lt_30) = 0 THEN ${top_n}
                WHEN (lt_50 + lt_30) = 1 THEN 50
                ELSE 30
            END AS max_usuals

        FROM (
            SELECT
                customer_key,
                MAX(basket_quantity) AS max_items,
                CASE
                    WHEN MAX(basket_quantity) <= 30 THEN 1
                    ELSE 0
                END AS lt_30,
                CASE
                    WHEN MAX(basket_quantity) <= 50 THEN 1
                    ELSE 0
                END AS lt_50
            FROM `${gcp_project}.CDA_VISTAS.VW_SALES_BASKET` sales_basket


            INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE` dim_store
            USING (store_id)

            INNER JOIN `${gcp_project}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
            USING (market_basket_key)

            WHERE
                transaction_date >= DATE('${execution_date}') - INTERVAL ${rollback_months_filter} MONTH
                AND transaction_date < DATE('${execution_date}')
                AND canal_venta = 'E-COMMERCE'
                AND store_banner = '${store_banner}'
                AND fnc_doc_tp_dsc IN ('NE','FX','BX','B ','BE','FE','F ','NC','TN','TF')
                AND itm_txn_fcn_tp_dsc = 'V'
                AND total_value > 0
            GROUP BY 1
        )
    ),

    max_usuals_filter AS (
        SELECT
            customer_key,
            30 AS max_usuals
        FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_APP_USERS`
        LEFT JOIN (
            SELECT
                customer_key,
                1 AS in_filter
            FROM max_usuals_per_customer
        )
        USING (customer_key)
        WHERE in_filter IS NULL
            AND unimarc_logged = 1
        GROUP BY 1

        UNION ALL

        SELECT *
        FROM max_usuals_per_customer
    ),

    transactions AS (
        SELECT
            customer_key AS customer_id,
            CAST(ean AS BIGINT) AS ean,
            txn_key,
            -- I'm using log2() inspired by DCG@K
            LOG(2,
                -- Must add bc starts from 0
                2 + DATE_DIFF(
                    DATE('${execution_date}'),
                    transaction_date,
                    ISOWEEK
                )
            ) AS backwards_date_week_diff,
            FORMAT_DATE('%m%Y', DATE(transaction_date)) AS transaction_month

        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM` sales_item

        INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE` dim_store
        USING (store_id)

        LEFT JOIN (
            SELECT
                market_basket_key,
                TRUE AS from_other_ecommerce
            FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
            WHERE canal_venta IN ('PEDIDOS YA','UBER EATS','RAPPI','RAPPI TURBO')
        ) external_ecommerce_filter
        USING (market_basket_key)

        WHERE
            transaction_date >= DATE('${execution_date}') - INTERVAL ${rollback_months} MONTH
            AND transaction_date < DATE('${execution_date}')
            AND store_banner = '${store_banner}'
            AND sku_product <> 'None'
            AND transaction_type IN ('NE','FX','BX','B ','BE','FE','F ','NC','TN','TF')
            AND itm_txn_fcn_tp_dsc = 'V'
            AND from_other_ecommerce IS NULL
    ),

    week_weighted_transactions AS (
        SELECT
            customer_id,
            ean,
            CAST(COUNT(DISTINCT txn_key) AS FLOAT64) / backwards_date_week_diff AS weighted_week_frequency
        FROM transactions
        INNER JOIN (
            SELECT customer_id
            FROM transactions
            -- Removes customers with less than min_transacted_months transactions in diferent months
            GROUP BY customer_id
            HAVING COUNT(DISTINCT transaction_month) >= ${min_transacted_months}
        ) filtered_transactions
        USING (customer_id)
        GROUP BY customer_id, ean, backwards_date_week_diff
    ),

    ranked_transactions AS (
        SELECT
            customer_id AS customer_key,
            ean,
            ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY SUM(weighted_week_frequency) DESC) AS relevance
        FROM week_weighted_transactions
        GROUP BY customer_id, ean
    ),

    base_my_usuals AS (
        SELECT
            DATE('${execution_date}') AS date,
            customer_key,
            ean,
            relevance,
            '${store_banner}' AS store_banner
        FROM ranked_transactions
        INNER JOIN max_usuals_filter
        USING (customer_key)
        WHERE relevance <= max_usuals
    )


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

    FROM base_my_usuals

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
    USING (EAN)

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
    """  # noqa: E501
})


# -------------------------------------------------------------------------
#                        Main Function
# -------------------------------------------------------------------------
def main():
    # ----------
    # Parameters
    # ----------
    args = vars(parser.parse_args())
    # Environment
    user: str = args['project_name'] + '_my_usuals'  # noqa: F841
    gcp_project: str = args['gcp_project']
    gcp_project_cda: str = {
        'cl-bigdata-analytics': 'cl-cda-unidata-dev',
        'cl-bigdata-analytics-dev': 'cl-cda-unidata-dev',
        'cl-bigdata-analytics-preprod': 'cl-cda-unidata-prod',
        'cl-bigdata-analytics-prod': 'cl-cda-unidata-prod',
    }[args['gcp_project']]
    execution_date: pendulum.Date = pendulum.date(
        *list(map(int, args['execution_date'].split('-')))
    )
    store_banner: str = args['store_banner']
    rollback_months: int = args['rollback_months']
    rollback_months_filter: int = args['rollback_months_filter']
    min_transacted_months: int = args['min_transacted_months']
    top_n: int = args['top_n']

    execution_date_3n = execution_date.subtract(months=3).replace(day=1)
    inicio_mes = execution_date.replace(day=1)
    upper_store_banner = store_banner.upper()

    # Hardcoded
    gbq_client = Client()
    table_ref=f'{gcp_project}.ECOMMERCE.MY_USUALS'
    table_base_ref=f'{gcp_project}.TMP.TMP_BASE_MY_USUALS'
    table_base_adj_ref=f'{gcp_project}.TMP.TMP_BASE_MY_USUALS_ADJ'
    table_mp_ref=f'{gcp_project}.TMP.TMP_MARCAS_PROPIAS_{upper_store_banner}'

    logging.info(f'gcp_project = {gcp_project}')
    logging.info(f'gcp_project_cda = {gcp_project_cda}')
    logging.info(f'execution_date = {execution_date}')
    logging.info(f'execution_date_3n = {execution_date_3n}')
    logging.info(f'inicio_mes = {inicio_mes}')
    logging.info(f'store_banner = {store_banner}')
    logging.info(f'rollback_months: {rollback_months}')
    logging.info(f'rollback_months_filter: {rollback_months_filter}')
    logging.info(f'min_transacted_months: {min_transacted_months}')
    logging.info(f'top_n: {top_n}')

    # Create table using DDL JSON
    logging.info('Creating table schema if needed')
    createTableFromJSON(
        table_ddl_json_path=os.path.join('gbq_objects', 'my_usuals.json'),
        project=gcp_project,
        gbq_client=gbq_client,
        if_exists='ignore',
    )

    # Remove past run if needed
    logging.info(f'Removing past run from {table_ref}')
    store_banner_numbers = {
            'Unimarc': 1,
            'Mayorista': 4,
            'Alvi': 5,
            'Super 10': 15,
    }
    deleteFromTable(
        table_ref=table_ref,
        where_clause=f"""
            date = '{execution_date}'
            AND store_banner = {store_banner_numbers[store_banner]}
        """,
        gbq_client=gbq_client,
    )


    if store_banner == 'Unimarc':

        createTableAsSelect(
            query=SQL_QUERIES['base_my_usuals'].substitute(
                gcp_project=gcp_project,
                execution_date=execution_date,
                store_banner=store_banner,
                rollback_months=rollback_months,
                rollback_months_filter=rollback_months_filter,
                min_transacted_months=min_transacted_months,
                top_n=top_n,
            ),
            table_ref=table_base_ref,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            use_legacy_sql=False,
            gbq_client=gbq_client,
        )

        createTableAsSelect(
            query=SQL_QUERIES['marcas_propias'].substitute(
                gcp_project=gcp_project,
                execution_date=execution_date,
                execution_date_3n=execution_date_3n,
                store_banner=store_banner
            ),
            table_ref=table_mp_ref,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            use_legacy_sql=False,
            gbq_client=gbq_client
        )

        createTableAsSelect(
            query=SQL_QUERIES['base_my_usuals_adj'].substitute(
                gcp_project=gcp_project,
                inicio_mes = inicio_mes,
                store_banner = store_banner,
                upper_store_banner = upper_store_banner
            ),
            table_ref=table_base_adj_ref,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            use_legacy_sql=False,
            gbq_client=gbq_client
        )

        # Create the table
        logging.info('Creating new partition')
        createTableAsSelect(
            query=SQL_QUERIES['usuals_partition'].substitute(
                gcp_project=gcp_project,
                gcp_project_cda=gcp_project_cda,
                store_banner=store_banner
            ),
            table_ref=table_ref,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_APPEND',
            use_legacy_sql=False,
            gbq_client=gbq_client,
        )

        logging.info('Done!')

    elif store_banner == 'Alvi':
        # Create the table
        logging.info('Creating new partition')
        createTableAsSelect(
            query=SQL_QUERIES['compute_usuals_partition_alvi'].substitute(
                gcp_project=gcp_project,
                gcp_project_cda=gcp_project_cda,
                execution_date=execution_date,
                store_banner=store_banner,
                rollback_months=rollback_months,
                rollback_months_filter=rollback_months_filter,
                min_transacted_months=min_transacted_months,
                top_n=top_n,
            ),
            table_ref=table_ref,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_APPEND',
            use_legacy_sql=False,
            gbq_client=gbq_client,
        )

        logging.info('Done!')


if __name__ == '__main__':
    main()
