# Default
from __future__ import annotations

import gc  # noqa: F401
import io  # noqa: F401
import os
import logging
import argparse
from logging import config

import numpy as np  # noqa: F401

# Pip
import pandas as pd  # noqa: F401
import pendulum

# Own
from google.cloud.bigquery import Client

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    uploadFrame,
    readBigQuery,
    deleteFromTable,
    setTableExpiration,
    createTableAsSelect,
    createTableFromJSON,  # noqa: F401
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
    '--month_interval', default=3, type=int,
    help='Number of months of past transactions from the execution date to view'
)
parser.add_argument(
    '--min_canasta', default=500, type=int,
    help='Minimum purchase quantity for a product'
)
parser.add_argument(
    '--min_freq_conj', default=500, type=int,
    help='Minimum quantity for joint product purchases'
)
parser.add_argument(
    '--max_ir', default=0.8, type=float,
    help='Maximum allowed value for the imbalance ratio'
)
parser.add_argument(
    '--max_compl', default=250, type=int,
    help='Maximum allowed value for the imbalance ratio'
)

# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'base':
    """
    WITH TRANSACCIONES AS (
        SELECT
        SALES.market_basket_key,
        CAST(SALES.SKU_PRODUCT AS INT) AS material
        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM` AS SALES

        INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE` AS STORE
        ON SALES.STORE_ID = STORE.STORE_ID

        LEFT JOIN (
            SELECT MARKET_BASKET_KEY
            FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
            WHERE canal_venta IN ('PEDIDOS YA','UBER EATS','RAPPI','RAPPI TURBO')
        ) ECOMMERCE
        ON SALES.MARKET_BASKET_KEY = ECOMMERCE.MARKET_BASKET_KEY

        WHERE
            ECOMMERCE.MARKET_BASKET_KEY IS NULL
            AND STORE.STORE_BANNER = '${store_banner}'
            AND SALES.VALUE > 0
            AND SALES.ITM_TXN_FCN_TP_DSC = 'V'
            AND SALES.TRANSACTION_TYPE IN ('TN','TF','BX','B','BE','F','NC')
            AND SALES.TRANSACTION_DATE >= DATE_SUB('${fecha}', INTERVAL ${month_interval} MONTH)
            AND SALES.TRANSACTION_DATE < '${fecha}'
    ),

    FRECUENCIA AS (
        SELECT
            material,
            COUNT(DISTINCT market_basket_key) AS freq_i
        FROM TRANSACCIONES
        GROUP BY material
        HAVING COUNT(DISTINCT market_basket_key) >= ${min_canasta}
    ),

    TRANSACCIONES_FILTRADAS AS (
        SELECT t.*
        FROM TRANSACCIONES t

        INNER JOIN FRECUENCIA f
        ON t.material = f.material
    ),

    TOTAL_TRANSACCIONES AS (
        SELECT COUNT(DISTINCT market_basket_key) AS total_transacciones
        FROM TRANSACCIONES
    ),

    PARES_COOCURRENCIA AS (
        SELECT
            t1.material AS material_a,
            t2.material AS material_b,
            COUNT(DISTINCT t1.market_basket_key) AS freq_conjunta
        FROM TRANSACCIONES_FILTRADAS t1
        JOIN TRANSACCIONES_FILTRADAS t2
            ON t1.market_basket_key = t2.market_basket_key
            AND t1.material < t2.material
        GROUP BY material_a, material_b
        HAVING COUNT(DISTINCT t1.market_basket_key) >= ${min_freq_conj}
    ),

    METRICAS_BASE AS (
        SELECT
            p.material_a,
            p.material_b,
            p.freq_conjunta,
            SAFE_DIVIDE(p.freq_conjunta, tt.total_transacciones) AS support,
            SAFE_DIVIDE(fa.freq_i, tt.total_transacciones) AS p_a,
            SAFE_DIVIDE(fb.freq_i, tt.total_transacciones) AS p_b,
            SAFE_DIVIDE(p.freq_conjunta, fa.freq_i) AS confidence_a_b,
            SAFE_DIVIDE(p.freq_conjunta, fb.freq_i) AS confidence_b_a,
            SAFE_DIVIDE(
                SAFE_DIVIDE(p.freq_conjunta, tt.total_transacciones),
                SAFE_DIVIDE(fa.freq_i, tt.total_transacciones) * SAFE_DIVIDE(fb.freq_i, tt.total_transacciones)
            ) AS lift,
            LN(SAFE_DIVIDE(
                SAFE_DIVIDE(p.freq_conjunta, tt.total_transacciones),
                SAFE_DIVIDE(fa.freq_i, tt.total_transacciones) * SAFE_DIVIDE(fb.freq_i, tt.total_transacciones)
            )) AS pmi
        FROM PARES_COOCURRENCIA p

        CROSS JOIN TOTAL_TRANSACCIONES tt

        JOIN FRECUENCIA fa
        ON p.material_a = fa.material

        JOIN FRECUENCIA fb
        ON p.material_b = fb.material
    ),

    METRICAS AS (
        SELECT
            *,
            SAFE_DIVIDE(pmi, -LN(support)) AS npmi,
            (confidence_a_b + confidence_b_a) / 2 AS kulczynski,
            SAFE_DIVIDE(ABS(p_a - p_b), p_a + p_b - support) AS imbalance_ratio,
            CASE
                WHEN confidence_a_b > p_b THEN SAFE_DIVIDE(confidence_a_b - p_b, 1 - p_b)
                WHEN confidence_a_b < p_b THEN SAFE_DIVIDE(confidence_a_b - p_b, p_b)
                ELSE 0
            END AS certainty_factor_a_b,
            CASE
                WHEN confidence_b_a > p_a THEN SAFE_DIVIDE(confidence_b_a - p_a, 1 - p_a)
                WHEN confidence_b_a < p_a THEN SAFE_DIVIDE(confidence_b_a - p_a, p_a)
                ELSE 0
            END AS certainty_factor_b_a
        FROM METRICAS_BASE
        WHERE
            SAFE_DIVIDE(pmi, -LN(support)) > 0
            AND TRUNC(SAFE_DIVIDE(ABS(p_a - p_b), p_a + p_b - support),2) < ${max_ir}
    ),

    EMBEDDINGS AS (
        SELECT
            e.sku AS material,
            [e.dim_0, e.dim_1, e.dim_2, e.dim_3, e.dim_4, e.dim_5, e.dim_6, e.dim_7, e.dim_8, e.dim_9,
            e.dim_10, e.dim_11, e.dim_12, e.dim_13, e.dim_14, e.dim_15, e.dim_16, e.dim_17, e.dim_18, e.dim_19,
            e.dim_20, e.dim_21, e.dim_22, e.dim_23, e.dim_24, e.dim_25, e.dim_26, e.dim_27, e.dim_28, e.dim_29,
            e.dim_30, e.dim_31, e.dim_32, e.dim_33, e.dim_34, e.dim_35, e.dim_36, e.dim_37, e.dim_38, e.dim_39,
            e.dim_40, e.dim_41, e.dim_42, e.dim_43, e.dim_44, e.dim_45, e.dim_46, e.dim_47, e.dim_48, e.dim_49,
            e.dim_50, e.dim_51, e.dim_52, e.dim_53, e.dim_54, e.dim_55, e.dim_56, e.dim_57, e.dim_58, e.dim_59,
            e.dim_60, e.dim_61, e.dim_62, e.dim_63, e.dim_64, e.dim_65, e.dim_66, e.dim_67, e.dim_68, e.dim_69,
            e.dim_70, e.dim_71, e.dim_72, e.dim_73, e.dim_74, e.dim_75, e.dim_76, e.dim_77, e.dim_78, e.dim_79,
            e.dim_80, e.dim_81, e.dim_82, e.dim_83, e.dim_84, e.dim_85, e.dim_86, e.dim_87, e.dim_88, e.dim_89,
            e.dim_90, e.dim_91, e.dim_92, e.dim_93, e.dim_94, e.dim_95, e.dim_96, e.dim_97, e.dim_98, e.dim_99
            ] AS vector_emb
        FROM `${gcp_project}.ML_LAB.W2V_SKU_EMBEDDINGS` e

        INNER JOIN FRECUENCIA f
        ON e.sku = f.material

        WHERE
            e.date = '${fecha}'
            AND e.store_banner = '${store_banner}'
    ),

    SIMILITUD_COSENO AS (
        SELECT
            e1.material AS material_a,
            e2.material AS material_b,
            1 - ML.DISTANCE(e1.vector_emb, e2.vector_emb, 'COSINE') AS similitud_coseno
        FROM EMBEDDINGS e1
        JOIN EMBEDDINGS e2
            ON e1.material < e2.material
    ),

    METRICAS_CON_EMB AS (
        SELECT M.*, S.similitud_coseno
        FROM METRICAS M
        JOIN SIMILITUD_COSENO S
            ON M.material_a = S.material_a
            AND M.material_b = S.material_b
    ),

    DIRECCIONAL AS (
        SELECT
            material_a AS producto_base,
            material_b AS producto_complementario,
            freq_conjunta, support,
            p_a, p_b,
            confidence_a_b, confidence_b_a,
            npmi, kulczynski, imbalance_ratio,
            certainty_factor_a_b, certainty_factor_b_a,
            similitud_coseno
        FROM METRICAS_CON_EMB
    )

    SELECT D.*
    FROM DIRECCIONAL D

    LEFT JOIN (
        SELECT sku, substitute
        FROM `${gcp_project}.ML_LAB.SKU_SUBSTITUTES_BY_CATEGORY`
        WHERE date = '${fecha}'
        AND store_banner = '${store_banner}'
    ) S
    ON (D.producto_base = S.sku AND D.producto_complementario = S.substitute)
    WHERE S.substitute IS NULL
    """,  # noqa: E501

    'ranking':
    """
    SELECT
        producto_base,
        producto_complementario,
        npmi,
        confidence_a_b,
        certainty_factor_a_b,
        similitud_coseno
    FROM `${gcp_project}.TMP.TMP_BASE_COMPLEMENTARY_PRODUCTS_${upper_store_banner}`
    """
})

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------

def main() -> None:
    user = 'complementary_products'

    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project: str = args['project_id']
    store_banner: str = args['store_banner']
    month_interval: int = args['month_interval']
    min_canasta: int = args['min_canasta']
    min_freq_conj: int = args['min_freq_conj']
    max_ir: float = args['max_ir']
    max_compl: int = args['max_compl']
    execution_date: pendulum.Date = pendulum.date(
        *list(map(int, args['execution_date'].split('-')))
    ).set(
        day=1
    )

    gbq_client = Client()
    upper_store_banner = store_banner.upper()

    weight_npmi = 0.35
    weight_certainity = 0.275
    weight_sm = 0.275
    weight_confidence = 0.1

    logging.info(f'gcp_project = {gcp_project}')
    logging.info(f'execution_date = {execution_date}')
    logging.info(f'store_banner = {store_banner}')
    logging.info(f'month_interval = {month_interval}')
    logging.info(f'min_canasta = {min_canasta}')
    logging.info(f'min_freq_conj = {min_freq_conj}')
    logging.info(f'weight_npmi = {weight_npmi}')
    logging.info(f'max_ir = {max_ir}')
    logging.info(f'max_compl = {max_compl}')
    logging.info(f'weight_certainity = {weight_certainity}')
    logging.info(f'weight_sm = {weight_sm}')
    logging.info(f'weight_confidence = {weight_confidence}')

    now = pendulum.now()
    expiration = now.add(minutes=1440)

    logging.info('Creacion Tabla Base')

    createTableAsSelect(
        query=SQL_QUERIES['base'].substitute(
            gcp_project = gcp_project,
            store_banner = store_banner,
            fecha = execution_date,
            month_interval = month_interval,
            min_canasta = min_canasta,
            min_freq_conj = min_freq_conj,
            max_ir = max_ir
        ),
        table_ref=f'{gcp_project}.TMP.TMP_BASE_COMPLEMENTARY_PRODUCTS_{upper_store_banner}',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )

    setTableExpiration(
        table_ref = f'{gcp_project}.TMP.TMP_BASE_COMPLEMENTARY_PRODUCTS_{upper_store_banner}',
        expiration = expiration,
        gbq_client= gbq_client
    )

    logging.info('Rankeo de productos complementarios')

    ranking = readBigQuery(SQL_QUERIES['ranking'].substitute(
        gcp_project = gcp_project,
        upper_store_banner = upper_store_banner
        ),
        user = user,
        gbq_client = gbq_client
    )

    ranking['score'] = (
        weight_npmi * ranking['npmi'] +
        weight_certainity * ranking['certainty_factor_a_b'] +
        weight_sm * ranking['similitud_coseno'] +
        weight_confidence * ranking['confidence_a_b']
    )

    ranking['relevance'] = (
        ranking.groupby('producto_base')['score']
        .rank(method='first', ascending=False)
        .astype(int)
    )

    ranking = ranking[ranking['relevance'] <= max_compl]

    ranking['store_banner'] = store_banner
    ranking['date'] = execution_date

    logging.info('Ingesta de datos')

    deleteFromTable(
        table_ref=f'{gcp_project}.ML_LAB.COMPLEMENTARY_PRODUCTS',
        where_clause=f"date = '{execution_date}' and store_banner = '{store_banner}'",
        gbq_client=gbq_client,
    )

    uploadFrame(
        ranking[[
            'producto_base',
            'producto_complementario',
            'score',
            'relevance',
            'store_banner',
            'date'
        ]],
        table_ddl_json_path=os.path.join('gbq_objects','complementary_products.json'),
        project = gcp_project,
        gbq_client = gbq_client,
        if_exists = 'append'
    )

if __name__ == '__main__':
    main()
