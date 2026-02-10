# Default
from __future__ import annotations

import os

# Pip
import logging
import argparse
from logging import config

import pandas as pd
import pendulum  # noqa: F401
from google.cloud import bigquery  # noqa: F401
from google.cloud.bigquery import Client

import common.gcp_extended.bigquery as gbq_extended  # noqa: F401

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import uploadFrame, readBigQuery, createTableAsSelect


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


# -------------------------------------------------------------------------
# SQL Queries
# -------------------------------------------------------------------------

FULL_SALES_NEW_QUERIES = QueryDict({
    'Alvi':
    """
    WITH FULL_SALES as (
        WITH DP AS (
            SELECT
                CAT_ID AS CATEGORY_CODE,
                BRAND_DESC AS BRAND,
                LTRIM(SKU_PRODUCT,'0') AS SKU_PRODUCT,
                EAN,
                CONCAT (CAT_DSC,' ',BRAND_DESC) AS CAT_BRAND,
                CONT_CONV_UMB AS SALES_UNIT
            FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_PRODUCT`
            GROUP BY BRAND_DESC,CAT_ID,SKU_PRODUCT,EAN,CONT_CONV_UMB,CAT_DSC
        ),

        DIM_CUSTOMER AS (
            SELECT
                CTD.CUSTOMER_KEY,
                MONTHID,
                CASE
                    WHEN SHABIT IN('VIP','ORO') THEN 1 ELSE 0
                END AS LOYALTY,
                SHABIT,
                NIVEL_INFORMADO,
                CTD.CUSTOMER_TYPE_DET
            FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_SHABITS_UNIDATA_ALVI` SHABITS

            INNER JOIN (
            SELECT
                CUSTOMER_TYPE_DET,
                CUSTOMER_KEY,
                MONTH_ID
            FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_ALVI_TYPE` CTD
            WHERE
                PARSE_DATE('%Y%m%d',CONCAT(CAST(MONTH_ID AS STRING),'01')) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 WEEK) AND CURRENT_DATE()
                AND CUSTOMER_TYPE_DET IN ('COMERCIANTE_DECLARADO', 'COMERCIANTE_INFERIDO')
            ) AS CTD
            ON
                LPAD(CAST(CTD.MONTH_ID AS STRING), 6, '0') = LPAD(CAST(SHABITS.MONTHID AS STRING), 6, '0')
                AND CTD.CUSTOMER_KEY = SHABITS.CUSTOMER_KEY
            WHERE
            PARSE_DATE('%Y%m%d',CONCAT(CAST(MONTHID AS STRING),'01')) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 WEEK) AND CURRENT_DATE()
        )

        SELECT
            DP.CAT_BRAND,
            DP.SALES_UNIT,
            CAST(DP.SKU_PRODUCT as INT64) as MATERIAL,
            FIT.QUANTITY AS OG_QUANTITY,
            FIT.QUANTITY_SU,
            CASE
                WHEN FIT.UNIDAD_DE_MEDIDA IN ('KGV','KG') THEN FIT.WEIGHT ELSE FIT.QUANTITY
            END AS QUANTITY,
            FIT.UNIDAD_DE_MEDIDA,
            FIT.VALUE,
            FIT.TXN_KEY,
            FIT.MARKET_BASKET_KEY,
            FIT.CUSTOMER_KEY AS CUSTOMER_ID,
            CAST(FIT.TRANSACTION_DATE AS DATE) AS TRANSACTION_DATE,
            DIM_CUSTOMER.LOYALTY,
            DIM_CUSTOMER.SHABIT,
            DIM_CUSTOMER.MONTHID
        FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_SALES_ITEM` AS FIT

        INNER JOIN (
            SELECT STORE_ID,STORE_NAME
            FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_STORE`
            WHERE
                STORE_BANNER = 'Alvi'
                AND STORE_NAME NOT IN ('ALVI CANASTA INSTITUCIONAL')
        ) AS DSH
        USING(STORE_ID)

        INNER JOIN DP
        ON FIT.EAN = DP.EAN

        INNER JOIN DIM_CUSTOMER
        ON
            DIM_CUSTOMER.CUSTOMER_KEY = FIT.CUSTOMER_KEY
            AND CAST(DIM_CUSTOMER.MONTHID AS STRING) = FORMAT_DATE('%Y%m', DATE_SUB(CAST(FIT.TRANSACTION_DATE AS DATE), INTERVAL 1 MONTH))

        LEFT JOIN (
            SELECT MARKET_BASKET_KEY
            FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
            WHERE CANAL_VENTA IN ('PEDIDOS YA','CORNER SHOP','RAPPI','RAPPI TURBO')
        ) AS ECOM
        ON FIT.MARKET_BASKET_KEY = ECOM.MARKET_BASKET_KEY

        WHERE
            CAST(FIT.TRANSACTION_DATE AS DATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 52 WEEK) AND CURRENT_DATE()
            AND FIT.VALUE > 0
            AND FIT.SKU_PRODUCT <> 'None'
            AND FIT.TRANSACTION_TYPE  IN ('NE','FX','BX','B ','BE','FE','F ','NC','TN','TF')
            AND FIT.ITM_TXN_FCN_TP_DSC = 'V'
            AND ECOM.MARKET_BASKET_KEY IS NULL
        )

    SELECT
        FULL_SALES.*,
        SUPP.supplier_nm,
        OIC.CICLO,
        OIC.INICIO_CICLO,
        OIC.FIN_CICLO
    FROM FULL_SALES

    JOIN (
        SELECT
            CICLO_INTERNO AS CICLO,
            INICIO_CICLO,
            FIN_CICLO
        FROM `cl-bigdata-analytics-preprod.FIDELIZACION.DIM_OFFER_ID_CYCLES`
        WHERE store_banner = 'Alvi'
    ) AS OIC
    ON FULL_SALES.TRANSACTION_DATE BETWEEN OIC.INICIO_CICLO AND OIC.FIN_CICLO

    LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_VTA_DIRECTA` VNP
    ON VNP.MARKET_BASKET_KEY = FULL_SALES.MARKET_BASKET_KEY

    LEFT JOIN `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_SUPPLIER` supp
    ON supp.MATERIAL = FULL_SALES.MATERIAL

    WHERE VNP.MARKET_BASKET_KEY IS NULL
    """, # noqa: E501

    'Unimarc':
    """
    WITH FULL_SALES as (
        WITH DP AS (
            SELECT
                CAT_ID AS CATEGORY_CODE,
                BRAND_DESC AS BRAND,
                LTRIM(SKU_PRODUCT,'0') AS SKU_PRODUCT,
                EAN,
                CONCAT (CAT_DSC,' ',BRAND_DESC) AS CAT_BRAND,
                CONT_CONV_UMB AS SALES_UNIT
            FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_PRODUCT`
            GROUP BY BRAND_DESC,CAT_ID,SKU_PRODUCT,EAN,CONT_CONV_UMB,CAT_DSC
        ),

    DIM_CUSTOMER AS (
        SELECT
            CUSTOMER_KEY,
            MONTHID,
            CASE
                WHEN SHABIT IN('VIP','ORO', 'VIP Platino') THEN 1 ELSE 0
            END AS LOYALTY,
            SHABIT,
            NIVEL_INFORMADO
        FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_SHABITS_UNIDATA` SHABITS
        WHERE PARSE_DATE('%Y%m%d',CONCAT(CAST(MONTHID AS STRING),'01')) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 WEEK) AND CURRENT_DATE()
    )

    SELECT
        DP.CAT_BRAND,
        DP.SALES_UNIT,
        FIT.QUANTITY AS OG_QUANTITY,
        FIT.QUANTITY_SU,
        CASE
            WHEN FIT.UNIDAD_DE_MEDIDA IN ('KGV', 'KG') THEN FIT.WEIGHT ELSE FIT.QUANTITY
        END AS QUANTITY,
        FIT.VALUE,
        FIT.TXN_KEY,
        FIT.MARKET_BASKET_KEY,
        FIT.CUSTOMER_KEY AS CUSTOMER_ID,
        CAST(FIT.TRANSACTION_DATE AS DATE) AS TRANSACTION_DATE,
        DIM_CUSTOMER.LOYALTY,
        DIM_CUSTOMER.SHABIT,
        DIM_CUSTOMER.MONTHID
    FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_SALES_ITEM` AS FIT

    INNER JOIN (
        SELECT STORE_ID,STORE_NAME
        FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_STORE`
        WHERE STORE_BANNER = 'Unimarc'
    ) AS DSH
    USING(STORE_ID)

    INNER JOIN DP
    ON FIT.EAN = DP.EAN

    INNER JOIN DIM_CUSTOMER
    ON
        DIM_CUSTOMER.CUSTOMER_KEY = FIT.CUSTOMER_KEY
        AND CAST(DIM_CUSTOMER.MONTHID AS STRING) = FORMAT_DATE('%Y%m', DATE_SUB(CAST(FIT.TRANSACTION_DATE AS DATE), INTERVAL 1 MONTH))

    LEFT JOIN (
        SELECT MARKET_BASKET_KEY
        FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
        WHERE CANAL_VENTA IN ('PEDIDOS YA','CORNER SHOP','RAPPI','RAPPI TURBO')
    ) AS ECOM
    ON FIT.MARKET_BASKET_KEY = ECOM.MARKET_BASKET_KEY

    WHERE
        CAST(FIT.TRANSACTION_DATE AS DATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 52 WEEK) AND CURRENT_DATE()
        AND FIT.VALUE > 0
        AND FIT.SKU_PRODUCT <> 'None'
        AND FIT.TRANSACTION_TYPE  IN ('NE','FX','BX','B ','BE','FE','F ','NC','TN','TF')
        AND FIT.ITM_TXN_FCN_TP_DSC = 'V'
        AND ECOM.MARKET_BASKET_KEY IS NULL
    )

    SELECT *
    FROM FULL_SALES
    """ # noqa: E501
})

FULL_SALES_OLD_QUERIES = QueryDict(
    common_query =
    """
    WITH LAST_WEEK_CUSTOMER_ORGANIZATION_SHABITS AS (
        WITH base AS (
            SELECT
            WEEK_ISO_ID,
            PARSE_DATE('%G-W%V-%u',CONCAT(SUBSTR(CAST(week_iso_id AS STRING), 1, 4),
                '-W',LPAD(SUBSTR(CAST(week_iso_id AS STRING), 5, 2), 2, '0'),'-1')
            ) AS week_start_monday
            FROM `cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_FACT_WEEK_CUSTOMER_ORGANIZATION_SHABITS`
            WHERE WEEK_ISO_ID >= 202301
            GROUP BY WEEK_ISO_ID
        ),

        enriched AS (
            SELECT
            week_iso_id,
            week_start_monday,
            DATE_ADD(week_start_monday, INTERVAL 6 DAY) AS week_end_sunday,
            EXTRACT(ISOYEAR FROM week_start_monday) AS iso_year,
            EXTRACT(ISOWEEK FROM week_start_monday) AS iso_week,
            EXTRACT(YEAR FROM week_start_monday)    AS cal_year_lunes,
            EXTRACT(MONTH FROM week_start_monday)   AS month_first
            FROM base
        ),

        rango AS (
            SELECT
            DATE_TRUNC(MIN(week_start_monday), MONTH) AS min_month,
            DATE_TRUNC(MAX(week_start_monday), MONTH) AS max_month
            FROM enriched
        ),

        meses AS (
            SELECT month_start
            FROM rango,
            UNNEST(GENERATE_DATE_ARRAY(min_month, max_month, INTERVAL 1 MONTH)) AS month_start
        ),

        ultimo_dia_mes AS (
            SELECT
            month_start,
            DATE_SUB(DATE_ADD(month_start, INTERVAL 1 MONTH), INTERVAL 1 DAY) AS month_end,
            EXTRACT(YEAR  FROM month_start) AS cal_year,
            EXTRACT(MONTH FROM month_start) AS cal_month
            FROM meses
        ),

        target_semana_mes AS (
            SELECT
            cal_year,
            cal_month,
            CASE
                WHEN cal_month = 12 THEN cal_year -- diciembre: ISOYEAR = año calendario
                ELSE EXTRACT(ISOYEAR FROM month_end) -- otros meses: ISOYEAR del último día
            END AS iso_year_target,
            CASE
                WHEN cal_month = 12 THEN EXTRACT(ISOWEEK FROM DATE(cal_year, 12, 28)) -- última semana ISO del año cal_year
                ELSE EXTRACT(ISOWEEK FROM month_end) -- semana del último día del mes
            END AS iso_week_target
            FROM ultimo_dia_mes
        ),

        LAST_WEEK AS (
            SELECT
            --t.cal_year,
            e.week_iso_id as WEEK_ISO_ID, -- etiqueta ISO existente en tu tabla
            t.cal_month as MONTH_NUMBER
            --e.iso_year,
            --e.iso_week,
            --e.week_start_monday  AS semana_inicio_lunes,
            --e.week_end_sunday    AS semana_fin_domingo,
            --EXTRACT(MONTH FROM e.week_start_monday) AS month_first  -- (b) mes "primero"
            FROM target_semana_mes t
            JOIN enriched e
            ON e.iso_year = t.iso_year_target
            AND e.iso_week = t.iso_week_target
            ORDER BY t.cal_year, t.cal_month
        )

        SELECT
            a.ORGANIZATION_ID,
            b.WEEK_ISO_ID,
            a.CUSTOMER_ID,
            a.SHABITS,
            b.MONTH_NUMBER
        FROM `cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_FACT_WEEK_CUSTOMER_ORGANIZATION_SHABITS` a
        INNER JOIN LAST_WEEK b
        USING (WEEK_ISO_ID)
    ),

    FULL_SALES as (
        WITH DP AS (
            SELECT
                CAT_ID AS CATEGORY_CODE,
                BRAND_DESC AS BRAND,
                LTRIM(SKU_PRODUCT,'0') AS SKU_PRODUCT,
                EAN,
                CONCAT (CAT_DSC, ' ', BRAND_DESC) AS CAT_BRAND,
                CONT_CONV_UMB AS SALES_UNIT
            FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_PRODUCT`
            GROUP BY BRAND_DESC,CAT_ID,SKU_PRODUCT,EAN,CONT_CONV_UMB,CAT_DSC
        ),

        DIM_CUSTOMER AS (
            SELECT
                customer_id,
                shabits,
                CASE
                    WHEN SHABITS IN('VIP','ORO') THEN 1 ELSE 0
                END AS LOYALTY,
                week_iso_id,
                month_number,
                FORMAT_DATE('%Y%m',DATE_ADD(DATE_ADD(PARSE_DATE('%Y-%m-%d',
                    CONCAT(SUBSTR(CAST(week_iso_id AS STRING), 1, 4), '-01-01')),
                    INTERVAL CAST(SUBSTR(CAST(week_iso_id AS STRING), 5, 2) AS INT64) - 1 WEEK),
                    INTERVAL 1 MONTH)
                ) AS monthid
            FROM LAST_WEEK_CUSTOMER_ORGANIZATION_SHABITS
            where
                organization_id = ${organization_id}
                and DATE_ADD(DATE(CONCAT(SUBSTR(CAST(week_iso_id AS STRING), 1, 4), '-01-01')),
                    INTERVAL CAST(SUBSTR(CAST(week_iso_id AS STRING), 5, 2) AS INT64) - 1 WEEK)
                    BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -60 WEEK) AND CURRENT_DATE()
        )

        SELECT
            DP.CAT_BRAND,
            DP.SALES_UNIT,
            FIT.VALUE / FIT.UNIT_PRICE UNITS,
            FIT.QUANTITY AS OG_QUANTITY,
            FIT.QUANTITY_SU,
            CASE
                WHEN FIT.UNIDAD_DE_MEDIDA IN ('KGV','KG') THEN FIT.WEIGHT ELSE FIT.QUANTITY
            END AS QUANTITY,
            FIT.UNIDAD_DE_MEDIDA,
            FIT.VALUE,
            FIT.UNIT_PRICE,
            FIT.TXN_KEY,
            FIT.MARKET_BASKET_KEY,
            FIT.CUSTOMER_KEY AS CUSTOMER_ID,
            CAST(FIT.TRANSACTION_DATE AS DATE) AS TRANSACTION_DATE,
            DIM_CUSTOMER.LOYALTY,
            DIM_CUSTOMER.SHABITS,
            DIM_CUSTOMER.MONTHID,
            DIM_CUSTOMER.WEEK_ISO_ID AS WEEK_ID
        FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_SALES_ITEM` AS FIT

        INNER JOIN (
            SELECT STORE_ID,STORE_NAME
            FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_STORE`
            WHERE STORE_BANNER = '${store_banner}'
        ) AS DSH
        USING(STORE_ID)

        INNER JOIN DP
        ON FIT.EAN = DP.EAN

        LEFT JOIN DIM_CUSTOMER
        ON
            DIM_CUSTOMER.CUSTOMER_ID = FIT.CUSTOMER_KEY
            AND CAST(DIM_CUSTOMER.MONTHID AS STRING) = CONCAT(SUBSTR(CAST(FIT.TRANSACTION_DATE AS STRING),1,4),SUBSTR(CAST(FIT.TRANSACTION_DATE AS STRING),6,2))

        LEFT JOIN (
            SELECT MARKET_BASKET_KEY
            FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
            WHERE CANAL_VENTA IN ('PEDIDOS YA','CORNER SHOP','RAPPI','RAPPI TURBO')
        ) AS ECOM
        ON FIT.MARKET_BASKET_KEY = ECOM.MARKET_BASKET_KEY

        WHERE
            CAST(FIT.TRANSACTION_DATE AS DATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 52 WEEK) AND CURRENT_DATE()
            AND FIT.VALUE > 0
            AND FIT.SKU_PRODUCT <> 'None'
            AND FIT.TRANSACTION_TYPE  IN ('NE','FX','BX','B ','BE','FE','F ','NC','TN','TF')
            AND FIT.ITM_TXN_FCN_TP_DSC = 'V'
            AND ECOM.MARKET_BASKET_KEY IS NULL
        )
    """, # noqa: E501
    query_dict={
        'Mayorista':
        """
        SELECT *
        FROM FULL_SALES
        """,

        'Super 10':
        """
        SELECT *
        FROM FULL_SALES
        """
    })

SQL_QUERIES = QueryDict({
    'create_offer_tool_ranking':
    """
    WITH offer_tool_base AS(
        WITH total_summary AS (
            SELECT
                SUM(value) AS total_sales,
                COUNT(DISTINCT MARKET_BASKET_KEY) AS total_visits
            FROM `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_${upper_store_banner}`
        ),

        total_loyal_purchases AS (
            SELECT SUM(loyalty) as loyalty
            from `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_${upper_store_banner}`
            WHERE loyalty = 1
        )

        SELECT
            cat_brand,
            COUNT(DISTINCT customer_id) AS unique_customers,
            CAST(SUM(value) AS INT64) AS total_sales,
            SUM(loyalty) AS loyal_customers,
            COUNT(customer_id) AS not_unique_customers,
            SUM(quantity) AS units,
            COUNT(DISTINCT MARKET_BASKET_KEY) AS visits,
            CAST(SUM(loyalty) AS FLOAT64) / CAST(COUNT(customer_id) AS FLOAT64) AS percent_loyal,
            CAST(SUM(value) AS FLOAT64) / CAST((SELECT SUM(value)
                FROM `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_${upper_store_banner}`) AS FLOAT64) AS sales_contrib,
            CAST(COUNT(DISTINCT MARKET_BASKET_KEY) AS FLOAT64) / CAST((SELECT COUNT(DISTINCT MARKET_BASKET_KEY)
                FROM `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_${upper_store_banner}`) AS FLOAT64) AS basket_penetration,
            CAST(SUM(loyalty) AS FLOAT64) / CAST((SELECT(loyalty)
                FROM total_loyal_purchases) AS FLOAT64) AS loyalty_penetration
        FROM `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_${upper_store_banner}`
        WHERE cat_brand  NOT LIKE '%CIGARRILLOS%'
        GROUP BY cat_brand
    ),

    offer_tool_scored as (
        WITH min_max_values AS (
            SELECT
                MIN(basket_penetration) AS min_bp,
                MAX(basket_penetration) AS max_bp,
                MIN(loyalty_penetration) AS min_lp,
                MAX(loyalty_penetration) AS max_lp,
                MIN(sales_contrib) AS min_sc,
                MAX(sales_contrib) AS max_sc
            FROM offer_tool_base
        )

        SELECT
            unique_customers AS TOTAL_CUSTOMERS,
            TOTAL_SALES,
            cast(UNITS as INT64) as UNITS,
            VISITS,
            CAT_BRAND AS CATEGORIA_MARCA,
            CAST((basket_penetration - mv.min_bp) / (mv.max_bp - mv.min_bp) AS FLOAT64) AS NORMALIZED_BASKET_PENETRATION,
            CAST((loyalty_penetration - mv.min_lp) / (mv.max_lp - mv.min_lp) AS FLOAT64) AS NORMALIZED_LOYALTY_PENETRATION,
            CAST((sales_contrib - mv.min_sc) / (mv.max_sc - mv.min_sc) AS FLOAT64) AS NORMALIZED_SALES_CONTRIB,
            cast( 0.4 * CAST((loyalty_penetration - mv.min_lp) / (mv.max_lp - mv.min_lp) AS FLOAT64) + 0.4 *
                CAST((basket_penetration - mv.min_bp) / (mv.max_bp - mv.min_bp) AS FLOAT64) + 0.2 * CAST((sales_contrib - mv.min_sc) /
                (mv.max_sc - mv.min_sc) AS FLOAT64) as FLOAT64 ) as score
        FROM offer_tool_base, min_max_values mv
    )

    SELECT
        TOTAL_CUSTOMERS,
        TOTAL_SALES,
        UNITS,
        VISITS,
        CATEGORIA_MARCA,
        ROW_NUMBER() OVER (ORDER BY score desc) AS RANKING
    from offer_tool_scored
    """, # noqa: E501

    'create_offer_tool_ranking_alvi':
    """
    WITH offer_tool_base as (
        WITH total_summary AS (
            SELECT
            SUM(value) AS total_sales,
            COUNT(DISTINCT MARKET_BASKET_KEY) AS total_visits
            FROM `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_ALVI`
        ),

        total_loyal_purchases AS (
            SELECT SUM(loyalty) AS total_loyalty
            FROM `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_ALVI`
            WHERE loyalty = 1
        ),

        aggregated_data AS (
            SELECT
                t.cat_brand,
                t.supplier_nm,
                COUNT(DISTINCT t.customer_id) AS unique_customers,
                CAST(SUM(t.value) AS INT64) AS total_sales,
                SUM(t.loyalty) AS loyal_customers,
                COUNT(t.customer_id) AS not_unique_customers,
                SUM(t.quantity) AS units,
                COUNT(DISTINCT t.MARKET_BASKET_KEY) AS visits,
                CAST(SUM(t.loyalty) AS FLOAT64) / NULLIF(CAST(COUNT(t.customer_id) AS FLOAT64), 0) AS percent_loyal,
                CAST(SUM(t.value) AS FLOAT64) / NULLIF(CAST(ts.total_sales AS FLOAT64), 0) AS sales_contrib,
                CAST(COUNT(DISTINCT t.MARKET_BASKET_KEY) AS FLOAT64) / NULLIF(CAST(ts.total_visits AS FLOAT64), 0) AS basket_penetration,
                CAST(SUM(t.loyalty) AS FLOAT64) / NULLIF(CAST(tl.total_loyalty AS FLOAT64), 0) AS loyalty_penetration
            FROM `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_ALVI` t

            CROSS JOIN total_summary ts

            CROSS JOIN total_loyal_purchases tl

            GROUP BY t.cat_brand,t.supplier_nm,ts.total_sales,ts.total_visits,tl.total_loyalty

            UNION ALL

            SELECT
                t.cat_brand,
                'All' AS supplier_nm,
                COUNT(DISTINCT t.customer_id) AS unique_customers,
                CAST(SUM(t.value) AS INT64) AS total_sales,
                SUM(t.loyalty) AS loyal_customers,
                COUNT(t.customer_id) AS not_unique_customers,
                SUM(t.quantity) AS units,
                COUNT(DISTINCT t.MARKET_BASKET_KEY) AS visits,
                CAST(SUM(t.loyalty) AS FLOAT64) / NULLIF(CAST(COUNT(t.customer_id) AS FLOAT64), 0) AS percent_loyal,
                CAST(SUM(t.value) AS FLOAT64) / NULLIF(CAST(ts.total_sales AS FLOAT64), 0) AS sales_contrib,
                CAST(COUNT(DISTINCT t.MARKET_BASKET_KEY) AS FLOAT64) / NULLIF(CAST(ts.total_visits AS FLOAT64), 0) AS basket_penetration,
                CAST(SUM(t.loyalty) AS FLOAT64) / NULLIF(CAST(tl.total_loyalty AS FLOAT64), 0) AS loyalty_penetration
            FROM `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_ALVI` t

            CROSS JOIN total_summary ts

            CROSS JOIN total_loyal_purchases tl

            WHERE
            t.cat_brand IN (
                SELECT cat_brand
                FROM `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_ALVI`
                GROUP BY cat_brand
                HAVING COUNT(DISTINCT supplier_nm) > 1
                )
            GROUP BY t.cat_brand, ts.total_sales, ts.total_visits, tl.total_loyalty
        )

        SELECT * FROM aggregated_data
    ),

    filtered_brands AS (
        SELECT
            cat_brand,
            CASE
                WHEN COUNT(DISTINCT supplier_nm) = 1 THEN 'single_supplier'
                ELSE 'multiple_suppliers'
            END AS brand_type
        FROM offer_tool_base
        GROUP BY cat_brand
    ),

    offer_tool_scored AS (
        WITH min_max_values AS (
            SELECT
                MIN(basket_penetration) AS min_bp,
                MAX(basket_penetration) AS max_bp,
                MIN(loyalty_penetration) AS min_lp,
                MAX(loyalty_penetration) AS max_lp,
                MIN(sales_contrib) AS min_sc,
                MAX(sales_contrib) AS max_sc
            FROM offer_tool_base
            WHERE supplier_nm = 'All'OR cat_brand IN (
                SELECT cat_brand
                FROM filtered_brands WHERE brand_type = 'single_supplier'
            )
        )

        SELECT
            otb.unique_customers AS TOTAL_CUSTOMERS,
            otb.TOTAL_SALES,
            CAST(otb.UNITS AS INT64) AS UNITS,
            otb.VISITS,
            otb.CAT_BRAND AS CATEGORIA_MARCA,
            otb.supplier_nm,
            CAST((otb.basket_penetration - mv.min_bp) / NULLIF((mv.max_bp - mv.min_bp), 0) AS FLOAT64) AS NORMALIZED_BASKET_PENETRATION,
            CAST((otb.loyalty_penetration - mv.min_lp) / NULLIF((mv.max_lp - mv.min_lp), 0) AS FLOAT64) AS NORMALIZED_LOYALTY_PENETRATION,
            CAST((otb.sales_contrib - mv.min_sc) / NULLIF((mv.max_sc - mv.min_sc), 0) AS FLOAT64) AS NORMALIZED_SALES_CONTRIB,
            CAST(
                0.4 * CAST((otb.loyalty_penetration - mv.min_lp) / NULLIF((mv.max_lp - mv.min_lp), 0) AS FLOAT64) +
                0.4 * CAST((otb.basket_penetration - mv.min_bp) / NULLIF((mv.max_bp - mv.min_bp), 0) AS FLOAT64) +
                0.2 * CAST((otb.sales_contrib - mv.min_sc) / NULLIF((mv.max_sc - mv.min_sc), 0) AS FLOAT64)
            AS FLOAT64) AS score,
            CASE
                WHEN otb.supplier_nm = 'All' OR fb.brand_type = 'single_supplier' THEN 1 ELSE 0
            END AS is_ranked
        FROM offer_tool_base otb

        LEFT JOIN filtered_brands fb
        ON otb.cat_brand = fb.cat_brand

        CROSS JOIN min_max_values mv
    )

    SELECT
        CATEGORIA_MARCA,
        supplier_nm,
        TOTAL_CUSTOMERS,
        TOTAL_SALES,
        UNITS,
        VISITS,
        CASE
            WHEN is_ranked = 1 THEN ROW_NUMBER() OVER (PARTITION BY is_ranked ORDER BY score DESC)
            ELSE NULL
        END AS ranking,
        is_ranked
        FROM offer_tool_scored
    ORDER BY score DESC
    """, # noqa: E501

    'presentation':
    """
    WITH presentation AS (
        SELECT
            cat_brand,
            unidad_de_medida AS unit_of_measure,
            COUNT(MARKET_BASKET_KEY) AS n_sales,
            SUM(loyalty) AS loyalty_purchases,
            CASE
                WHEN unidad_de_medida IN ('DIS','PAQ','CS') THEN 1 ELSE 0
            END AS display
        FROM `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_${upper_store_banner}`
        GROUP BY cat_brand, unidad_de_medida
    ),

    aggregated AS (
        SELECT
            cat_brand,
            MAX(display) AS display,
            SUM(CASE WHEN display = 1 THEN n_sales ELSE 0 END) AS total_n_sales,
            SUM(CASE WHEN display = 1 THEN loyalty_purchases ELSE 0 END) AS total_loyalty_purchases
        FROM presentation
        GROUP BY cat_brand
    )

    SELECT
        cat_brand,
        display,
        total_n_sales AS n_sales,
        CASE
            WHEN total_n_sales > 0 THEN total_loyalty_purchases * 1.0 / total_n_sales
            ELSE 0
        END AS loyalty_penetration
    FROM aggregated
    """,

    'presentation_alvi':
    """
    WITH presentation AS (
        SELECT
            cat_brand,
            supplier_nm,
            unidad_de_medida AS unit_of_measure,
            COUNT(MARKET_BASKET_KEY) AS n_sales,
            SUM(loyalty) AS loyalty_purchases,
            CASE
                WHEN unidad_de_medida IN ('DIS','PAQ','CS') THEN 1 ELSE 0
            END AS display
        FROM `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_ALVI`
        GROUP BY cat_brand,unidad_de_medida,supplier_nm
    ),

    aggregated AS (
        SELECT
            cat_brand,
            supplier_nm,
            MAX(display) AS display,
            SUM(CASE WHEN display = 1 THEN n_sales ELSE 0 END) AS total_n_sales,
            SUM(CASE WHEN display = 1 THEN loyalty_purchases ELSE 0 END) AS total_loyalty_purchases
        FROM presentation
        GROUP BY cat_brand, supplier_nm
    )

    SELECT
        cat_brand,
        supplier_nm,
        display,
        total_n_sales AS n_sales,
        CASE
            WHEN total_n_sales > 0 THEN total_loyalty_purchases * 1.0 / total_n_sales
            ELSE 0
        END AS loyalty_penetration
    FROM aggregated
    """,

    'max_suggested_units':
    # Compute max suggested units (Alvi)
    """
    WITH customer_cycle_purchases AS (
        SELECT
            ciclo,
            ots.customer_id,
            cat_brand,
            supplier_nm,
            SUM(quantity) AS cycle_units,
            MAX(loyalty) AS loyalty,
            COUNT(ots.market_basket_key) AS n_visits
        FROM `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_ALVI` ots

        LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_VTA_DIRECTA` vnp
        ON ots.MARKET_BASKET_KEY = vnp.MARKET_BASKET_KEY

        WHERE vnp.MARKET_BASKET_KEY IS NULL
        GROUP BY ciclo, ots.customer_id, cat_brand, supplier_nm
    ),

    aupv AS (
        SELECT
            customer_cycle_purchases.*,
            cycle_units / n_visits AS units_per_visit
        FROM customer_cycle_purchases
    ),

    CLCP AS (
        SELECT
            cat_brand,
            supplier_nm,
            ciclo,
            CAST(APPROX_QUANTILES(units_per_visit, 100)[OFFSET(75)] AS INT64) AS avg_units_per_visit
        FROM aupv
        GROUP BY cat_brand, ciclo, supplier_nm
    ),

    final_table AS (
        SELECT
            cat_brand,
            supplier_nm,
            CAST(APPROX_QUANTILES(avg_units_per_visit, 100)[OFFSET(75)] AS INT64) AS max_suggested_units
        FROM CLCP
        GROUP BY cat_brand, supplier_nm

        UNION ALL

        SELECT
            cat_brand,
            'All' AS supplier_nm,
            max(max_suggested_units) AS max_suggested_units
        FROM (
            SELECT
                cat_brand,
                supplier_nm,
                CAST(APPROX_QUANTILES(avg_units_per_visit, 100)[OFFSET(75)] AS INT64) AS max_suggested_units
            FROM CLCP
            GROUP BY cat_brand, supplier_nm
        ) subquery
        WHERE cat_brand IN (
            SELECT cat_brand
            FROM CLCP
            GROUP BY cat_brand
            HAVING COUNT(DISTINCT supplier_nm) > 1
        )
        GROUP BY cat_brand
    )

    SELECT * FROM final_table
    """,  # noqa: E501

    'customers_per_cycle':
    # Computes avg customers per cycle (Alvi)
    """
    WITH customer_cycle_purchases AS (
        SELECT
            ciclo,
            ots.customer_id,
            cat_brand,
            supplier_nm,
            SUM(quantity) AS cycle_units,
            MAX(loyalty) AS loyalty,
            COUNT(ots.market_basket_key) AS n_visits
        FROM `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_ALVI` ots

        LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_VTA_DIRECTA` vnp
        ON ots.MARKET_BASKET_KEY = vnp.MARKET_BASKET_KEY

        WHERE vnp.MARKET_BASKET_KEY IS NULL
        GROUP BY ciclo, ots.customer_id, cat_brand, supplier_nm
    ),

    aupv AS (
        SELECT
            customer_cycle_purchases.*,
            cycle_units / n_visits AS units_per_visit
        FROM customer_cycle_purchases
    ),

    cpc AS (
        SELECT
            cat_brand,
            supplier_nm,
            ciclo,
            COUNT(DISTINCT customer_id) AS unique_customers
        FROM aupv
        WHERE loyalty = ${loyalty}
        GROUP BY cat_brand, ciclo, supplier_nm
    ),

    acpc AS (
        SELECT
            cat_brand,
            supplier_nm,
            CAST(APPROX_QUANTILES(unique_customers, 100)[OFFSET(75)] AS INT64) AS avg_cycle_customers
        FROM cpc
        GROUP BY ciclo, cat_brand, supplier_nm
    ),

    final_table AS (
        SELECT
            cat_brand,
            supplier_nm,
            CAST(APPROX_QUANTILES(avg_cycle_customers, 100)[OFFSET(75)] AS INT64) AS customers_per_cycle
        FROM acpc
        GROUP BY cat_brand, supplier_nm

        UNION ALL

        SELECT
            cat_brand,
            'All' AS supplier_nm,
            SUM(customers_per_cycle) AS customers_per_cycle
        FROM (
            SELECT
                cat_brand,
                supplier_nm,
                CAST(APPROX_QUANTILES(avg_cycle_customers, 100)[OFFSET(75)] AS INT64) AS customers_per_cycle
            FROM acpc
            GROUP BY cat_brand, supplier_nm
        ) subquery
        WHERE cat_brand IN (
            SELECT cat_brand
            FROM acpc
            GROUP BY cat_brand
            HAVING COUNT(DISTINCT supplier_nm) > 1
        )
        GROUP BY cat_brand
    )

    SELECT * FROM final_table
    """,  # noqa: E501

    'offer_tool_source':
    # Retrives all last 52 weeks transactions with useful info (Alvi)
    """
    SELECT *
    FROM `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_${upper_store_banner}` ots
    """,

    'ranking_offer_tool':
    # Retrives offer tool ranking
    """
    SELECT *
    FROM `cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_RANKING_${upper_store_banner}`
    """,

    'offer_tool_discount':
    # Reads table with suggested discount for offer tool
    """
    SELECT CAT_BRAND as cat_brand,DISCOUNT as discount
    FROM `cl-bigdata-analytics-preprod.ML_LAB.OFFER_TOOL_DISCOUNT`
    """,

    'cycles':
    # Gets last 52 week cycles info
    """
    SELECT CYCLE_ID,
        CYCLE_NUMBER,
        START_DATE,
        END_DATE
    FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_CYCLE_DH`
    WHERE
        (CYCLE_DESCRIPTION LIKE '%PERSONALIZED%' OR CYCLE_DESCRIPTION LIKE '%PENETRATION%')
        AND end_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 52 WEEK)
        AND organization_id = 5
        AND campaign_type_id in (2,6)
    """,

    'redemptions':
    # Retrives customer_key and nivel redemptions for given cycle
    """
    SELECT REDEMPTIONS.CUSTOMER_KEY AS CUSTOMER_ID,
            MAX(SHABITS.NIVEL_INFORMADO) AS NIVEL_INFORMADO
    FROM `cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_FACT_MONTH_DAY_CUSTOMER_CENTER_ARTICLE_REDEMPTION_MART` REDEMPTIONS
    INNER JOIN (
        SELECT  CUSTOMER_KEY,NIVEL_INFORMADO
        FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_SHABITS_UNIDATA_ALVI`
        WHERE MONTHID = CAST('${monthid}' AS INT)
    ) SHABITS
    ON SHABITS.CUSTOMER_KEY = REDEMPTIONS.CUSTOMER_KEY
    WHERE org_ip_id = '08'
    AND cycle_id IN ${cycle_id}
    GROUP BY REDEMPTIONS.CUSTOMER_KEY
    """,  # noqa: E501

    'allocation':
    # Retrives customer allocaction for given cycle
    """
    SELECT
        b.customer_key as CUSTOMER_ID,
        MAX(SHABITS.NIVEL_INFORMADO) AS NIVEL_INFORMADO
    FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_CUSTOMER_CYCLE_DH` a

    LEFT JOIN `cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID` b
    ON a.CUSTOMER_ID = b.pda_customer_key

    INNER JOIN (
        SELECT CUSTOMER_KEY,NIVEL_INFORMADO
        FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_SHABITS_UNIDATA_ALVI`
        WHERE MONTHID = CAST('${monthid}' AS INT)
    ) SHABITS
    ON SHABITS.CUSTOMER_KEY = b.customer_key

    WHERE CYCLE_ID IN ${cycle_id}
    GROUP BY b.CUSTOMER_KEY
    """  # noqa: E501
})

# -------------------------------------------------------------------------
# Functions
# -------------------------------------------------------------------------
def calculateRedemptionRate(
        cycles_df: pd.DataFrame,
        usuario: str,
        gbq_client: Client
) -> pd.DataFrame:
    """Compute Alvi Redemption Rate for loyal and non loyal customers.

    Parameters.
    ----------
    cycles_df: pd.DataFrame
        Dataframe with last 52 week cycles info

    Returns.
    --------
    redemption_rate: pd.DataFrame
        Dataframe containing basic ranking data and compueted columns
    """
    # Precompute unique cycles and initialize results DataFrame
    unique_cycles = cycles_df['CYCLE_NUMBER'].unique()
    redemption_rate = pd.DataFrame()

    for cycle in unique_cycles:
        # Set cycle variables
        cycle_df = cycles_df[cycles_df['CYCLE_NUMBER'] == cycle]
        start_date = cycle_df['START_DATE'].iloc[0]
        monthid = pendulum.date(start_date.year, start_date.month, start_date.day).subtract(months=1).format('YYYYMM')  # noqa: E501
        cycle_id = tuple(cycle_df['CYCLE_ID'].values)

        # Read Redemptions and Allocation data
        redemptions_df = readBigQuery(SQL_QUERIES['redemptions'].substitute(
            monthid=monthid,
            cycle_id=cycle_id
        ),
        user = usuario,
        gbq_client = gbq_client
        )

        allocation = readBigQuery(SQL_QUERIES['allocation'].substitute(
            cycle_id=cycle_id,
            monthid=monthid
        ),
        user = usuario,
        gbq_client = gbq_client
        )

        # Split redemptions in groups
        redemptions_groups = {
            'loyals': redemptions_df[redemptions_df['NIVEL_INFORMADO'].isin(['Socio VIP', 'Socio Oro'])],  # noqa: E501
            'non_loyals': redemptions_df[~redemptions_df['NIVEL_INFORMADO'].isin(['Socio VIP', 'Socio Oro'])]  # noqa: E501
        }

        # Calculating redemption rates by group and consolidating results
        aux_df = {'ciclo': cycle}
        for group, group_redemptions in redemptions_groups.items():
            allocation_group = allocation[allocation['NIVEL_INFORMADO'].isin(group_redemptions['NIVEL_INFORMADO'].unique())]  # noqa: E501
            aux_df[f'rr_{group}'] = round(
                len(group_redemptions['CUSTOMER_ID']) / len(allocation_group['CUSTOMER_ID']), 4
                ) if len(allocation_group['CUSTOMER_ID']) > 0 else 0.0

        # Calculate general redemption rate
        aux_df['rr_raw'] = round(
            len(redemptions_df['CUSTOMER_ID']) / len(allocation['CUSTOMER_ID']), 4
            ) if len(allocation['CUSTOMER_ID']) > 0 else 0.0
        # Concat
        redemption_rate = pd.concat([redemption_rate, pd.DataFrame([aux_df])], ignore_index=True)
    return redemption_rate

def productPropsAlvi(
        ranking: pd.DataFrame,
        usuario: str,
        gbq_client: Client
) -> pd.DataFrame:
    """Compute and add product related columns to Alvi offer tool.

    Parameters.
    ----------
    ranking: pd.DataFrame
        Dataframe containing basic ranking offer tool data

    Returns.
    --------
    ranking: pd.DataFrame
        Dataframe containing basic ranking data and compueted columns
    """
    # Retrive data from tables
    #----------------------------------------------------------------------

    # Load offer tool source
    # Load last 52 weeks cycles
    cycles_df = readBigQuery(SQL_QUERIES['cycles'].substitute(
    ),
    user = usuario,
    gbq_client = gbq_client
    )

    # Load suggested discount table
    suggested_discount = readBigQuery(SQL_QUERIES['offer_tool_discount'].substitute(
    ),
    user = usuario,
    gbq_client = gbq_client
    )

    # Load max suggested units
    max_suggested_units = readBigQuery(SQL_QUERIES['max_suggested_units'].substitute(
    ),
    user = usuario,
    gbq_client = gbq_client
    )

    # Load customers per cycle
    customers_per_cycle = {}
    for loyalty, value in {'loyals': 1, 'non_loyals': 0}.items():
        customers_per_cycle[loyalty] = readBigQuery(SQL_QUERIES['customers_per_cycle'].substitute(
            loyalty=value
        ),
        user = usuario,
        gbq_client = gbq_client
        )

    # Add calculated columns to ranking
    #----------------------------------------------------------------------

    # Format offer tool columns
    ranking = ranking.rename(columns={'categoria_marca':'cat_brand'})

    # Add suggested units
    ranking = ranking.merge(
    max_suggested_units,
    on=['cat_brand', 'supplier_nm'],
    how='left')

    del max_suggested_units

    # Calculate needed columns
    ranking['average_units'] = ranking['units'] / ranking['visits']
    ranking['precio_neto'] = ranking['total_sales'] / ranking['units']
    ranking[['average_units', 'precio_neto']] = ranking[
        ['average_units', 'precio_neto']
    ].astype('int32')

    # Calculate Redemption Rate
    redemption_rate = calculateRedemptionRate(cycles_df,usuario,gbq_client)  # noqa: E501

    redemption_rate =  {col: redemption_rate[col].mean()
                        for col in redemption_rate.columns[1:]}

    # Merge loyal and non loyals customers per cycle for each categoria_marca  # noqa: W505
    all_customers_per_cycle = customers_per_cycle['non_loyals'].merge(
        customers_per_cycle['loyals'],
        on=['cat_brand', 'supplier_nm'],
        how='outer',
        suffixes=('_non_loyals', '_loyals')
    )
    all_customers_per_cycle = all_customers_per_cycle.fillna(0)

    # Calculate customer redemption estimation per cycle
    all_customers_per_cycle['estimated_customers'] = (
    all_customers_per_cycle['customers_per_cycle_non_loyals'] * redemption_rate['rr_non_loyals'] +
    all_customers_per_cycle['customers_per_cycle_loyals'] * redemption_rate['rr_loyals']
    )

    # Add estimated customers to ranking
    ranking = ranking.merge(
        all_customers_per_cycle[['estimated_customers','cat_brand', 'supplier_nm']],
        on=['cat_brand', 'supplier_nm'],
        how='inner'
    )

    del customers_per_cycle
    del all_customers_per_cycle

    # Add suggested discount column to ranking
    ranking = ranking.merge(
        suggested_discount,
        on='cat_brand',
        how='left'
    )

    # Compute Sell Out
    ranking['sell_out'] = (
        ranking['precio_neto'] *
        ranking['discount'] *
        ranking['average_units'] *
        ranking['estimated_customers'] *
        4
    ).round()

    # Format columns
    ranking[['estimated_customers', 'max_suggested_units']] = ranking[
        ['estimated_customers', 'max_suggested_units']
    ].astype('int32')

    return ranking

# -------------------------------------------------------------------------
#                               Main process
# -------------------------------------------------------------------------
def main() -> None:
    usuario = 'offer_tool'
    logging.info('STARTING: Offer Tool Script...')

    # Main variables
    # --------------

    logging.info('Getting variables')
    # Parsed Variables

    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']
    store_banner: str = args['store_banner']

    logging.info(f'Execution date: {execution_date}')
    logging.info(f'store_banner: {store_banner}')

    gbq_client = Client()

    # Hardcoded Vars
    new_shabit_store_banners = ('Unimarc', 'Alvi')
    wholesalers = ('Alvi', 'Mayorista', 'Super 10')
    offer_tool_cols = ['total_customers', 'total_sales','units', 'visits', 'cat_brand', 'ranking',
                    'display', 'n_sales', 'loyalty_penetration']
    presentation = None

    # Redemption Rate vars (ALVI)
    all_cycle_redemptions = pd.DataFrame()  # noqa: F841
    redemption_rate = pd.DataFrame()  # noqa: F841

    upper_store_banner = {
            'Alvi': 'ALVI',
            'Unimarc': 'UNIMARC',
            'Super 10': 'S10',
            'Mayorista': 'M10'
    }[store_banner]

    logging.info(f'upper_store_banner: {upper_store_banner}')

    monthid = pendulum.parse(execution_date).start_of('month').format('YYYYMM')

    logging.info(f'monthid: {monthid}')

    # ---------------------------------------------------------------------
    #                       Stage 1: Create Offer Tool Tables
    # ---------------------------------------------------------------------
    logging.info('STARTING: Create Offer Tool Tables')
    # Create offer tool source (contains last 52 weeks transactions)
    # ---------------------------------------------------------------------

    _ = createTableAsSelect(
        query=(FULL_SALES_NEW_QUERIES if store_banner in new_shabit_store_banners
            else FULL_SALES_OLD_QUERIES)[store_banner].substitute(
                organization_id={
                    'Mayorista': 4,
                    'Super 10': 15
                }.get(store_banner, 0),
            store_banner=store_banner
        ),
        table_ref=f'cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_SOURCE_{upper_store_banner}',
        gbq_client=gbq_client,
        use_legacy_sql = False,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE'
    )

    logging.info('Table rebuild completed')

    # Create Offer Tool Ranking Table
    #----------------------------------------------------------------------
    logging.info(f'Rebuilding Offer Tool Ranking {store_banner} GCP table...')

    _ = createTableAsSelect(
        query=(SQL_QUERIES['create_offer_tool_ranking'] if store_banner !='Alvi'
        else SQL_QUERIES['create_offer_tool_ranking_alvi']).substitute(
            upper_store_banner = upper_store_banner
        ),
        table_ref=f'cl-bigdata-analytics-preprod.TMP.TMP_OFFER_TOOL_RANKING_{upper_store_banner}',
        gbq_client=gbq_client,
        use_legacy_sql = False,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE'
    )

    logging.info('Table rebuild completed')
    logging.info('COMPLETED: Create Offer Tool Tables')

    # ---------------------------------------------------------------------
    #                       Stage 2: Data Load
    # ---------------------------------------------------------------------
    logging.info('STARTING: Data load process')

    # Stage 2.1 Load data from tables
    #----------------------------------------------------------------------
    logging.info('STARTING: Retrive data from tables')

    # Load offer tool from table
    ranking = readBigQuery(
        query=SQL_QUERIES['ranking_offer_tool'].substitute(
            upper_store_banner = upper_store_banner
        ),
        user = usuario,
        gbq_client = gbq_client
    )
    if store_banner in wholesalers:
        presentation = readBigQuery(
        query=(SQL_QUERIES['presentation'] if store_banner != 'Alvi'
            else SQL_QUERIES['presentation_alvi']).substitute(
            upper_store_banner = upper_store_banner
        ),
        user = usuario,
        gbq_client = gbq_client
    )

    logging.info('Retrive data from tables complete')
    logging.info('COMPLETED: Data Load Process')

    ranking.columns = ranking.columns.str.lower()

    # ---------------------------------------------------------------------
    #                       Stage 2: Offer Tool Construction
    # ---------------------------------------------------------------------
    logging.info('STARTED: Offer Tool Construction')
    # Rename categoria marca column
    ranking =  ranking.rename(columns={'categoria_marca': 'cat_brand'})

    if store_banner == 'Alvi':
        ranking = productPropsAlvi(
            ranking=ranking,
            usuario=usuario,
            gbq_client = gbq_client
        )

    if presentation is not None and not presentation.empty:
        logging.info('Adding presentation columns')
        ranking = ranking.merge(
            presentation,
            on='cat_brand' if store_banner != 'Alvi' else ['cat_brand', 'supplier_nm'],
            how='left'
        )

    offer_tool_cols = offer_tool_cols[:6] + [  # noqa: RUF005
    'max_suggested_units',
    'average_units',
    'precio_neto',
    'estimated_customers',
    'discount',
    'sell_out',
    'is_ranked',
    'supplier_nm'] + offer_tool_cols[6:] if store_banner == 'Alvi' else offer_tool_cols

    ranking = ranking[offer_tool_cols[:-3]] if store_banner == 'Unimarc' else ranking[offer_tool_cols]  # noqa: E501

    logging.info('COMPLETED: Offer Tool Construction')

    ranking['monthid'] = monthid
    ranking.columns = ranking.columns.str.upper()

    if store_banner == 'Unimarc':
        uploadFrame(
            ranking,
            table_ddl_json_path=os.path.join('gbq_objects','ds_offer_tool_unimarc.json'),
            project=proyecto,
            gbq_client=gbq_client,
            if_exists='replace'
        )

    elif store_banner == 'Alvi':
        uploadFrame(
            ranking,
            table_ddl_json_path=os.path.join('gbq_objects','ds_offer_tool_alvi.json'),
            project=proyecto,
            gbq_client=gbq_client,
            if_exists='replace'
    )

    elif store_banner == 'Mayorista':
        uploadFrame(
            ranking,
            table_ddl_json_path=os.path.join('gbq_objects','ds_offer_tool_m10.json'),
            project=proyecto,
            gbq_client=gbq_client,
            if_exists='replace'
    )

    elif store_banner == 'Super 10':
        uploadFrame(
            ranking,
            table_ddl_json_path=os.path.join('gbq_objects','ds_offer_tool_s10.json'),
            project=proyecto,
            gbq_client=gbq_client,
            if_exists='replace'
    )


if __name__ == '__main__':
    main()
