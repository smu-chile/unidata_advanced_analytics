# Default
from __future__ import annotations

# Pip
import os
import re  # noqa: F401
import logging
import argparse
import posixpath  # noqa: F401
import unicodedata  # noqa: F401
from string import Template  # noqa: F401
from typing import Optional  # noqa: F401
from logging import config
from textwrap import dedent  # noqa: F401
from functools import partial  # noqa: F401
from itertools import islice  # noqa: F401

import numpy as np
import pandas as pd
from google.cloud import bigquery  # noqa: F401
from google.cloud.bigquery import Client

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    uploadFrame,
    readBigQuery,
    deleteFromTable,
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
    help='store_banner'
)
parser.add_argument(
    '--n_substitutes', type=int,
    help='number of substitutes to consider'
)

# -------------------------------------------------------------------------
# SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'raw_sales':
    """
    WITH raw_sales AS (
    -- ==========================================================
    -- 1. EXTRACCIÓN TRANSACCIONAL (Ventana 12 meses móviles)
    -- ==========================================================
    SELECT
        A.TXN_KEY AS TXN_KEY,
        A.MARKET_BASKET_KEY,
        A.ITM_TXN_FCN_TP_DSC,
        LTRIM(B.STORE_ID, '0') AS STORE_ID,
        DATE(A.ITM_TXN_TMS) AS TRANSACTION_DATE,
        TIME(A.ITM_TXN_TMS) AS TRANSACTION_TIME,
        D.SKU_PRODUCT AS SKU_PRODUCT,
        CAT_DSC, LIN_DESC, SEC_DSC, NEG_DSC,
        C.EAN AS EAN,

        ROUND(
            SUM(CASE
                WHEN (((D.NEG_ID = '14') OR (D.NEG_ID = '15')) AND (D.GRUPO_ID <> '210010103')) THEN 0
                WHEN (A.WGHT_ITM <> 0 AND A.WGHT_ITM IS NOT NULL) THEN A.ITM_TXN_AMT / (A.WGHT_ITM / 1000)
                WHEN (A.NBR_PD_ITM <> 0 AND C.CONT_CONV_UMB IS NOT NULL) THEN A.ITM_TXN_AMT / (CAST(C.CONT_CONV_UMB AS NUMERIC) * A.NBR_PD_ITM)
                WHEN (A.NBR_PD_ITM <> 0 AND C.CONT_CONV_UMB IS NULL) THEN A.ITM_TXN_AMT / A.NBR_PD_ITM
                ELSE 0
            END),
            2
        ) AS UNIT_PRICE,

        CASE
            WHEN (((D.NEG_ID = '14') OR (D.NEG_ID = '15')) AND (D.GRUPO_ID <> '210010103')) THEN 0
            WHEN (A.NBR_PD_ITM = 0 AND A.WGHT_ITM > 0 AND A.WGHT_ITM IS NOT NULL) THEN 1
            WHEN ((A.WGHT_ITM < 0) AND (A.WGHT_ITM IS NOT NULL)) THEN -1
            WHEN (A.NBR_PD_ITM <> 0 AND C.CONT_CONV_UMB IS NOT NULL) THEN CAST(C.CONT_CONV_UMB AS NUMERIC) * A.NBR_PD_ITM
            ELSE A.NBR_PD_ITM
        END AS QUANTITY,

        SUM(CASE
            WHEN (((D.NEG_ID = '14') OR (D.NEG_ID = '15')) AND (D.GRUPO_ID <> '210010103')) THEN 0
            ELSE A.ITM_TXN_AMT
        END) AS VALUE,

        SUM(CASE
            WHEN (A.WGHT_ITM IS NOT NULL) THEN A.WGHT_ITM / 1000
            ELSE 0
        END) AS WEIGHT,

        C.UNIDAD_DE_MEDIDA AS UNIDAD_DE_MEDIDA,
        A.CUSTOMER_KEY,

        CASE
            WHEN (E.FNC_DOC_TP_DSC = 'NE') THEN 'TN'
            WHEN ((E.FNC_DOC_TP_DSC = 'FE') OR (E.FNC_DOC_TP_DSC = 'FX')) THEN 'TF'
            ELSE E.FNC_DOC_TP_DSC
        END AS TRANSACTION_TYPE,

        SUM(A.DCN_AMT) AS DISCOUNT_VALUE,

        CASE
            WHEN (((D.NEG_ID = '14') OR (D.NEG_ID = '15')) AND (D.GRUPO_ID <> '210010103')) THEN 0
            WHEN (A.NBR_PD_ITM = 0 AND A.WGHT_ITM > 0 AND A.WGHT_ITM IS NOT NULL) THEN 1
            WHEN ((A.WGHT_ITM < 0) AND (A.WGHT_ITM IS NOT NULL)) THEN -1
            WHEN (A.NBR_PD_ITM <> 0 AND C.CONT_CONV_UMB IS NOT NULL) THEN CAST(C.CONT_CONV_UMB AS NUMERIC) * A.NBR_PD_ITM / (COALESCE(C.UMREZ, 1) / COALESCE(C.UMREN, 1))
            ELSE A.NBR_PD_ITM
        END AS QUANTITY_SU,

        SUM(TAX_AMOUNT) AS TAX_AMOUNT

    FROM `${gcp_project_cda}.DS_CDA_VW_SMU.DW_VW_FACT_ITM_TXN` A

    JOIN `${gcp_project_cda}.DS_CDA_VW_SMU.DW_VW_DIM_STORE_HIERARCHY` B
    ON (
        (A.STORE_KEY = B.STORE_KEY)
        AND (B.ORG_IP_ID IN ('01', '04', '02', '08', '06', '09'))
    )

    JOIN (
        SELECT
            PRODUCT_KEY,
            EAN,
            CONT_CONV_UMB,
            UNIDAD_DE_MEDIDA,
            CAST(CONT_CONV_UMB AS NUMERIC) AS UMREZ,
            CAST(DENOM_UMB AS NUMERIC) AS UMREN
        FROM `${gcp_project_cda}.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT`
    ) C
        ON A.PRODUCT_KEY_1 = C.PRODUCT_KEY

    JOIN (
        SELECT
            PRODUCT_KEY,
            SKU_PRODUCT,
            NEG_ID,
            GRUPO_ID, CAT_DSC, LIN_DESC, SEC_DSC, NEG_DSC
        FROM `${gcp_project_cda}.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT_HIERARCHY`
    ) D
        ON A.PRODUCT_KEY_1 = D.PRODUCT_KEY

    JOIN `${gcp_project_cda}.DS_CDA_VW_SMU.DW_VW_DIM_FIN_DOC_TP_TYPE` E
        ON A.FNC_DOC_TP_KEY = E.FIN_DOC_TP_KEY

    WHERE
        A.ITM_TXN_TMS >= DATE_TRUNC(DATE_SUB('${execution_date}', INTERVAL 12 MONTH), MONTH)
        AND A.ITM_TXN_TMS < DATE_TRUNC('${execution_date}', MONTH)
        AND A.MARKET_BASKET_KEY NOT IN (
            SELECT DISTINCT MARKET_BASKET_KEY
            FROM `${gcp_project_cda}.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_E_COMMERCE`
            WHERE CANAL_VENTA IN ('PEDIDOS YA', 'UBER EATS', 'RAPPI', 'RAPPI TURBO')
        )
        AND B.ORG_IP = '${store_banner}'
        AND A.CUSTOMER_HEX NOT IN ('3588d47a76aac91fcf2a3c2f55f6a351')
    GROUP BY
        D.NEG_ID, D.GRUPO_ID, A.NBR_PD_ITM, C.CONT_CONV_UMB, A.WGHT_ITM, A.ITM_TXN_TMS, A.TXN_KEY,
        B.STORE_ID, D.SKU_PRODUCT, CAT_DSC, LIN_DESC, SEC_DSC, NEG_DSC, C.EAN, A.MARKET_BASKET_KEY,
        A.ITM_TXN_FCN_TP_DSC, C.UNIDAD_DE_MEDIDA, C.UMREZ, C.UMREN, A.CUSTOMER_KEY, E.FNC_DOC_TP_DSC
    )
    """,  # noqa: E501

    'infaltables_penetracion':
    """
    WITH clientes_sensibles AS (
        SELECT DISTINCT
            CUSTOMER_KEY
        FROM `${gcp_project}.CONOCIMIENTO_CLIENTE.CUSTOMER_SEGMENTATION_SOPHISTICATION`
        WHERE STORE_BANNER = '${store_banner}'
        AND CLASIFICACION_CLIENTE = 'PRICE SENSITIVE'
        AND DATE = DATE_TRUNC('${execution_date}', MONTH)
    ),

    universo_totales AS (
        SELECT
            COUNT(DISTINCT MARKET_BASKET_KEY) AS TOTAL_CANASTAS,
            SUM(VALUE) AS TOTAL_VENTA,
            SUM(VALUE)-SUM(TAX_AMOUNT) AS TOTAL_VENTA_NETA,
            COUNT(DISTINCT CASE WHEN CUSTOMER_KEY IS NOT NULL THEN CUSTOMER_KEY END) AS TOTAL_CLIENTES
        FROM `${gcp_project}.TMP.TMP_INFALTABLES_RAW_SALES_${upper_store_banner}`
    ),

    penetracion_producto AS (
        SELECT
            r.SKU_PRODUCT, CAT_DSC, LIN_DESC, SEC_DSC, NEG_DSC,
            COUNT(DISTINCT r.MARKET_BASKET_KEY) AS CANASTAS_PRODUCTO,
            COUNT(DISTINCT CASE WHEN r.CUSTOMER_KEY IS NOT NULL THEN r.CUSTOMER_KEY END) AS CLIENTES_PRODUCTO,
            SUM(VALUE) AS VENTA_PRODUCTO,
            SUM(VALUE)-SUM(TAX_AMOUNT) as VENTA_NETA_PRODUCTO,
            SUM(VALUE)/sum(QUANTITY) AS PVP,

            -- Métricas de Novedad / Antigüedad
            MIN(r.TRANSACTION_DATE) AS FECHA_PRIMERA_VENTA,
            COUNT(DISTINCT DATE_TRUNC(r.TRANSACTION_DATE, MONTH)) AS MESES_CON_VENTA,
            DATE_DIFF('${execution_date}', MIN(r.TRANSACTION_DATE), DAY) AS DIAS_DESDE_PRIMERA_VENTA
        FROM `${gcp_project}.TMP.TMP_INFALTABLES_RAW_SALES_${upper_store_banner}` r
        WHERE r.QUANTITY > 0
        GROUP BY r.SKU_PRODUCT, CAT_DSC, LIN_DESC, SEC_DSC, NEG_DSC
    ),

    penetracion_producto_clientes_sensibles AS (
        SELECT
            r.SKU_PRODUCT, CAT_DSC, LIN_DESC, SEC_DSC, NEG_DSC,
            COUNT(DISTINCT r.MARKET_BASKET_KEY) AS CANASTAS_PRODUCTO_CS,
            COUNT(DISTINCT CASE WHEN r.CUSTOMER_KEY IS NOT NULL THEN r.CUSTOMER_KEY END) AS CLIENTES_PRODUCTO_CS,
            SUM(VALUE) AS VENTA_PRODUCTO_CS
        FROM `${gcp_project}.TMP.TMP_INFALTABLES_RAW_SALES_${upper_store_banner}` r
        JOIN clientes_sensibles s ON s.customer_key = r.customer_key
        WHERE r.QUANTITY > 0
        GROUP BY r.SKU_PRODUCT, CAT_DSC, LIN_DESC, SEC_DSC, NEG_DSC
    ),

    penetracion_producto_combinado AS (
        SELECT
            r.SKU_PRODUCT, r.CAT_DSC, r.LIN_DESC, r.SEC_DSC, r.NEG_DSC,
            r.CANASTAS_PRODUCTO, r.CLIENTES_PRODUCTO, r.VENTA_PRODUCTO,r.VENTA_NETA_PRODUCTO,r.PVP,
            s.CANASTAS_PRODUCTO_CS, s.CLIENTES_PRODUCTO_CS, s.VENTA_PRODUCTO_CS,
            r.FECHA_PRIMERA_VENTA,
            r.MESES_CON_VENTA,
            r.DIAS_DESDE_PRIMERA_VENTA
        FROM penetracion_producto r
        JOIN penetracion_producto_clientes_sensibles s ON s.SKU_PRODUCT = r.SKU_PRODUCT
    ),

    importancia_categoria AS (
        SELECT
            CAT_DSC,
            SUM(VALUE) AS TOTAL_VENTA_CATEGORIA,
            SUM(CASE WHEN S.CUSTOMER_KEY IS NOT NULL THEN VALUE END) AS TOTAL_VENTA_CATEGORIA_CS
        FROM `${gcp_project}.TMP.TMP_INFALTABLES_RAW_SALES_${upper_store_banner}` r
        LEFT JOIN clientes_sensibles s ON s.customer_key = r.customer_key
        WHERE r.QUANTITY > 0
        GROUP BY CAT_DSC
    ),

    recompra_cliente_mensual AS (
        SELECT
            CUSTOMER_KEY, SKU_PRODUCT,
            DATE_TRUNC(TRANSACTION_DATE, MONTH) AS MES,
            COUNT(DISTINCT MARKET_BASKET_KEY) AS COMPRAS_EN_MES
        FROM `${gcp_project}.TMP.TMP_INFALTABLES_RAW_SALES_${upper_store_banner}`
        WHERE QUANTITY > 0 AND CUSTOMER_KEY IS NOT NULL
        GROUP BY CUSTOMER_KEY, SKU_PRODUCT, MES
    ),

    resumen_recompra_mensual AS (
        SELECT
            SKU_PRODUCT,
            ROUND(AVG(COMPRAS_EN_MES), 2) AS FREC_RECOMPRA_MENSUAL_PROM,
            MAX(COMPRAS_EN_MES) AS MAX_RECOMPRA_MES
        FROM recompra_cliente_mensual
        GROUP BY SKU_PRODUCT
    ),

    recompra_cliente_trimestral AS (
        SELECT
            CUSTOMER_KEY, SKU_PRODUCT,
            DATE_TRUNC(TRANSACTION_DATE, QUARTER) AS TRIMESTRE,
            COUNT(DISTINCT MARKET_BASKET_KEY) AS COMPRAS_EN_TRIMESTRE
        FROM `${gcp_project}.TMP.TMP_INFALTABLES_RAW_SALES_${upper_store_banner}`
        WHERE QUANTITY > 0 AND CUSTOMER_KEY IS NOT NULL
        GROUP BY CUSTOMER_KEY, SKU_PRODUCT, TRIMESTRE
    ),

    resumen_recompra_trimestral AS (
        SELECT
            r.SKU_PRODUCT,
            COUNT(DISTINCT r.CUSTOMER_KEY) AS cant_clientes,
            -- Suavizado Bayesiano (K=15, M=1.22)
            ROUND(
                SAFE_DIVIDE(
                    (COUNT(DISTINCT r.CUSTOMER_KEY) * AVG(r.COMPRAS_EN_TRIMESTRE)) + (15 * 1.22),
                    (COUNT(DISTINCT r.CUSTOMER_KEY) + 15)
                ), 2
            ) AS FREC_RECOMPRA_TRIMESTRAL_PROM,
            MAX(r.COMPRAS_EN_TRIMESTRE) AS MAX_RECOMPRA_TRIMESTRE
        FROM recompra_cliente_trimestral r
        GROUP BY r.SKU_PRODUCT
    ),

    category_frequency_prep AS (
        SELECT DISTINCT
            p.CAT_DSC,
            PERCENTILE_CONT(rm.FREC_RECOMPRA_MENSUAL_PROM, 0.5) OVER(PARTITION BY p.CAT_DSC) AS cat_frec_mensual_mediana,
            PERCENTILE_CONT(rt.FREC_RECOMPRA_TRIMESTRAL_PROM, 0.5) OVER(PARTITION BY p.CAT_DSC) AS cat_frec_trimestral_mediana
        FROM penetracion_producto p
        LEFT JOIN resumen_recompra_mensual rm ON p.SKU_PRODUCT = rm.SKU_PRODUCT
        LEFT JOIN resumen_recompra_trimestral rt ON p.SKU_PRODUCT = rt.SKU_PRODUCT
    )

    -- ==========================================================
    -- 2. RESULTADO FINAL CONSOLIDADO
    -- ==========================================================
    SELECT
        p.NEG_DSC, p.SEC_DSC, p.LIN_DESC, p.CAT_DSC,
        CAST(p.SKU_PRODUCT AS INT64) AS PRODUCT_ID,

        -- Indicadores de Novedad y Ciclo de Vida
        p.FECHA_PRIMERA_VENTA,
        p.MESES_CON_VENTA,
        p.DIAS_DESDE_PRIMERA_VENTA,
        CASE
            WHEN p.DIAS_DESDE_PRIMERA_VENTA <= 90 THEN 1
            ELSE 0
        END AS ES_PRODUCTO_NUEVO_90D,
        CASE
            WHEN p.MESES_CON_VENTA >= 12 THEN 'TODO_EL_ANIO_12M'
            WHEN p.DIAS_DESDE_PRIMERA_VENTA <= 90 THEN 'LANZAMIENTO_RECIENTE_LE_3M'
            WHEN p.DIAS_DESDE_PRIMERA_VENTA <= 180 THEN 'MEDIO_ANIO_LE_6M'
            ELSE 'INTERMITENTE_O_PARCIAL'
        END AS SEGMENTO_MADUREZ_SKU,

        -- Penetración en Canastas
        p.CANASTAS_PRODUCTO AS CANASTAS_CON_SKU,
        p.CANASTAS_PRODUCTO_CS AS CANASTAS_CON_SKU_CS,
        u.TOTAL_CANASTAS,
        ROUND(p.CANASTAS_PRODUCTO * 100.0 / NULLIF(u.TOTAL_CANASTAS, 0), 4) AS PENETRACION_CANASTAS_PCT,
        ROUND(p.CANASTAS_PRODUCTO_CS * 100.0 / NULLIF(u.TOTAL_CANASTAS, 0), 4) AS PENETRACION_CANASTAS_PCT_CS,

        -- Penetración en Clientes
        p.CLIENTES_PRODUCTO AS CLIENTES_CON_SKU,
        p.CLIENTES_PRODUCTO_CS AS CLIENTES_CON_SKU_CS,
        u.TOTAL_CLIENTES,
        ROUND(p.CLIENTES_PRODUCTO * 100.0 / NULLIF(u.TOTAL_CLIENTES, 0), 4) AS PENETRACION_CLIENTES_PCT,
        ROUND(p.CLIENTES_PRODUCTO_CS * 100.0 / NULLIF(u.TOTAL_CLIENTES, 0), 4) AS PENETRACION_CLIENTES_PCT_CS,

        -- Venta e Importancia
        im.TOTAL_VENTA_CATEGORIA,
        im.TOTAL_VENTA_CATEGORIA_CS,
        u.TOTAL_VENTA,
        u.TOTAL_VENTA_NETA,
        p.VENTA_PRODUCTO,p.VENTA_NETA_PRODUCTO,p.PVP,
        p.VENTA_PRODUCTO_CS,
        ROUND(im.TOTAL_VENTA_CATEGORIA * 100.0 / NULLIF(u.TOTAL_VENTA, 0), 4) AS IMPORTANCIA_CATEGORIA_PCT,
        ROUND(im.TOTAL_VENTA_CATEGORIA_CS * 100.0 / NULLIF(u.TOTAL_VENTA, 0), 4) AS IMPORTANCIA_CATEGORIA_PCT_CS,

        -- Recompra SKU (Winsorizadas)
        LEAST(COALESCE(rm.FREC_RECOMPRA_MENSUAL_PROM, 1.0), 3.0) AS FREC_RECOMPRA_MENSUAL_PROM,
        COALESCE(rm.MAX_RECOMPRA_MES, 0) AS MAX_RECOMPRA_UN_CLIENTE_MES,
        LEAST(COALESCE(rt.FREC_RECOMPRA_TRIMESTRAL_PROM, 1.22), 5.0) AS FREC_RECOMPRA_TRIMESTRAL_PROM,
        COALESCE(rt.MAX_RECOMPRA_TRIMESTRE, 0) AS MAX_RECOMPRA_UN_CLIENTE_TRIMESTRE,

        -- Recompra Categoría (Medianas)
        COALESCE(cf.cat_frec_mensual_mediana, 1.15) AS cat_frec_mensual_mediana,
        COALESCE(cf.cat_frec_trimestral_mediana, 1.22) AS cat_frec_trimestral_mediana

    FROM penetracion_producto_combinado p
    CROSS JOIN universo_totales u
    LEFT JOIN resumen_recompra_mensual rm ON p.SKU_PRODUCT = rm.SKU_PRODUCT
    LEFT JOIN resumen_recompra_trimestral rt ON p.SKU_PRODUCT = rt.SKU_PRODUCT
    LEFT JOIN category_frequency_prep cf ON p.CAT_DSC = cf.CAT_DSC
    LEFT JOIN importancia_categoria im ON p.CAT_DSC = im.CAT_DSC
    """,  # noqa: E501

    'data_infaltables':
    """
    WITH

    -- 1. Identificación de SKUs de Marca Propia (Para exclusión)
    marca_propia_skus AS (
    SELECT DISTINCT
        CAST(sku_product AS INT64) AS sku_marca_propia
    FROM `${gcp_project_cda}.DS_CDA_VW_SMU.DW_VW_DIM_SKU_ATTR`
    WHERE TIPO_MARCA IN ('1', '3')
    ),

    -- 2. Dimensión de productos limpia (Relación EAN -> PRODUCT_ID)
    distinct_products AS (
    SELECT DISTINCT
        P.EAN,
        P.CAT_DSC AS CATEGORY_DESCRIPTION,
        P.GRUPO_DSC AS SUB_CATEGORY_DESCRIPTION,
        P.LIN_DESC, P.SEC_DSC,
        P.NM AS PRODUCT_DESCRIPTION,
        CAST(P.SKU_PRODUCT AS INT64) AS PRODUCT_ID,
        P.NEG_DSC,
        CASE WHEN MP.sku_marca_propia IS NULL THEN 0 ELSE 1 END AS MPE
    FROM `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT` P
    LEFT JOIN marca_propia_skus MP
        ON CAST(P.SKU_PRODUCT AS INT64) = MP.sku_marca_propia
    WHERE P.SKU_PRODUCT IS NOT NULL
        AND P.SKU_PRODUCT != 'None'
    ),

    -- 2.1. Aseguramos UNICIDAD a nivel de PRODUCT_ID para los datos descriptivos del producto
    distinct_products_unique AS (
    SELECT
        PRODUCT_ID,
        ANY_VALUE(EAN) AS EAN, -- Trae un EAN representativo
        ANY_VALUE(CATEGORY_DESCRIPTION) AS CATEGORY_DESCRIPTION,
        ANY_VALUE(SUB_CATEGORY_DESCRIPTION) AS SUB_CATEGORY_DESCRIPTION,
        ANY_VALUE(LIN_DESC) AS LIN_DESC,
        ANY_VALUE(SEC_DSC) AS SEC_DSC,
        ANY_VALUE(PRODUCT_DESCRIPTION) AS PRODUCT_DESCRIPTION,
        ANY_VALUE(NEG_DSC) AS NEG_DSC,
        MAX(MPE) AS MPE -- Si al menos uno registra como Marca Propia, queda en 1
    FROM distinct_products
    GROUP BY PRODUCT_ID
    ),

    prom_por_sku AS (
    SELECT
        DP.sub_category_description AS subcategoria,
        DP.CATEGORY_DESCRIPTION as categoria,
        sku,
        AVG(substitution_score) AS score_promedio,
        SUM(CASE
            WHEN substitution_rank = 1 THEN substitution_score * 0.60
            WHEN substitution_rank = 2 THEN substitution_score * 0.30
            WHEN substitution_rank = 3 THEN substitution_score * 0.10
        ELSE 0
        END) AS score_facilidad_sustitucion_directa,
    FROM `${gcp_project}.ML_LAB.SKU_SUBSTITUTES_BY_CATEGORY` S

    LEFT JOIN distinct_products_unique DP
    ON DP.PRODUCT_ID = S.sku

    WHERE S.store_banner = '${store_banner}'
    AND S.date = DATE_TRUNC('${execution_date}', MONTH)
    and substitution_rank <= ${n_substitutes}
    GROUP BY DP.CATEGORY_DESCRIPTION, DP.sub_category_description, sku
    ),

    con_prom_subcat AS (
    SELECT
        subcategoria,
        categoria,
        sku,
        score_promedio,
        score_facilidad_sustitucion_directa,
        AVG(score_promedio) OVER (PARTITION BY subcategoria) AS score_promedio_subcategoria,
        AVG(score_promedio) OVER (PARTITION BY categoria) AS score_promedio_categoria
    FROM prom_por_sku
    ),

    sku_difficulty as (
    SELECT
        subcategoria,
        categoria,
        sku,
        score_facilidad_sustitucion_directa,
        score_promedio,
        score_promedio_subcategoria,score_promedio_categoria,
        CASE WHEN COUNT(*) OVER (PARTITION BY subcategoria) >= 5
        THEN score_promedio / NULLIF(score_promedio_subcategoria, 0)
        ELSE score_promedio / NULLIF(score_promedio_categoria, 0)
    END AS ponderador
    FROM con_prom_subcat
    ORDER BY subcategoria, sku
    ),

    subcat_difficulty as (
    select
        subcategoria,
        avg(score_promedio_subcategoria) as score_promedio_subcategoria
    from sku_difficulty
    group by 1
    ),

    cat_difficulty as (
    select
        categoria,
        avg(score_promedio_categoria) as score_promedio_categoria
    from sku_difficulty
    group by 1
    ),

    penetracion AS (
    SELECT *
    FROM `${gcp_project}.TMP.TMP_INFALTABLES_PENETRACION_SP_${upper_store_banner}`
    ),

    -- 6. Importancia Nielsen ÚNICA por PRODUCT_ID
    importancia_nielsen AS (
    SELECT
        p.PRODUCT_ID,
        MAX(indice_importancia_nielsen) AS indice_importancia_nielsen -- Evita duplicar si hay más de un EAN por SKU
    FROM `${gcp_project}.CDA_VISTAS.VW_INFALTABLES_IMPORTANCIA_NIELSEN` n

    LEFT JOIN distinct_products p
    ON p.EAN = n.EAN

    WHERE p.PRODUCT_ID IS NOT NULL
    GROUP BY p.PRODUCT_ID
    )

    -- 7. Selección final explícita (Evita columnas duplicadas en la visualización)
    SELECT
        p.PRODUCT_ID,
        p.CANASTAS_CON_SKU,
        p.CANASTAS_CON_SKU_CS,
        p.TOTAL_CANASTAS,
        p.PENETRACION_CANASTAS_PCT,
        p.PENETRACION_CANASTAS_PCT_CS,
        p.CLIENTES_CON_SKU,
        p.CLIENTES_CON_SKU_CS,
        p.TOTAL_CLIENTES,
        p.PENETRACION_CLIENTES_PCT,
        p.PENETRACION_CLIENTES_PCT_CS,
        p.TOTAL_VENTA_CATEGORIA,
        p.TOTAL_VENTA_CATEGORIA_CS,
        p.TOTAL_VENTA,p.TOTAL_VENTA_NETA,
        p.VENTA_PRODUCTO,p.VENTA_NETA_PRODUCTO,p.PVP,
        p.VENTA_PRODUCTO_CS,
        p.IMPORTANCIA_CATEGORIA_PCT,
        p.IMPORTANCIA_CATEGORIA_PCT_CS,
        p.FREC_RECOMPRA_MENSUAL_PROM,
        p.FREC_RECOMPRA_TRIMESTRAL_PROM,
        p.cat_frec_mensual_mediana,
        p.cat_frec_trimestral_mediana,

        -- Indicadores de Novedad y Ciclo de Vida
        p.FECHA_PRIMERA_VENTA,
        p.MESES_CON_VENTA,
        p.DIAS_DESDE_PRIMERA_VENTA,
        p.ES_PRODUCTO_NUEVO_90D,
        p.SEGMENTO_MADUREZ_SKU,

        -- Información de la dificultad por SKU
        s.score_promedio as score_promedio_sustitucion,
        s.score_facilidad_sustitucion_directa,
        s.ponderador as sku_ponderador_sustitucion,
        -- Información agregada de la SUB Categoría
        sc.score_promedio_subcategoria as  subcat_ponderador_sustitucion,
        -- Nielsen
        i.indice_importancia_nielsen,

        -- Atributos de Producto limpios
        pr.EAN,
        pr.CATEGORY_DESCRIPTION,
        pr.SUB_CATEGORY_DESCRIPTION,
        pr.LIN_DESC,
        pr.SEC_DSC,
        pr.PRODUCT_DESCRIPTION,
        pr.NEG_DSC,
        pr.MPE AS marca_propia_flag

    FROM penetracion p

    LEFT JOIN sku_difficulty s
    ON s.sku = p.PRODUCT_ID

    LEFT JOIN importancia_nielsen i
    ON i.PRODUCT_ID = p.PRODUCT_ID

    LEFT JOIN distinct_products_unique pr
    ON pr.PRODUCT_ID = p.PRODUCT_ID

    left join subcat_difficulty sc
    on sc.subcategoria = pr.SUB_CATEGORY_DESCRIPTION
    """  # noqa: E501
})


# -------------------------------------------------------------------------
# Functions and Classes
# -------------------------------------------------------------------------

"""ÍNDICE DE INFALTABLES UNIMARC - v9
===================================

Metricas
---------------------------------

  1. Penetración (canastas, general)       — PENETRACION_CANASTAS_PCT
  2. Penetración Sensibles (canastas, CS)  — PENETRACION_CANASTAS_PCT_CS
  3. Peso en Categoría (venta general)     — combinar_peso_categoria(geom, 0.6/0.4)
  4. Nielsen                               — indice_importancia_nielsen
  5. Ponderador de Sustitución             — score_facilidad_sustitucion_directa
                                              (cascada + ratio + rank separado, sin cambios de v8)

Pesos
-----------------------------------------------

  1. Penetración (canastas, general):        40%
  2. Penetración Sensibles (canastas, CS):   25%
  3. Peso en Categoría:                      20%
  4. Nielsen:                                10%
  5. Ponderador de Sustitución:              5%

Autor: Catalina (SMU Data & Analytics)
Versión: 9
Fecha: Agosto 2026
"""  # noqa: W505

def _kneedle(valores: np.ndarray):
    """Punto de máxima curvatura de una curva decreciente: mayor distancia
    vertical entre la curva normalizada y la recta que une su primer y
    último punto.

    Distinto de un detector de "aplanamiento total" (que busca dónde la
    curva deja de caer para siempre): kneedle encuentra un quiebre
    intermedio dentro de un tramo, necesario para subdividir el índice en
    más de dos grupos sin recurrir a fracciones fijas (tercios, mitades).
    """
    y = np.asarray(valores, dtype=float)
    if len(y) < 20:
        return None, None
    x = np.linspace(0, 1, len(y))
    y_norm = (y - y.min()) / max(y.max() - y.min(), 1e-9)
    dist = (1 - x) - y_norm
    pos = int(np.argmax(dist))
    return pos, float(y[pos])


def clasificar_3_tramos(indice: pd.Series) -> pd.Series:
    """Clasificación de 3 tramos (Crítico / Importante / Complementario),
    con los dos cortes calculados por curvatura (kneedle), no por
    fracciones fijas del resto.

    kneedle se aplica dos veces: sobre la curva completa (corte Crítico), y
    sobre lo que queda por debajo de ese corte (corte Importante), cada
    corte sale de la forma real de su propio tramo.
    """
    indice_valido = indice.dropna()

    if len(indice_valido) < 40:
        def clasificar_fallback(v):
            if pd.isna(v):
                return 'Sin Datos'
            elif v >= 80:  # noqa: RET505
                return 'Crítico'
            elif v >= 40:
                return 'Importante'
            else:
                return 'Complementario'
        return indice.apply(clasificar_fallback)

    valores = indice_valido.sort_values(ascending=False).values  # noqa: PD011

    _, corte_critico = _kneedle(valores)
    resto = valores[valores < corte_critico]

    if len(resto) >= 20:
        _, corte_importante = _kneedle(resto)
    else:
        corte_importante = resto.min() if len(resto) else corte_critico

    n_critico = int((valores >= corte_critico).sum())
    n_importante = int(((valores < corte_critico) & (valores >= corte_importante)).sum())
    n_complementario = int((valores < corte_importante).sum())

    print('  ✓ Cortes por curvatura (kneedle), no fracciones fijas:')
    print(f'    - Crítico:        índice >= {corte_critico:.2f}  ({n_critico:,} SKUs, '
          f'{100*n_critico/len(valores):.1f}%)')
    print(f'    - Importante:     índice >= {corte_importante:.2f}  ({n_importante:,} SKUs, '
          f'{100*n_importante/len(valores):.1f}%)')
    print(f'    - Complementario: índice <  {corte_importante:.2f}  ({n_complementario:,} SKUs, '
          f'{100*n_complementario/len(valores):.1f}%)')

    def clasificar(v):
        if pd.isna(v):
            return 'Sin Datos'
        elif v >= corte_critico:  # noqa: RET505
            return 'Crítico'
        elif v >= corte_importante:
            return 'Importante'
        else:
            return 'Complementario'

    return indice.apply(clasificar)


def combinar_peso_categoria(
    df: pd.DataFrame,
    modo: str = 'geom',
    w_a: float = 0.6,
    w_b: float = 0.4,
    col_venta_producto: str = 'VENTA_PRODUCTO',
    col_venta_categoria: str = 'TOTAL_VENTA_CATEGORIA',
    col_venta_total: str = 'TOTAL_VENTA',
    out_col: str = 'peso_ventas_en_categoria'
) -> pd.DataFrame:
    """Calcula importancia del SKU en su categoría, sobre venta GENERAL.

    Combina dos factores:
      a = venta_producto / venta_categoria  (peso del SKU en su categoría)
      b = venta_categoria / venta_total     (peso de la categoría en el total)

    Fórmula final (modo geométrico, default v9: w_a=0.6, w_b=0.4):
      importancia = a^w_a * b^w_b

    Parámetros
    ----------
    df : pd.DataFrame
      Debe contener las tres columnas de venta indicadas.
    modo : str
      'geom' (default, a^w_a * b^w_b) | 'harm' (media armónica) | 'arith' (media aritmética)
    w_a, w_b : float
      Pesos de cada factor. Default v9: 0.6 / 0.4 (antes 0.7 / 0.3 en v8).
    col_venta_producto, col_venta_categoria, col_venta_total : str
      Nombres de columna configurables
    out_col : str
      Nombre de columna output.

    Retorno
    -------
    pd.DataFrame
      Copia con columnas:
      - 'peso_producto_en_cat'  (a, en [0,1])
      - 'peso_categoria_en_total' (b, en [0,1])
      - out_col                 (combinación, en [0,1])
    """  # noqa: W505
    req = [col_venta_producto, col_venta_categoria, col_venta_total]
    faltan = [c for c in req if c not in df.columns]
    if faltan:
        msg = f'Faltan columnas requeridas: {faltan}'
        raise KeyError(msg)

    X = df.copy()  # noqa: N806

    a = (
        pd.to_numeric(X[col_venta_producto], errors='coerce') /
        pd.to_numeric(X[col_venta_categoria], errors='coerce').replace(0, np.nan)
    )
    a = a.clip(0, 1).fillna(0.0)
    X['peso_producto_en_cat'] = a

    b = (
        pd.to_numeric(X[col_venta_categoria], errors='coerce') /
        pd.to_numeric(X[col_venta_total], errors='coerce').replace(0, np.nan)
    )
    b = b.clip(0, 1).fillna(0.0)
    X['peso_categoria_en_total'] = b

    w_a = float(w_a)
    w_b = float(w_b)
    if w_a < 0 or w_b < 0:
        msg_0 = 'w_a y w_b deben ser >= 0.'
        raise ValueError(msg_0)
    if w_a + w_b == 0:
        msg_1 = 'w_a + w_b no puede ser 0.'
        raise ValueError(msg_1)

    if modo == 'geom':
        y = (a ** w_a) * (b ** w_b)
    elif modo == 'harm':
        y = pd.Series(np.nan, index=X.index)
        mask = (a > 0) & (b > 0)
        y[mask] = 1.0 / (w_a / a[mask] + w_b / b[mask])
        y = y.fillna(0.0)
    elif modo == 'arith':
        y = w_a * a + w_b * b
    else:
        msg_2 = "modo debe ser 'geom', 'harm' o 'arith'."
        raise ValueError(msg_2)

    X[out_col] = y.clip(0, 1)
    return X


class IndiceInfaltablesCalculator_v9:  # noqa: N801
    """Calcula Índice de Infaltables v9 — modelo simplificado de 5 métricas.

    Entrada esperada (DataFrame):
    - PRODUCT_ID, PRODUCT_DESCRIPTION, CATEGORY_DESCRIPTION, etc.
    - PENETRACION_CANASTAS_PCT: penetración general (canastas)
    - PENETRACION_CANASTAS_PCT_CS: penetración clientes sensibles a precio
    - VENTA_PRODUCTO, TOTAL_VENTA_CATEGORIA, TOTAL_VENTA: para peso en categoría
    - indice_importancia_nielsen: score Nielsen
    - score_facilidad_sustitucion_directa: distancia a sustitutos (CDT)
    - PENETRACION_CLIENTES_PCT: usado solo por la compuerta de demanda
    - FREC_RECOMPRA_TRIMESTRAL_PROM: se calcula y expone, sin peso en v9

    Opcionales, no entran a ningún cálculo: VENTA_NETA_PRODUCTO, TOTAL_VENTA_NETA,
    PVP, FECHA_PRIMERA_VENTA, MESES_CON_VENTA, DIAS_DESDE_PRIMERA_VENTA,
    ES_PRODUCTO_NUEVO_90D y SEGMENTO_MADUREZ_SKU pasan derecho al resultado como
    contexto de lectura del ranking.

    Peso en Categoría se calcula con venta BRUTA (VENTA_PRODUCTO). El Excel de
    rankings muestra venta NETA. Son dos conceptos distintos conviviendo a
    propósito: pasar la métrica a neta exige TOTAL_VENTA_CATEGORIA neta.
    """  # noqa: W505

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.n_skus = len(df)
        self.resultados = None
        print(f'✓ Calculadora v9 inicializada: {self.n_skus:,} SKUs')

    # =====================================================================
    # CÁLCULO DE MÉTRICAS RAW
    # =====================================================================

    def calcular_penetracion_canastas(self) -> pd.Series:
        """MÉTRICA 1: Penetración (canastas, general) — 40%"""
        p_canastas = self.df['PENETRACION_CANASTAS_PCT'].copy()
        n_con_datos = p_canastas.notna().sum()
        pct = 100 * n_con_datos / self.n_skus
        print(f'  ✓ Penetración (canastas, general): {n_con_datos:,} SKUs ({pct:.1f}%) con datos')
        return p_canastas

    def calcular_penetracion_canastas_sensibles(self) -> pd.Series:
        """MÉTRICA 2: Penetración Sensibles (canastas, CS) — 25%"""
        p_can_cs = self.df['PENETRACION_CANASTAS_PCT_CS'].copy()
        n_con_datos = p_can_cs.notna().sum()
        pct = 100 * n_con_datos / self.n_skus
        print(f'  ✓ Penetración Sensibles (canastas, CS): {n_con_datos:,} SKUs ({pct:.1f}%) con datos')  # noqa: E501
        return p_can_cs

    def calcular_peso_categoria(self, w_a: float = 0.6, w_b: float = 0.4) -> pd.Series:
        """MÉTRICA 3: Peso en Categoría (venta general) — 20%

        v9 usa venta general:
          a = VENTA_PRODUCTO / TOTAL_VENTA_CATEGORIA
          b = TOTAL_VENTA_CATEGORIA / TOTAL_VENTA
          peso = a^0.6 * b^0.4
        """
        for col in ['VENTA_PRODUCTO', 'TOTAL_VENTA_CATEGORIA', 'TOTAL_VENTA']:
            if col not in self.df.columns:
                msg = f'Falta columna requerida: {col}'
                raise KeyError(msg)

        print(f'  ✓ Calculando Peso en Categoría (venta general, geom {w_a}/{w_b})...')

        venta_producto  = pd.to_numeric(self.df['VENTA_PRODUCTO'], errors='coerce')
        venta_categoria = pd.to_numeric(self.df['TOTAL_VENTA_CATEGORIA'], errors='coerce').replace(0, np.nan)  # noqa: E501
        venta_total     = pd.to_numeric(self.df['TOTAL_VENTA'], errors='coerce').replace(0, np.nan)

        a = (venta_producto / venta_categoria).clip(0, 1).fillna(0.0)
        b = (venta_categoria / venta_total).clip(0, 1).fillna(0.0)

        peso_cat = ((a ** w_a) * (b ** w_b)).clip(0, 1).fillna(0.0)

        self.df['peso_producto_en_cat'] = a
        self.df['peso_categoria_en_total'] = b

        n_con_datos = (peso_cat > 0).sum()
        pct = 100 * n_con_datos / self.n_skus
        print(f'    a:   min={a.min():.6f} | media={a.mean():.6f} | max={a.max():.6f}')
        print(f'    b:   min={b.min():.6f} | media={b.mean():.6f} | max={b.max():.6f}')
        print(f'    peso: min={peso_cat.min():.6f} | media={peso_cat.mean():.6f} | max={peso_cat.max():.6f}')  # noqa: E501
        print(f'    SKUs con peso > 0: {n_con_datos:,} ({pct:.1f}%)')

        return peso_cat

    def calcular_frecuencia(self) -> tuple:
        """Frecuencia de Recompra — SIN PESO en v9 (ver aviso al inicio del archivo).

        Se conserva el cálculo y la corrección de outliers validada el 20 ago
        (IQR + baja penetración → reemplazo por mediana de categoría), y la
        columna se expone en el output para diagnóstico, pero no entra a
        `indice_infaltable`.
        """  # noqa: W505
        frecuencia = self.df['FREC_RECOMPRA_TRIMESTRAL_PROM'].copy()
        penetracion = self.df.get('PENETRACION_CLIENTES_PCT', pd.Series([0.0] * self.n_skus))

        cat_frec = self.df.get('cat_frec_trimestral_mediana', pd.Series([np.nan] * self.n_skus))
        cat_frec = cat_frec.fillna(1.22) if cat_frec is not None else pd.Series([1.22] * self.n_skus)  # noqa: E501

        q1 = frecuencia.quantile(0.25)
        q3 = frecuencia.quantile(0.75)
        iqr = q3 - q1
        upper_bound = q3 + 1.5 * iqr
        lower_bound = q1 - 1.5 * iqr
        es_outlier = frecuencia.notna() & ((frecuencia < lower_bound) | (frecuencia > upper_bound))

        es_baja_penetracion = penetracion.notna() & (penetracion < 0.1)
        reemplazar_con_categoria = es_outlier & es_baja_penetracion
        n_reemplazos = reemplazar_con_categoria.sum()

        origen_frecuencia = pd.Series([np.nan] * self.n_skus, dtype=object)
        usa_sku = frecuencia.notna() & ~reemplazar_con_categoria
        usa_cat = frecuencia.notna() & reemplazar_con_categoria
        origen_frecuencia[usa_sku] = 'sku'
        origen_frecuencia[usa_cat] = 'categoria'

        if n_reemplazos > 0:
            frecuencia[reemplazar_con_categoria] = cat_frec[reemplazar_con_categoria]

        n_con_datos = frecuencia.notna().sum()
        pct = 100 * n_con_datos / self.n_skus
        print(f'  ✓ Frecuencia (diagnóstico, sin peso en v9): {n_con_datos:,} SKUs ({pct:.1f}%) '
              f'con datos ({n_reemplazos} reemplazos por categoría)')

        return frecuencia, frecuencia.notna(), origen_frecuencia

    def calcular_sustiticion(self) -> tuple:
        """MÉTRICA 5: Ponderador de Sustitución — 5%

        Cascada de fallback (SKU → subcategoría →categoría → negocio),
        ratio contra promedio de categoría, rank
        percentil separado entre SKUs con dato propio y SKUs con fallback.
        """
        col_dist = 'score_facilidad_sustitucion_directa'
        col_subcat = 'SUB_CATEGORY_DESCRIPTION'
        col_cat = 'CATEGORY_DESCRIPTION'

        d_sku = pd.to_numeric(self.df[col_dist], errors='coerce')
        tiene_sku = d_sku.notna()

        d_subcat = self.df.groupby(col_subcat)[col_dist].transform(
            lambda s: pd.to_numeric(s, errors='coerce').median())
        d_cat = self.df.groupby(col_cat)[col_dist].transform(
            lambda s: pd.to_numeric(s, errors='coerce').median())
        d_neg = self.df.groupby('NEG_DSC')[col_dist].transform(
            lambda s: pd.to_numeric(s, errors='coerce').median())
        d_final = d_sku.combine_first(d_subcat).combine_first(d_cat).combine_first(d_neg)
        d_final = d_final.fillna(d_neg.median())

        prom_cat = self.df.groupby(col_cat)[col_dist].transform(
            lambda s: pd.to_numeric(s, errors='coerce').mean())
        ratio = d_final / prom_cat.replace(0, np.nan)

        score_norm = pd.Series(np.nan, index=self.df.index, dtype=float)
        score_norm[tiene_sku] = (
            ratio[tiene_sku].groupby(self.df.loc[tiene_sku, col_cat]).rank(pct=True)
        )
        score_norm[~tiene_sku] = (
            ratio[~tiene_sku].groupby(self.df.loc[~tiene_sku, col_cat]).rank(pct=True)
        )
        score_norm = score_norm.fillna(0.5)

        origen = pd.Series('negocio', index=self.df.index)
        origen[d_neg.notna()]    = 'negocio'
        origen[d_cat.notna()]    = 'categoria'
        origen[d_subcat.notna()] = 'subcategoria'
        origen[tiene_sku]        = 'sku'

        tiene_datos = pd.Series(True, index=self.df.index)  # noqa: FBT003

        print('  ✓ Ponderador Sustitución (ratio, rank separado sku/fallback):')
        print(f'    Score final: min={score_norm.min():.4f} | media={score_norm.mean():.4f} | max={score_norm.max():.4f}')  # noqa: E501
        print(f'    Con dato propio: {tiene_sku.sum():,} ({tiene_sku.mean():.1%})')

        return score_norm, tiene_datos, origen

    def calcular_nielsen(self, strategy: str = 'exclude') -> tuple:
        """MÉTRICA 4: Nielsen — 10%"""
        nielsen = self.df['indice_importancia_nielsen'].copy()
        tiene_datos = nielsen.notna()
        n_con_datos = tiene_datos.sum()
        pct = 100 * n_con_datos / self.n_skus
        print(f'  ✓ Nielsen: {n_con_datos:,} SKUs ({pct:.1f}%) con datos')

        if strategy == 'fillna_zero':
            return nielsen.fillna(0), tiene_datos
        return nielsen, tiene_datos

    # =====================================================================
    # NORMALIZACIÓN MIN-MAX
    # =====================================================================

    def _normalizar_minmax(self, series: pd.Series) -> pd.Series:
        datos = series.dropna()
        if len(datos) == 0:
            return pd.Series([0.5] * len(series))
        s_min, s_max = datos.min(), datos.max()
        if s_max == s_min:
            return pd.Series([0.5] * len(series))
        return (series - s_min) / (s_max - s_min)

    # =====================================================================
    # CÁLCULO PRINCIPAL
    # =====================================================================

    def calcular(self, estrategia_nielsen: str = 'exclude',
                w_a_categoria: float = 0.6, w_b_categoria: float = 0.4) -> pd.DataFrame:
        """Calcula Índice de Infaltables v9 (5 métricas).

        ESTRATEGIA NIELSEN: EXCLUSIÓN
        - SKUs CON Nielsen: usa 5 métricas con pesos normales (100%)
        - SKUs SIN Nielsen: redistribuye su peso a las otras 4
        """
        print('\n' + '='*80)
        print('CÁLCULO ÍNDICE DE INFALTABLES v9 (5 MÉTRICAS)')
        print('='*80)
        print(f'Total SKUs: {self.n_skus:,}\n')

        # =================================================================
        # 🎛️ PESOS v9 — entregados 20 ago 2026 (suman 100%)
        # =================================================================
        W_PENETRACION       = 0.40   # Penetración (canastas, general)  # noqa: N806
        W_PENETRACION_SENSIBLES  = 0.25   # Penetración Sensibles (canastas, CS)  # noqa: N806
        W_PESO_CATEGORIA    = 0.20   # Peso en Categoría (venta general)  # noqa: N806
        W_NIELSEN           = 0.10   # Nielsen  # noqa: N806
        W_SUSTITUCION       = 0.05   # Ponderador de Sustitución  # noqa: N806
        # Total: 40+25+20+10+5 = 100%
        # =================================================================

        suma_pesos_check = W_PENETRACION + W_PENETRACION_SENSIBLES + W_PESO_CATEGORIA + W_NIELSEN + W_SUSTITUCION  # noqa: E501
        assert abs(suma_pesos_check - 1.0) < 1e-9, f'Los pesos v9 suman {suma_pesos_check:.4f}, no 1.0'  # noqa: E501, S101

        # ====== PASO 1: MÉTRICAS RAW ======
        print('PASO 1: Calculando métricas raw (5 métricas + diagnóstico)...\n')

        p_canastas_raw    = self.calcular_penetracion_canastas()
        p_canastas_cs_raw = self.calcular_penetracion_canastas_sensibles()
        peso_cat_raw       = self.calcular_peso_categoria(w_a=w_a_categoria, w_b=w_b_categoria)
        nielsen_raw, tiene_nielsen = self.calcular_nielsen(strategy=estrategia_nielsen)
        sustitucion_raw, tiene_sustitucion, origen_sustitucion = self.calcular_sustiticion()

        # Diagnóstico, sin peso en el índice
        frecuencia_raw, tiene_frecuencia, origen_frecuencia = self.calcular_frecuencia()
        p_clientes_raw = self.df.get('PENETRACION_CLIENTES_PCT', pd.Series([np.nan] * self.n_skus))
        p_clientes_cs_raw = self.df.get('PENETRACION_CLIENTES_PCT_CS', pd.Series([np.nan] * self.n_skus))  # noqa: E501

        print()

        # ====== PASO 2: NORMALIZAR ======
        print('PASO 2: Normalizando métricas (Min-Max [0, 1])...\n')

        norm_penetracion     = self._normalizar_minmax(p_canastas_raw)
        norm_penetracion_cs  = self._normalizar_minmax(p_canastas_cs_raw)
        norm_peso_categoria  = self._normalizar_minmax(peso_cat_raw)

        norm_nielsen = pd.Series([np.nan] * self.n_skus)
        if tiene_nielsen.sum() > 0:
            n_con_datos = nielsen_raw[tiene_nielsen]
            n_min, n_max = n_con_datos.min(), n_con_datos.max()
            if n_max != n_min:
                norm_nielsen[tiene_nielsen] = (nielsen_raw[tiene_nielsen] - n_min) / (n_max - n_min)  # noqa: E501
            else:
                norm_nielsen[tiene_nielsen] = 0.5

        # Sustitución ya viene normalizada (rank percentil)
        # de calcular_sustiticion()
        norm_sustitucion = sustitucion_raw

        # Compuerta de demanda: la irremplazabilidad solo vale si
        # hay demanda real. Se mantiene el corte en percentil 90
        # sobre PENETRACION_CLIENTES_PCT_CS.
        pen_rank = self.df['PENETRACION_CLIENTES_PCT_CS'].rank(pct=True)
        compuerta = (pen_rank / 0.90).clip(upper=1.0).fillna(0.0)
        norm_sustitucion = norm_sustitucion * compuerta

        print('  ✓ Todas las métricas normalizadas a [0, 1]\n')

        # ====== PASO 3: ÍNDICE (vectorizado, sin loop) ======
        print('PASO 3: Calculando Índice de Infaltables v9...\n')

        # Matriz de valores y pesos por SKU, redistribuyendo Nielsen
        # si falta. A diferencia de v8, este cálculo está
        # vectorizado (sin bucle for por fila), lo que baja el tiempo de
        # cálculo en órdenes de magnitud sobre el universo completo.
        valores_fijos = pd.DataFrame({
            'penetracion':    norm_penetracion.fillna(0.0),
            'penetracion_cs': norm_penetracion_cs.fillna(0.0),
            'peso_categoria': norm_peso_categoria.fillna(0.0),
            'sustitucion':    norm_sustitucion.fillna(0.0),
        })
        pesos_fijos = {
            'penetracion': W_PENETRACION,
            'penetracion_cs': W_PENETRACION_SENSIBLES,
            'peso_categoria': W_PESO_CATEGORIA,
            'sustitucion': W_SUSTITUCION,
        }

        numerador = sum(valores_fijos[k] * w for k, w in pesos_fijos.items())
        peso_total = pd.Series(sum(pesos_fijos.values()), index=self.df.index)

        # Nielsen solo suma si el SKU tiene dato; si no, ni numerador ni
        # denominador lo incluyen (redistribución automática por fila).
        tiene_niel_mask = norm_nielsen.notna()
        numerador = numerador + (norm_nielsen.fillna(0.0) * W_NIELSEN).where(tiene_niel_mask, 0.0)
        peso_total = peso_total + pd.Series(W_NIELSEN, index=self.df.index).where(tiene_niel_mask, 0.0)  # noqa: E501

        indice = (numerador / peso_total * 100)

        print('  ✓ Pesos v9:')
        print(f'    Penetración (canastas, gral.): {W_PENETRACION:.0%}')
        print(f'    Penetración Sensibles (CS):       {W_PENETRACION_SENSIBLES:.0%}')
        print(f'    Peso en Categoría (gral.):     {W_PESO_CATEGORIA:.0%}')
        print(f'    Nielsen:                       {W_NIELSEN:.0%} (se excluye si falta)')
        print(f'    Ponderador Sustitución:        {W_SUSTITUCION:.0%}')
        print(f'    TOTAL:                         {suma_pesos_check:.0%}')
        print('    (Frecuencia y penetración de clientes quedan fuera del índice — solo diagnóstico)')  # noqa: E501
        print()
        print(f"  ✓ Estrategia Nielsen: {'EXCLUSIÓN' if estrategia_nielsen == 'exclude' else 'FILLNA_ZERO'}")  # noqa: E501
        print(f'    SKUs con Nielsen:    {tiene_nielsen.sum():>7,} ({100*tiene_nielsen.sum()/self.n_skus:.1f}%)')  # noqa: E501
        print(f'    SKUs sin Nielsen:    {(~tiene_nielsen).sum():>7,} ({100*(~tiene_nielsen).sum()/self.n_skus:.1f}%) — NO castigados')  # noqa: E501
        print()
        print(f'  ✓ Índice final: min={indice.min():.1f} | mediana={indice.median():.1f} | max={indice.max():.1f}\n')  # noqa: E501

        # ====== PASO 5: CLASIFICAR (3 tramos, cortes por curvatura) ======
        # Reemplaza el esquema de 5 tramos
        # (codo + tercios/mitades del resto). Los tercios y mitades no
        # correspondían a quiebres reales de la distribución
        # -- se verificó con evidencia el 22 ago: Muy Importante
        # y Complementario/Ocasional salían en proporciones idénticas
        # entre sí (33,2%/33,2% y 16,6%/16,6%) porque eran fracciones fijas
        # del resto, no hallazgos de los datos. clasificar_3_tramos() usa
        # kneedle (punto de máxima curvatura) en vez de fracciones fijas,
        # aplicado dos veces: una vez sobre la curva
        # completa (corte Crítico) y otra sobre lo que
        # queda (corte Importante) -- cada corte sale de la forma real
        # de su propio tramo, no de una regla aritmética sobre el resto.
        print('PASO 5: Clasificando SKUs con distribución real (3 tramos, curvatura)...\n')

        clasificacion = clasificar_3_tramos(indice)
        print()

        for clase in ['Crítico', 'Importante', 'Complementario']:
            count = (clasificacion == clase).sum()
            pct = 100 * count / self.n_skus
            print(f'  ✓ {clase:20s}: {count:7,} ({pct:6.1f}%)')

        sin_datos = (clasificacion == 'Sin Datos').sum()
        if sin_datos > 0:
            pct = 100 * sin_datos / self.n_skus
            print(f"  ✓ {'Sin Datos':20s}: {sin_datos:7,} ({pct:6.1f}%)")
        print()

        # ====== CONSTRUIR DATAFRAME RESULTADOS ======
        self.resultados = pd.DataFrame({
            'PRODUCT_ID': self.df.get('PRODUCT_ID', range(self.n_skus)),
            'EAN': self.df.get('EAN', ''),
            'PRODUCT_DESCRIPTION': self.df.get('PRODUCT_DESCRIPTION', ''),
            'NEG_DSC': self.df.get('NEG_DSC', ''),
            'LIN_DESC': self.df.get('LIN_DESC', ''),
            'SEC_DSC': self.df.get('SEC_DSC', ''),
            'CATEGORY_DESCRIPTION': self.df.get('CATEGORY_DESCRIPTION', ''),
            'SUB_CATEGORY_DESCRIPTION': self.df.get('SUB_CATEGORY_DESCRIPTION', ''),
            'marca_propia_flag': self.df.get('marca_propia_flag', 0),

            # Raw — 5 métricas con peso
            'penetracion_raw': p_canastas_raw,
            'penetracion_sensibles_raw': p_canastas_cs_raw,
            'peso_categoria_raw': peso_cat_raw,
            'nielsen_raw': nielsen_raw,
            'sustitucion_raw': sustitucion_raw,

            # Raw — diagnóstico, sin peso en v9
            'penetracion_clientes_raw': p_clientes_raw,
            'penetracion_clientes_cs_raw': p_clientes_cs_raw,
            'frecuencia_raw': frecuencia_raw,

            'peso_producto_en_cat': self.df.get('peso_producto_en_cat', np.nan),
            'peso_categoria_en_total': self.df.get('peso_categoria_en_total', np.nan),

            # Normalizados
            'penetracion_norm': norm_penetracion,
            'penetracion_sensibles_norm': norm_penetracion_cs,
            'peso_categoria_norm': norm_peso_categoria,
            'nielsen_norm': norm_nielsen,
            'sustitucion_norm': norm_sustitucion,

            # Trazabilidad
            'tiene_datos_frecuencia': tiene_frecuencia,
            'origen_frecuencia': origen_frecuencia,
            'tiene_datos_sustitucion': tiene_sustitucion,
            'origen_sustitucion': origen_sustitucion,
            'tiene_datos_nielsen': tiene_nielsen,
            'estrategia_nielsen_usada': estrategia_nielsen,

            # Resultado
            'indice_infaltable': indice,
            'clasificacion': clasificacion,

            # Contexto comercial y de ciclo de vida — pasan derecho desde
            # el df de entrada, no entran a ningún cálculo.

            # Van acá para que viajen solos al Excel de rankings.
            # Si faltan en el df, quedan en NaN y el Excel sale sin dato.
            # Se exponen las dos versiones de venta. El índice usa la BRUTA
            # (peso en categoría); el Excel de rankings muestra la NETA.
            'VENTA_PRODUCTO': self.df.get('VENTA_PRODUCTO', np.nan),
            'VENTA_NETA_PRODUCTO': self.df.get('VENTA_NETA_PRODUCTO', np.nan),
            'TOTAL_VENTA': self.df.get('TOTAL_VENTA', np.nan),
            'TOTAL_VENTA_NETA': self.df.get('TOTAL_VENTA_NETA', np.nan),
            'PVP': self.df.get('PVP', np.nan),
            'FECHA_PRIMERA_VENTA': self.df.get('FECHA_PRIMERA_VENTA', pd.NaT),
            'MESES_CON_VENTA': self.df.get('MESES_CON_VENTA', np.nan),
            'DIAS_DESDE_PRIMERA_VENTA': self.df.get('DIAS_DESDE_PRIMERA_VENTA', np.nan),
            'ES_PRODUCTO_NUEVO_90D': self.df.get('ES_PRODUCTO_NUEVO_90D', np.nan),
            'SEGMENTO_MADUREZ_SKU': self.df.get('SEGMENTO_MADUREZ_SKU', ''),
        })

        print('='*80)
        print()

        return self.resultados

    def validaciones(self):
        """Ejecuta validaciones de calidad sobre las
        5 métricas con peso."""
        if self.resultados is None:
            print('❌ Error: Ejecuta calc.calcular() primero')
            return

        print('='*80)
        print('VALIDACIONES DE CALIDAD — v9')
        print('='*80 + '\n')

        print('1. RANGO DE ÍNDICE FINAL')
        print(f"   Mín: {self.resultados['indice_infaltable'].min():.2f}")
        print(f"   Máx: {self.resultados['indice_infaltable'].max():.2f}")
        print(f"   Mediana: {self.resultados['indice_infaltable'].median():.2f}\n")

        print('2. DISTRIBUCIÓN POR CLASIFICACIÓN')
        dist = self.resultados['clasificacion'].value_counts()
        for clase, count in dist.items():
            pct = 100 * count / len(self.resultados)
            print(f'   {clase:20s}: {count:7,} ({pct:6.1f}%)')
        print()

        print('3. CORRELACIONES ENTRE MÉTRICAS NORMALIZADAS (5 con peso)')
        cols_norm = ['penetracion_norm', 'penetracion_sensibles_norm',
                     'peso_categoria_norm', 'nielsen_norm', 'sustitucion_norm']
        labels = ['Penetr', 'Penetr_Sens', 'Peso_Cat', 'Nielsen', 'Sust']
        corr_matrix = self.resultados[cols_norm].corr()

        for i in range(len(cols_norm)):
            for j in range(i + 1, len(cols_norm)):
                r = corr_matrix.iloc[i, j]
                flag = '  ⚠️ ALTA' if abs(r) > 0.85 else ''
                print(f'   {labels[i]:12s} vs {labels[j]:12s}: {r:+.3f}{flag}')
        print()

        print('4. COMPLETITUD DE DATOS')
        for col in cols_norm:
            pct = 100 * (1 - self.resultados[col].isna().sum() / len(self.resultados))
            print(f'   {col}: {pct:.1f}% completo')

        if 'origen_sustitucion' in self.resultados.columns:
            n = len(self.resultados)
            for val, label in [('sku', 'Score SKU (directo)'),
                               ('subcategoria', 'Fallback subcategoría'),
                               ('categoria', 'Fallback categoría'),
                               ('negocio', 'Fallback negocio')]:
                n_val = (self.resultados['origen_sustitucion'] == val).sum()
                print(f'     {label:<24s}: {n_val:>7,} ({100*n_val/n:.1f}%)')

        print('\n5. CRITERIO DE CIERRE — Críticos en cuartil bajo de penetración sensibles')
        crit = self.resultados[self.resultados['clasificacion'] == 'Crítico']
        if len(crit) > 0:
            p25 = self.resultados['penetracion_sensibles_raw'].quantile(0.25)
            n_fuera = (crit['penetracion_sensibles_raw'] < p25).sum()
            estado = '✓ OK' if n_fuera == 0 else f'⚠️ REVISAR ({n_fuera} SKUs)'
            print(f'   Críticos en cuartil bajo: {n_fuera:,} de {len(crit):,}  {estado}')

        print('\n' + '='*80 + '\n')


# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------

def main() -> None:
    usuario = 'infaltables'
    # parse input variables
    args = vars(parser.parse_args())
    gcp_project: str = args['project_id']
    execution_date: str = args['execution_date']
    store_banner: str = args['store_banner']
    n_substitutes: int = args['n_substitutes']

    store_banner_table = store_banner.replace(' ', '_').lower()
    upper_store_banner_table = store_banner_table.upper()

    upper_store_banner = store_banner.upper()
    execution_date = pd.to_datetime(execution_date[:8] + '01').strftime('%Y-%m-%d')

    logging.info(f'execution_date: {execution_date}')
    logging.info(f'store_banner: {store_banner}')
    logging.info(f'n_substitutes: {n_substitutes}')

    # Set gbq client for all subsequent queries
    gbq_client = Client()

    logging.info('Creacion tabla infaltables raw sales')
    createTableAsSelect(
        query=SQL_QUERIES['raw_sales'].substitute(
            gcp_project_cda = 'cl-cda-prod',
            execution_date = execution_date,
            store_banner = store_banner
        ),
        table_ref=f'{gcp_project}.TMP.TMP_INFALTABLES_RAW_SALES_{upper_store_banner_table}',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )

    logging.info('Creacion tabla infaltable penetracion')
    createTableAsSelect(
        query=SQL_QUERIES['infaltables_penetracion'].substitute(
            gcp_project = gcp_project,
            gcp_project_cda = 'cl-cda-prod',
            execution_date = execution_date,
            store_banner = store_banner
        ),
        table_ref=f'{gcp_project}.TMP.TMP_INFALTABLES_PENETRACION_SP_{upper_store_banner_table}',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )

    logging.info('Ejecucion Query data_infaltables')
    data_infaltables = readBigQuery(SQL_QUERIES['data_infaltables'].substitute(
        gcp_project = gcp_project,
        gcp_project_cda = 'cl-cda-prod',
        execution_date = execution_date,
        n_substitutes = n_substitutes,
        store_banner = store_banner,
        upper_store_banner = upper_store_banner
        ),
    user = usuario,
    gbq_client = gbq_client
    )

    logging.info(f'Filas obtenidas: {len(data_infaltables):,}')
    logging.info(f'df: {len(data_infaltables):,} filas, {data_infaltables.shape[1]} columnas\n')

    # Columnas mínimas que requiere v9
    requeridas_v9 = [
        'PRODUCT_ID', 'PRODUCT_DESCRIPTION', 'CATEGORY_DESCRIPTION',
        'SUB_CATEGORY_DESCRIPTION', 'NEG_DSC',
        'PENETRACION_CANASTAS_PCT', 'PENETRACION_CANASTAS_PCT_CS',
        'PENETRACION_CLIENTES_PCT_CS', # usada por la compuerta de demanda
        'VENTA_PRODUCTO', 'TOTAL_VENTA_CATEGORIA', 'TOTAL_VENTA',
        'indice_importancia_nielsen',
        'score_facilidad_sustitucion_directa',
        'FREC_RECOMPRA_TRIMESTRAL_PROM', # diagnóstico, sin peso en v9
    ]
    faltan = [c for c in requeridas_v9 if c not in data_infaltables.columns]
    if faltan:
        logging.info(f'⚠️  Columnas faltantes en df: {faltan}')
    else:
        logging.info('✓ df tiene todas las columnas requeridas')

    calc = IndiceInfaltablesCalculator_v9(data_infaltables)
    resultados = calc.calcular(estrategia_nielsen='exclude')
    calc.validaciones()

    # Pesos y etiquetas alineados a las columnas normalizadas
    # que produce v9.
    PESOS = {  # noqa: N806
        'penetracion_norm':            0.40,
        'penetracion_sensibles_norm':  0.25,
        'peso_categoria_norm':         0.20,
        'sustitucion_norm':            0.05,
        'nielsen_norm':                0.10,
    }

    ETIQUETAS = {  # noqa: N806
        'penetracion_norm':           'Penetración',
        'penetracion_sensibles_norm': 'Penetración Sensibles',
        'peso_categoria_norm':        'Peso Categoría',
        'sustitucion_norm':           'Sustitución',
        'nielsen_norm':               'Nielsen',
    }

    # Métricas con peso 0%: se calculan y exportan en el resultado pero no
    # aportan al índice, por eso quedan fuera de estos rankings.
    PESOS_CERO = ['penetracion_clientes_norm', 'penetracion_canastas_norm']  # noqa: N806

    # Única métrica redistribuible: si falta, su peso se reparte
    # entre las demás.
    METRICA_REDISTRIBUIBLE = 'nielsen_norm'  # noqa: N806

    # Columnas de venta y ciclo de vida que vienen del df de origen.
    # No pesan en el índice: se muestran como contexto.
    #
    # El Excel trabaja con VENTA NETA. Numerador y denominador tienen que
    # ser el mismo concepto: mezclar venta neta con total bruto da
    # un porcentaje inflado por la diferencia de IVA y descuentos, no por
    # participación real. Si alguna vez hay que volver a bruta, se cambian
    # estas dos constantes y nada más.
    COL_VENTA         = 'VENTA_NETA_PRODUCTO'  # noqa: N806
    COL_TOTAL_FORMATO = 'TOTAL_VENTA_NETA'  # noqa: N806
    COL_PVP           = 'PVP'                      # precio de venta promedio  # noqa: N806
    COL_PCT_VENTA     = 'pct_venta_neta_formato'   # calculada acá, no viene del df  # noqa: N806

    COLS_CICLO_VIDA = [  # noqa: N806
        'FECHA_PRIMERA_VENTA',
        'MESES_CON_VENTA',
        'DIAS_DESDE_PRIMERA_VENTA',
        'ES_PRODUCTO_NUEVO_90D',
        'SEGMENTO_MADUREZ_SKU',
    ]

    # =====================================================================
    # CÁLCULO DE APORTES
    # =====================================================================

    def calcular_aportes(df: pd.DataFrame) -> pd.DataFrame:
        """Descompone el índice en el aporte en puntos de cada métrica.

            peso_total = 1,00 si el SKU tiene Nielsen, 0,90 si no
            aporte_m   = norm_m x peso_m / peso_total x 100

        Los aportes suman el valor del índice.
        Eso permite leer una fila y entender exactamente de
        dónde salió el puntaje.

        Nota sobre nulos: se rellenan con 0.
        Para Nielsen la ausencia es neutra porque la métrica sale
        del numerador y del denominador a la vez.
        Para sustitución NO es neutra: el cero entra con peso completo.
        """
        d = df.copy()

        faltan = [c for c in PESOS if c not in d.columns]
        if faltan:
            msg = (
                f"Faltan columnas normalizadas en 'resultados': {faltan}. "
            )
            raise KeyError(
                msg
            )

        tiene_nielsen = d[METRICA_REDISTRIBUIBLE].notna()
        peso_redistribuible = PESOS[METRICA_REDISTRIBUIBLE]
        peso_total = tiene_nielsen.map({True: 1.0, False: 1.0 - peso_redistribuible})

        cols_aporte = []
        for col, peso in PESOS.items():
            nombre = f"aporte_{col.replace('_norm', '')}"
            aporte = d[col].fillna(0.0) * peso / peso_total * 100
            if col == METRICA_REDISTRIBUIBLE:
                aporte = aporte.where(tiene_nielsen, 0.0)
            d[nombre] = aporte
            cols_aporte.append(nombre)

        d['peso_total_aplicado'] = peso_total
        d['suma_aportes'] = d[cols_aporte].sum(axis=1)

        return d

    def verificar_aportes(df: pd.DataFrame, tolerancia: float = 0.01) -> None:
        """Contrasta la suma de aportes contra el índice almacenado.

        Si no calzan, los pesos de PESOS no son los que se usaron al
        calcular el índice. Es la comprobación más rápida de que este
        script y el cálculo están sincronizados.
        """
        if 'indice_infaltable' not in df.columns:
            print("  ⚠ No existe 'indice_infaltable': no se puede verificar")
            return

        dif = (df['suma_aportes'] - df['indice_infaltable']).abs()
        dif_max = dif.max()
        n_fuera = int((dif > tolerancia).sum())

        if n_fuera == 0:
            print(f'  ✓ Aportes verificados: coinciden con el índice '
                f'(desviación máx. {dif_max:.6f})')
        else:
            print(f'  ⚠ {n_fuera:,} SKUs con desviación sobre {tolerancia}. '
                f'Máxima: {dif_max:.4f}')
            print('    Revisar que PESOS coincida con el bloque W_* de calcular()')

    # =====================================================================
    # COLUMNAS COMERCIALES Y DE CICLO DE VIDA
    # =====================================================================

    def _si_no(serie: pd.Series) -> pd.Series:
        """Normaliza un flag a 'Si'/'No'.

        BigQuery devuelve BOOL como True/False
        (Excel los muestra en inglés) e INT64
        como 0/1. Mapear los dos casos evita que
        la misma columna se vea distinta
        según cómo esté tipada la query.
        """
        mapa = {True: 'Si', False: 'No', 1: 'Si', 0: 'No',  # noqa: F601
                '1': 'Si', '0': 'No', 'true': 'Si', 'false': 'No',
                'True': 'Si', 'False': 'No', 'Y': 'Si', 'N': 'No'}
        return serie.map(lambda v: '' if pd.isna(v) else mapa.get(v, str(v)))

    def adjuntar_columnas_comerciales(df: pd.DataFrame,
                                    df_origen: pd.DataFrame | None = None) -> pd.DataFrame:
        """Deja el DataFrame listo para exportar el bloque comercial:

        1. Trae desde `df_origen` (el df de entrada al cálculo)
        las columnas de venta y ciclo de vida que el calculador
        no haya propagado al resultado.
        2. Calcula `pct_venta_formato`=VENTA_PRODUCTO/venta total del
        formato.
        3. Normaliza tipos: fecha real para Excel y flag en Si/No.

        El denominador de la participación es el total del formato
        y se calcula UNA vez sobre el universo completo, antes de filtrar.
        Por eso el porcentaje de un SKU es el mismo en las
        cuatro hojas: en la hoja de PGC sin marca propia los valores
        NO suman 100%, suman lo que ese subconjunto pesa en el formato.

        `escribir_hoja` omite en silencio las columnas que no existan,
        así que acá se informa explícitamente cuáles quedaron
        fuera: es la única forma de notar que el Excel salió sin ellas.
        """
        d = df.copy()
        requeridas = [COL_VENTA, COL_TOTAL_FORMATO, COL_PVP, *COLS_CICLO_VIDA]

        # --- 1. traer las que falten desde el df de origen ---------------
        if df_origen is not None:
            traer = [c for c in requeridas
                    if c not in d.columns and c in df_origen.columns]
            if traer:
                if 'PRODUCT_ID' not in df_origen.columns:
                    msg = 'df_origen no tiene PRODUCT_ID: no se puede cruzar.'
                    raise KeyError(msg)

                base = df_origen[['PRODUCT_ID', *traer]].copy()
                # Clave normalizada: PRODUCT_ID suele venir int en un lado
                # y str en el otro, y el merge directo devuelve todo nulo
                # sin avisar.
                base['_key'] = base['PRODUCT_ID'].astype(str).str.strip()
                n_antes = len(base)
                base = base.drop_duplicates(subset='_key').drop(columns='PRODUCT_ID')
                if len(base) < n_antes:
                    print(f'  ⚠ df_origen tenía {n_antes - len(base):,} filas repetidas '
                        f'por PRODUCT_ID: se conserva la primera de cada SKU')

                d['_key'] = d['PRODUCT_ID'].astype(str).str.strip()
                filas_antes = len(d)
                d = d.merge(base, on='_key', how='left').drop(columns='_key')
                assert len(d) == filas_antes, 'El cruce con df_origen duplicó filas'  # noqa: S101

                cruzadas = d[traer[0]].notna().mean() if traer else 0.0
                print(f"  ✓ Traídas desde df_origen: {', '.join(traer)} "
                    f"(cruce {cruzadas:.1%} de los SKUs)")

        ausentes = [c for c in requeridas if c not in d.columns]
        if ausentes:
            print(f"  ⚠ No están en el resultado ni en df_origen: {', '.join(ausentes)}")
            print('    Esas columnas van a salir vacías del Excel. Pasar df_origen=df '
                'si el calculador no las propaga.')

        # --- 2. participación en la venta del formato --------------------
        if COL_VENTA in d.columns:
            venta = pd.to_numeric(d[COL_VENTA], errors='coerce')

            if not venta.notna().any():
                # Pasa si el calculador expone la columna
                # pero la query no la trae:
                # llega llena de NaN y un denominador 0 no significa nada.
                print(f'  ⚠ {COL_VENTA} viene sin ningún valor: no se calcula el %')
                venta = None

        if COL_VENTA in d.columns and venta is not None:
            total = None
            if COL_TOTAL_FORMATO in d.columns:
                tot = pd.to_numeric(d[COL_TOTAL_FORMATO], errors='coerce').dropna()
                if len(tot):
                    if tot.nunique() > 1:  # noqa: PD101
                        print(f'  ⚠ {COL_TOTAL_FORMATO} no es constante '
                            f'({tot.nunique():,} valores distintos): se usa el máximo')
                    total = float(tot.max())

            if not total:
                total = float(venta.sum())
                print(f'  ⚠ Sin {COL_TOTAL_FORMATO} utilizable: el denominador pasa a ser '
                    f'la suma de {COL_VENTA} de los SKUs del resultado')

            d[COL_PCT_VENTA] = venta / total if total else pd.NA

            cobertura = venta.sum() / total if total else 0
            print(f'  · Denominador del % venta: {total:,.0f} · los {len(d):,} SKUs del '
                f'resultado representan {cobertura:.1%}')

        # --- 3. tipos ----------------------------------------------------
        if 'FECHA_PRIMERA_VENTA' in d.columns:
            fecha = pd.to_datetime(d['FECHA_PRIMERA_VENTA'], errors='coerce')
            # openpyxl no escribe datetimes
            # con timezone: revienta al guardar.
            if getattr(fecha.dtype, 'tz', None) is not None:
                fecha = fecha.dt.tz_localize(None)
            d['FECHA_PRIMERA_VENTA'] = fecha

        if 'ES_PRODUCTO_NUEVO_90D' in d.columns:
            d['ES_PRODUCTO_NUEVO_90D'] = _si_no(d['ES_PRODUCTO_NUEVO_90D'])

        for col in (COL_PVP, 'MESES_CON_VENTA', 'DIAS_DESDE_PRIMERA_VENTA'):
            if col in d.columns:
                d[col] = pd.to_numeric(d[col], errors='coerce')

        if COL_PVP in d.columns:
            # PVP suele venir de una división por unidades:
            # los SKUs sin unidades quedan en inf, y openpyxl escribe inf
            # como texto en una columna que el usuario va a
            # querer promediar. Mejor vacío.
            d[COL_PVP] = d[COL_PVP].replace([np.inf, -np.inf], np.nan)
            sin_pvp = d[COL_PVP].isna().sum()
            if sin_pvp:
                print(f'  · PVP sin valor en {sin_pvp:,} SKUs ({sin_pvp / len(d):.1%})')

        return d

    # =====================================================================
    # ARMADO DE COLUMNAS
    # =====================================================================

    COLS_ID = [  # noqa: N806
        ('PRODUCT_ID',               'SKU',           12),
        ('PRODUCT_DESCRIPTION',      'Descripción',   38),
        ('CATEGORY_DESCRIPTION',     'Categoría',     22),
        ('SUB_CATEGORY_DESCRIPTION', 'Subcategoría',  22),
        ('NEG_DSC',                  'Negocio',       10),
    ]

    COLS_TRAZA = [  # noqa: N806
        ('origen_frecuencia',  'Origen frec.',  13),
        ('origen_sustitucion', 'Origen sust.',  13),
    ]

    # Bloque comercial y de ciclo de vida. Va al final de
    # todas las hojas, después de las columnas del índice.
    # No entra al cálculo: es contexto de lectura.
    COLS_COMERCIALES = [  # noqa: N806
        (COL_VENTA,                  'Venta neta producto',  16),
        (COL_PCT_VENTA,              '% Venta neta formato', 14),
        (COL_PVP,                    'PVP',                  11),
        ('FECHA_PRIMERA_VENTA',      'Primera venta',        14),
        ('MESES_CON_VENTA',          'Meses con venta',      13),
        ('DIAS_DESDE_PRIMERA_VENTA', 'Días 1ª venta',        13),
        ('ES_PRODUCTO_NUEVO_90D',    'Nuevo 90d',            10),
        ('SEGMENTO_MADUREZ_SKU',     'Segmento madurez',     22),
    ]

    def construir_columnas(incluir_mpe: bool,
                        incluir_aportes: bool = False,
                        incluir_comerciales: bool = True) -> list[tuple[str, str, int]]:
        """Devuelve (columna_origen, encabezado, ancho)en orden de presentación.

        Por defecto muestra únicamente las 6 métricas NORMALIZADAS con peso.
        Quedan fuera las raw y las dos penetraciones generales, que tienen peso 0%.

        Con `incluir_aportes=True` se agrega, después de las normalizadas, el aporte
        en puntos de cada una al índice final.

        Con `incluir_comerciales=True` (por defecto) se agrega al final el bloque de
        venta y ciclo de vida. Para moverlo antes de las columnas de trazabilidad,
        invertir el orden de las dos últimas líneas de esta función.
        """  # noqa: W505
        cols = list(COLS_ID)
        if incluir_mpe:
            cols.append(('marca_propia_flag', 'MPE', 7))

        cols += [
            ('indice_infaltable', 'ÍNDICE', 11),
            ('clasificacion',     'Clasificación', 16),
        ]

        # Normalizadas efectivamente usadas, ordenadas por peso descendente
        por_peso = sorted(PESOS.items(), key=lambda kv: -kv[1])
        for col, peso in por_peso:
            cols.append((col, f'{ETIQUETAS[col]}\nnorm ({peso:.0%})', 14))

        if incluir_aportes:
            for col, _ in por_peso:
                nombre = f"aporte_{col.replace('_norm', '')}"
                cols.append((nombre, f'{ETIQUETAS[col]}\naporte (pts)', 14))

        cols += COLS_TRAZA
        if incluir_comerciales:
            cols += COLS_COMERCIALES
        return cols

    # =====================================================================
    # DEFINICIÓN DEL RANKING
    # =====================================================================

    RANKINGS = [  # noqa: N806
        {
            'hoja': '1. Ranking General',
            'filtro': None,
            'incluir_mpe': True,
            'por_categoria': False,
        }
    ]

    def preparar_ranking(df: pd.DataFrame, cfg: dict) -> tuple[pd.DataFrame, str]:
        """Aplica filtro, ordena y numera.
        Devuelve (df, nombre de la columna de rank)."""
        d = df if cfg['filtro'] is None else df[cfg['filtro'](df)]
        d = d.copy()

        if cfg['por_categoria']:
            d = d.sort_values(['CATEGORY_DESCRIPTION', 'indice_infaltable'],
                            ascending=[True, False]).reset_index(drop=True)
            d['rank_categoria'] = d.groupby('CATEGORY_DESCRIPTION').cumcount() + 1
            return d, 'rank_categoria'

        d = d.sort_values('indice_infaltable', ascending=False).reset_index(drop=True)
        d['rank'] = range(1, len(d) + 1)
        return d, 'rank'

    # =====================================================================
    # PIPELINE
    # =====================================================================

    def generar_rankings(resultados: pd.DataFrame,
                        incluir_aportes: bool = False,
                        df_origen: pd.DataFrame | None = None,
                        incluir_comerciales: bool = True) -> dict:
        """`df_origen` es el DataFrame de entrada al cálculo. Se usa solo
        para traer venta y ciclo de vida cuando el calculador no propaga
        esas columnas al resultado. Pasarlo siempre: si ya vienen en
        `resultados`, el cruce no se ejecuta.
        """

        print('\n' + '=' * 80)
        print('RANKINGS DE INFALTABLES — ESTILO SMU')
        print('=' * 80)

        print(f'\nSKUs en resultados: {len(resultados):,}')

        print('\nVerificando métricas de la metodología v6...')
        df_aportes = calcular_aportes(resultados)
        verificar_aportes(df_aportes)

        if incluir_comerciales:
            print('\nBloque comercial y de ciclo de vida...')
            df_aportes = adjuntar_columnas_comerciales(df_aportes, df_origen)

        print('\n  Métricas normalizadas que se muestran (las que tienen peso):')
        for col, peso in sorted(PESOS.items(), key=lambda kv: -kv[1]):
            cob = df_aportes[col].notna().mean()
            alerta = '  ← castiga si falta' if col == 'sustitucion_norm' and cob < 1 else ''
            print(f'    {ETIQUETAS[col]:<20s} {peso:>5.0%}   cobertura {cob:>6.1%}{alerta}')

        omitidas = [c for c in PESOS_CERO if c in df_aportes.columns]
        if omitidas:
            print(f"\n  Omitidas por tener peso 0% en v6: {', '.join(omitidas)}")

        salidas = {}
        for cfg in RANKINGS:
            d, _col_rank = preparar_ranking(df_aportes, cfg)
            construir_columnas(cfg['incluir_mpe'], incluir_aportes,
                                    incluir_comerciales)
            salidas[cfg['hoja']] = d

            extra = ''
            if cfg['por_categoria']:
                extra = f" · {d['CATEGORY_DESCRIPTION'].nunique()} categorías"
            print(f"\n  ✓ {cfg['hoja']:<26s} {len(d):>7,} SKUs{extra}")

            dist = d['clasificacion'].value_counts()
            for clase in ['Crítico', 'Muy Importante', 'Importante',
                        'Complementario', 'Ocasional']:
                n = int(dist.get(clase, 0))
                if n:
                    print(f'      {clase:<18s} {n:>7,} ({n/len(d):>5.1%})')

        print('\n' + '=' * 80 + '\n')
        return salidas

    resultados_rankings = generar_rankings(
        resultados,
        incluir_aportes=True # aporte en puntos de cada métrica, además del valor normalizado
    )

    df_ranking = resultados_rankings['1. Ranking General']

    df_ranking = df_ranking[[
        'PRODUCT_ID',
        'PRODUCT_DESCRIPTION',
        'CATEGORY_DESCRIPTION',
        'SUB_CATEGORY_DESCRIPTION',
        'NEG_DSC',
        'marca_propia_flag',
        'rank',
        'indice_infaltable',
        'clasificacion',
        'penetracion_norm',
        'penetracion_sensibles_norm',
        'peso_categoria_norm',
        'nielsen_norm',
        'sustitucion_norm',
        'aporte_penetracion',
        'aporte_penetracion_sensibles',
        'aporte_peso_categoria',
        'aporte_nielsen',
        'aporte_sustitucion',
        'origen_frecuencia',
        'origen_sustitucion',
        'VENTA_NETA_PRODUCTO',
        'pct_venta_neta_formato',
        'PVP',
        'FECHA_PRIMERA_VENTA',
        'MESES_CON_VENTA',
        'DIAS_DESDE_PRIMERA_VENTA',
        'ES_PRODUCTO_NUEVO_90D',
        'SEGMENTO_MADUREZ_SKU'
    ]]

    df_ranking['indice_infaltable'] = df_ranking['indice_infaltable'].round(2)
    df_ranking['penetracion_norm'] = df_ranking['penetracion_norm'].round(2)
    df_ranking['penetracion_sensibles_norm'] = df_ranking['penetracion_sensibles_norm'].round(2)
    df_ranking['peso_categoria_norm'] = df_ranking['peso_categoria_norm'].round(2)
    df_ranking['nielsen_norm'] = df_ranking['nielsen_norm'].round(2)
    df_ranking['sustitucion_norm'] = df_ranking['sustitucion_norm'].round(2)
    df_ranking['aporte_penetracion'] = df_ranking['aporte_penetracion'].round(2)
    df_ranking['aporte_penetracion_sensibles'] = df_ranking['aporte_penetracion_sensibles'].round(2)  # noqa: E501
    df_ranking['aporte_peso_categoria'] = df_ranking['aporte_peso_categoria'].round(2)
    df_ranking['aporte_nielsen'] = df_ranking['aporte_nielsen'].round(2)
    df_ranking['aporte_sustitucion'] = df_ranking['aporte_sustitucion'].round(2)
    df_ranking['VENTA_NETA_PRODUCTO'] = df_ranking['VENTA_NETA_PRODUCTO'].astype('int64')
    df_ranking['pct_venta_neta_formato'] = (df_ranking['pct_venta_neta_formato']*100).round(2)
    df_ranking['PVP'] = df_ranking['PVP'].round(0).astype('int64')

    df_ranking['STORE_BANNER'] = store_banner_table
    df_ranking['FECHA_CARGA'] = execution_date

    deleteFromTable(
        table_ref=f'{gcp_project}.GESTION_CATEGORIAS.INFALTABLES',
        where_clause=f"""FECHA_CARGA = '{execution_date}'
            AND store_banner = '{store_banner}'""",
        gbq_client=gbq_client,
    )

    uploadFrame(
        df_ranking,
        table_ddl_json_path=os.path.join('gbq_objects','infaltables.json'),
        project = gcp_project,
        gbq_client = gbq_client,
        if_exists = 'append'
    )

if __name__ == '__main__':
    main()

