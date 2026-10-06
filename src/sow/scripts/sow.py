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

# -------------------------------------------------------------------------
# SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'sow':
    """
    WITH BaseTransacciones AS (
        SELECT
            FIT.CUSTOMER_KEY,
            FIT.TXN_KEY,
            P.material,
            P.GRUPO_DSC,
            P.CAT_DSC,
            P.LIN_DESC,
            P.SEC_DSC,
            P.CAT_H_DSC,
            P.LIN_H_DSC,
            -- Lógica de Unidades
            CASE
                WHEN ((DPH.NEG_ID = '14') OR (DPH.NEG_ID = '15')) AND (DPH.GRUPO_ID <> '210010103') THEN 0
                WHEN (FIT.NBR_PD_ITM = 0 AND FIT.WGHT_ITM > 0 AND FIT.WGHT_ITM IS NOT NULL) THEN 1
                WHEN (FIT.WGHT_ITM < 0 AND FIT.WGHT_ITM IS NOT NULL) THEN -1
                WHEN (FIT.NBR_PD_ITM <> 0 AND P.CONT_CONV_UMB IS NOT NULL) THEN CAST(P.CONT_CONV_UMB AS NUMERIC) * FIT.NBR_PD_ITM
                ELSE FIT.NBR_PD_ITM
            END AS unidades,
            (FIT.ITM_TXN_AMT - COALESCE(FIT.TAX_AMOUNT, 0)) AS VENTA_NETA,
            DD.CALENDAR_YEAR * 100 + DD.CALENDAR_MONTH_NUMBER AS MONTH_ID
        FROM `${gcp_project_cda}.DS_CDA_VW_SMU.DW_VW_FACT_ITM_TXN` FIT

        LEFT JOIN `${gcp_project_cda}.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_E_COMMERCE` e
        ON e.MARKET_BASKET_KEY = FIT.MARKET_BASKET_KEY

        JOIN `${gcp_project_cda}.DS_CDA_VW_SMU.DW_VW_DIM_STORE_HIERARCHY` DSH
        ON DSH.STORE_KEY = FIT.STORE_KEY

        JOIN `${gcp_project_cda}.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT_HIERARCHY` DPH
        ON DPH.PRODUCT_KEY = FIT.PRODUCT_KEY_1

        LEFT JOIN (
            SELECT
                distinct ean,
                ltrim(sku_product,'0') as material,
                CONT_CONV_UMB,
                GRUPO_DSC,
                CAT_DSC,
                LIN_DESC,
                SEC_DSC,
                CAT_H_DSC,
                LIN_H_DSC
            FROM `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT`
        ) P
        ON P.ean = DPH.ean

        JOIN `${gcp_project_cda}.DS_CDA_VW_SMU.DW_VW_DIM_DATE` DD
        ON DD.DATE_KEY = FIT.DATE_KEY

        LEFT JOIN `${gcp_project_unidata}.DS_PROD_CLIENTES_IC.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_OUTLIER_MART` O
        ON O.CUSTOMER_KEY = FIT.CUSTOMER_KEY
        AND O.ORG_IP_ID = DSH.ORG_IP_ID
        AND (DD.CALENDAR_YEAR * 100 + DD.CALENDAR_MONTH_NUMBER) = CAST(
            FORMAT_DATE(
                '%Y%m',
                DATE_ADD(PARSE_DATE('%Y%m', CAST(O.MONTH_ID AS STRING)), INTERVAL -1 MONTH)
            ) AS INT64
        )

        WHERE FIT.ITM_TXN_TMS >= CAST(DATE_SUB('${execution_date}', INTERVAL 1 YEAR) AS DATETIME)
            AND FIT.ITM_TXN_TMS < '${execution_date}'
            AND FIT.ITM_TXN_FCN_TP_DSC = 'V'
            AND FIT.CUSTOMER_KEY IS NOT NULL
            AND FIT.CUSTOMER_HEX NOT IN ('3588d47a76aac91fcf2a3c2f55f6a351')
            AND FIT.FNC_DOC_TP_HEX IN (
                '5756ebdc189492f0ad8e05e633217018', '3ad6ff06d7bc49ae6f05b15354c3af0a',
                'a2f3a5cc2e8b1292bc6629beac500720', '4a209440364b13aa8cd293a37cee6ee1',
                '2fae0e1971b412541215bec30dcedf01', 'cfe71cea05fb5fa5cb5b5f2a72d616af',
                'e784c5e99b4e72f9e4d85a3f244246a9', 'b7ff659d1213e5fe6a36d081943123a2'
            )
            AND DPH.NEG_ID NOT IN ('14', '15')
            AND DSH.STORE_ID NOT IN ('0622')
            AND DSH.ORG_IP IN ('${store_banner}')
            AND O.CUSTOMER_KEY IS NULL
            AND COALESCE(e.CANAL_VENTA, 'SALA') IN ('SALA', 'E-COMMERCE')
            and FIT.ITM_TXN_AMT > 0
    ),

    cruce_ine as (
        SELECT
            bt.*,
            sum(venta_neta) over (partition by customer_key, month_id) as venta_neta_mes,
            sum(unidades) over (partition by customer_key, month_id) as unidades_mes,
            case
                when COALESCE(gr.glosa_producto, cat.glosa_producto, lin.glosa_producto,sec.glosa_producto, cat_h.glosa_producto) is null then 'NO'
                else 'SI'
            end as ES_CANASTA_INE,
            case
                when COALESCE(gr.glosa_producto, cat.glosa_producto, lin.glosa_producto,sec.glosa_producto, cat_h.glosa_producto) is null then 0
                else sum(venta_neta) over (partition by customer_key, month_id,case when COALESCE(gr.glosa_producto, cat.glosa_producto, lin.glosa_producto,sec.glosa_producto, cat_h.glosa_producto) is null then 'NO' else 'SI' end)
            end as Venta_Neta_Canasta_ine,
            COALESCE(gr.glosa_producto, cat.glosa_producto, lin.glosa_producto,sec.glosa_producto, cat_h.glosa_producto) as glosa_producto,
            COALESCE(gr.pond_producto, cat.pond_producto, lin.pond_producto,  sec.pond_producto,cat_h.pond_producto) as pond_producto,
            COALESCE(gr.glosa_division, cat.glosa_division, lin.glosa_division,sec.glosa_division, cat_h.glosa_division) as glosa_division,
            COALESCE(gr.pond_division, cat.pond_division, lin.pond_division,  sec.pond_division,cat_h.pond_division) as pond_division,
            COALESCE(gr.glosa_grupo, cat.glosa_grupo, lin.glosa_grupo,  sec.glosa_grupo,cat_h.glosa_grupo) as glosa_grupo,
            COALESCE(gr.pond_grupo, cat.pond_grupo, lin.pond_grupo, sec.pond_grupo,cat_h.pond_grupo) as pond_grupo,
            COALESCE(gr.glosa_clase, cat.glosa_clase, lin.glosa_clase,sec.glosa_clase, cat_h.glosa_clase) as glosa_clase,
            COALESCE(gr.pond_clase, cat.pond_clase, lin.pond_clase, sec.pond_clase,cat_h.pond_clase) as pond_clase,
            COALESCE(gr.glosa_subclase, cat.glosa_subclase, lin.glosa_subclase,sec.glosa_subclase, cat_h.glosa_subclase) as glosa_subclase,
            COALESCE(gr.pond_subclase, cat.pond_subclase, lin.pond_subclase, sec.pond_subclase, cat_h.pond_subclase) as pond_subclase,
            COALESCE(gr.nivel_homologacion, cat.nivel_homologacion, lin.nivel_homologacion, sec.nivel_homologacion,cat_h.nivel_homologacion) as nivel_homologacion,
            COALESCE(gr.match, cat.match, lin.match, sec.match,cat_h.match) as match,
            COALESCE(gr.pond_canasta, cat.pond_canasta, lin.pond_canasta, sec.pond_canasta,cat_h.pond_canasta) as pond_canasta,
            COALESCE(gr.porc_canasta, cat.porc_canasta, lin.porc_canasta, sec.porc_canasta,cat_h.porc_canasta) as porc_canasta
        FROM BaseTransacciones bt

        LEFT JOIN (
            SELECT
                match,
                glosa_producto,
                pond_producto,
                glosa_division,
                pond_division,
                glosa_grupo,
                pond_grupo,
                glosa_clase,
                pond_clase,
                glosa_subclase,
                pond_subclase,
                nivel_homologacion,
                sum(case when Revision IN ('OK') then pond_producto else 0 end) over (partition by Revision) as pond_canasta,
                pond_producto/sum(case when Revision IN ('OK') then pond_producto else 0 end) over (partition by Revision) as porc_canasta
            FROM `${gcp_project}.ML_LAB.DIM_IPC_PRODUCTO`
            WHERE Revision IN ('OK')
        ) gr
        ON gr.match = bt.GRUPO_DSC
        AND gr.nivel_homologacion = 'GRUPO_DSC'

        LEFT JOIN (
            SELECT
                match,
                glosa_producto,
                pond_producto,
                glosa_division,
                pond_division,
                glosa_grupo,
                pond_grupo,
                glosa_clase,
                pond_clase,
                glosa_subclase,
                pond_subclase,
                nivel_homologacion,
                sum(case when Revision IN ('OK') then pond_producto else 0 end) over (partition by Revision) as pond_canasta,
                pond_producto/sum(case when Revision IN ('OK') then pond_producto else 0 end) over (partition by Revision) as porc_canasta
            FROM `${gcp_project}.ML_LAB.DIM_IPC_PRODUCTO`
            WHERE Revision IN ('OK')
        ) cat
        ON cat.match = bt.CAT_DSC
        AND cat.nivel_homologacion = 'CAT_DSC'

        LEFT JOIN (
            SELECT
                match,
                glosa_producto,
                pond_producto,
                glosa_division,
                pond_division,
                glosa_grupo,
                pond_grupo,
                glosa_clase,
                pond_clase,
                glosa_subclase,
                pond_subclase,
                nivel_homologacion,
                sum(case when Revision IN ('OK') then pond_producto else 0 end) over (partition by Revision) as pond_canasta,
                pond_producto/sum(case when Revision IN ('OK') then pond_producto else 0 end) over (partition by Revision) as porc_canasta
            FROM `${gcp_project}.ML_LAB.DIM_IPC_PRODUCTO`
            WHERE Revision IN ('OK')
        ) lin
        ON lin.match = bt.LIN_DESC
        AND lin.nivel_homologacion = 'LIN_DESC'

        LEFT JOIN (
            SELECT
                match,
                glosa_producto,
                pond_producto,
                glosa_division,
                pond_division,
                glosa_grupo,
                pond_grupo,
                glosa_clase,
                pond_clase,
                glosa_subclase,
                pond_subclase,
                nivel_homologacion,
                sum(case when Revision IN ('OK') then pond_producto else 0 end) over (partition by Revision) as pond_canasta,
                pond_producto/sum(case when Revision IN ('OK') then pond_producto else 0 end) over (partition by Revision) as porc_canasta
            FROM `${gcp_project}.ML_LAB.DIM_IPC_PRODUCTO`
            WHERE Revision IN ('OK')
        ) sec
        ON sec.match = bt.SEC_DSC
        AND sec.nivel_homologacion = 'SEC_DSC'

        LEFT JOIN (
            SELECT
                match,
                glosa_producto,
                pond_producto,
                glosa_division,
                pond_division,
                glosa_grupo,
                pond_grupo,
                glosa_clase,
                pond_clase,
                glosa_subclase,
                pond_subclase,
                nivel_homologacion,
                sum(case when Revision IN ('OK') then pond_producto else 0 end) over (partition by Revision) as pond_canasta,
                pond_producto/sum(case when Revision IN ('OK') then pond_producto else 0 end) over (partition by Revision) as porc_canasta
            FROM `${gcp_project}.ML_LAB.DIM_IPC_PRODUCTO`
            WHERE Revision IN ('OK')
        ) cat_h
        ON cat_h.match = upper(bt.CAT_H_DSC)
        AND cat_h.nivel_homologacion = 'CAT_H_DSC'
    ),

    canasta_mensual as (
        SELECT
            ES_CANASTA_INE,
            glosa_producto,
            pond_producto,
            glosa_division,
            pond_division,
            glosa_grupo,
            pond_grupo,
            glosa_clase,
            pond_clase,
            glosa_subclase,
            pond_subclase,
            nivel_homologacion,
            match,
            pond_canasta,porc_canasta,
            MONTH_ID,
            customer_key,
            unidades_mes,
            venta_neta_mes,
            Venta_Neta_Canasta_ine,
            sum(unidades) as unidades,
            sum(venta_neta) as venta_neta
        FROM cruce_ine
        GROUP BY
            ES_CANASTA_INE,
            glosa_producto,
            pond_producto,
            glosa_division,
            pond_division,
            glosa_grupo,
            pond_grupo,
            glosa_clase,
            pond_clase,
            glosa_subclase,
            pond_subclase,
            nivel_homologacion,
            match,
            pond_canasta,
            porc_canasta,
            MONTH_ID,
            customer_key,
            unidades_mes,
            venta_neta_mes,
            Venta_Neta_Canasta_ine
    ),

    tabla_cliente_mes as (
        SELECT
            MONTH_ID,
            customer_key,
            unidades_mes,
            venta_neta_mes,

            -- 1. Venta Total en Categorías homologadas por el INE
            max(Venta_Neta_Canasta_ine) as Venta_Neta_Canasta_ine,

            -- 2. Venta de productos que NO pertenecen a la canasta INE
            sum(case when ES_CANASTA_INE = 'NO' then venta_neta else 0 end) as venta_neta_no_canasta,

            -- 3. Porcentaje total de la canasta INE que el cliente compró (cobertura)
            sum(porc_canasta) as porc_canasta,

            -- 4. ESTIMACIÓN ROBUSTA (Método de Cobertura Global)
            -- Si gastó Venta_Neta_Canasta_ine en categorías que representan porc_canasta de la canasta básica,
            -- estimamos de forma estable el costo total de la canasta para este cliente.
            safe_divide(max(Venta_Neta_Canasta_ine), sum(porc_canasta)) as estimacion_canasta_basica
        FROM canasta_mensual
        GROUP BY MONTH_ID, customer_key, unidades_mes, venta_neta_mes
    ),

    -- Base mensual por cliente
    base AS (
        SELECT
            customer_key,
            MONTH_ID,
            unidades_mes,
            venta_neta_mes,
            Venta_Neta_Canasta_ine,
            venta_neta_no_canasta,
            porc_canasta,
            estimacion_canasta_basica,
            -- Gasto total Unimarc (canasta + no canasta)
            Venta_Neta_Canasta_ine + venta_neta_no_canasta AS venta_total_unimarc,
            -- Share of wallet mensual
            SAFE_DIVIDE(Venta_Neta_Canasta_ine, estimacion_canasta_basica) AS sow_mes,
            -- % no canasta sobre ticket
            SAFE_DIVIDE(venta_neta_no_canasta, venta_neta_mes) AS pct_no_canasta_mes
        FROM tabla_cliente_mes
    ),

    -- Promedios anuales por cliente (suma / meses en que compró)
    resumen_cliente AS (
        SELECT
            customer_key,
            -- Meses activos
            COUNT(DISTINCT MONTH_ID) AS meses_activos,

            -- Volumen
            SUM(unidades_mes) AS unidades_total,
            SAFE_DIVIDE(SUM(unidades_mes), COUNT(DISTINCT MONTH_ID)) AS unidades_prom_mes,

            -- Venta total Unimarc
            SUM(venta_total_unimarc) AS venta_unimarc_total,
            SAFE_DIVIDE(SUM(venta_total_unimarc), COUNT(DISTINCT MONTH_ID)) AS venta_unimarc_prom_mes,

            -- Venta canasta básica Unimarc
            SUM(Venta_Neta_Canasta_ine) AS venta_canasta_total,
            SAFE_DIVIDE(SUM(Venta_Neta_Canasta_ine), COUNT(DISTINCT MONTH_ID)) AS venta_canasta_prom_mes,

            -- Venta no canasta Unimarc
            SUM(venta_neta_no_canasta) AS venta_no_canasta_total,
            SAFE_DIVIDE(SUM(venta_neta_no_canasta), COUNT(DISTINCT MONTH_ID)) AS venta_no_canasta_prom_mes,

            -- Estimación gasto canasta básica total (todos los supermercados)
            SUM(estimacion_canasta_basica) as estimacion_canasta_total,
            SAFE_DIVIDE(SUM(estimacion_canasta_basica), COUNT(DISTINCT MONTH_ID)) AS estimacion_canasta_prom_mes,

            -- Share of Wallet promedio (suma canasta Unimarc / suma estimación)
            SAFE_DIVIDE(SUM(Venta_Neta_Canasta_ine), SUM(estimacion_canasta_basica)) AS sow_anual,

            -- % no canasta sobre ticket Unimarc (promedio ponderado por venta)
            SAFE_DIVIDE(SUM(venta_neta_no_canasta), SUM(venta_total_unimarc)) AS pct_no_canasta_prom

            -- Frecuencia: tickets por mes activo (si tienes n_tickets en tu tabla agrégala)
            -- SAFE_DIVIDE(SUM(n_tickets), COUNT(DISTINCT MONTH_ID))  AS tickets_prom_mes,
        FROM base
        GROUP BY customer_key
    ),

    -- 5. Asignación de quintiles para cada cliente
    clientes_con_quintil AS (
        SELECT
            customer_key,
            meses_activos,
            unidades_total,
            venta_unimarc_total,
            venta_unimarc_prom_mes,
            venta_canasta_total,
            venta_no_canasta_total,
            venta_canasta_prom_mes,
            estimacion_canasta_total,
            estimacion_canasta_prom_mes,
            sow_anual,
            pct_no_canasta_prom,
            -- Quintil para estimacion_canasta_prom_mes
            NTILE(5) OVER(ORDER BY estimacion_canasta_prom_mes ASC) AS quintil_canasta_prom_mes,
            -- Quintil para venta_unimarc_prom_mes
            NTILE(10) OVER(ORDER BY venta_unimarc_prom_mes ASC) AS decil_venta_prom_mes
        FROM resumen_cliente
    ),

    -- 6. Obtención de los límites mínimos y máximos por cada tipo de quintil
    clientes_con_quintil_2 AS (
        SELECT
            *,
            case
                when decil_venta_prom_mes in (1,2,3,4,5) then 1
                when decil_venta_prom_mes in (6,7) then 2
                when decil_venta_prom_mes in (8) then 3
                when decil_venta_prom_mes in (9) then 4
                when decil_venta_prom_mes in (10) then 5
            end as quintil_venta_prom_mes
        FROM clientes_con_quintil
    ),

    -- 6. Obtención de los límites mínimos y máximos por cada tipo de quintil
    limites_quintil AS (
        SELECT
            *,

            -- Límites para canasta básica
            MIN(estimacion_canasta_prom_mes) OVER(PARTITION BY quintil_canasta_prom_mes) AS min_q_canasta,
            MAX(estimacion_canasta_prom_mes) OVER(PARTITION BY quintil_canasta_prom_mes) AS max_q_canasta,

            -- Límites para venta unimarc
            MIN(venta_unimarc_prom_mes) OVER(PARTITION BY quintil_venta_prom_mes) AS min_q_venta,
            MAX(venta_unimarc_prom_mes) OVER(PARTITION BY quintil_venta_prom_mes) AS max_q_venta
        FROM clientes_con_quintil_2
    )

    -- 7. Selección final formateando los límites
    SELECT
        customer_key,
        meses_activos,
        unidades_total,
        venta_unimarc_total,
        venta_unimarc_prom_mes,
        venta_canasta_total,
        venta_no_canasta_total,
        venta_canasta_prom_mes,
        estimacion_canasta_total,
        estimacion_canasta_prom_mes,
        sow_anual,
        estimacion_canasta_total-venta_canasta_total as gasto_por_capturar,
        pct_no_canasta_prom,

        -- Segmento 1: Estimación Canasta Básica
        quintil_canasta_prom_mes,
        CASE quintil_canasta_prom_mes
            WHEN 1 THEN CONCAT('1. Quintil 1 (<= $$', CAST(ROUND(max_q_canasta / 1000, 0) AS INT64), 'k)')
            WHEN 2 THEN CONCAT('2. Quintil 2 ($$', CAST(ROUND(min_q_canasta / 1000, 0) AS INT64), 'k - $$', CAST(ROUND(max_q_canasta / 1000, 0) AS INT64), 'k)')
            WHEN 3 THEN CONCAT('3. Quintil 3 ($$', CAST(ROUND(min_q_canasta / 1000, 0) AS INT64), 'k - $$', CAST(ROUND(max_q_canasta / 1000, 0) AS INT64), 'k)')
            WHEN 4 THEN CONCAT('4. Quintil 4 ($$', CAST(ROUND(min_q_canasta / 1000, 0) AS INT64), 'k - $$', CAST(ROUND(max_q_canasta / 1000, 0) AS INT64), 'k)')
            WHEN 5 THEN CONCAT('5. Quintil 5 (> $$', CAST(ROUND(min_q_canasta / 1000, 0) AS INT64), 'k)')
        END AS rango_gasto_canasta_hogar,

        -- Segmento 2: Venta Unimarc Promedio
        quintil_venta_prom_mes,
        CASE quintil_venta_prom_mes
            WHEN 1 THEN CONCAT('1. Deciles 1-5 (<= $$', CAST(ROUND(max_q_venta / 1000, 0) AS INT64), 'k)')
            WHEN 2 THEN CONCAT('2. Deciles 6-7 ($$', CAST(ROUND(min_q_venta / 1000, 0) AS INT64), 'k - $$', CAST(ROUND(max_q_venta / 1000, 0) AS INT64), 'k)')
            WHEN 3 THEN CONCAT('3. Decil 8 ($$', CAST(ROUND(min_q_venta / 1000, 0) AS INT64), 'k - $$', CAST(ROUND(max_q_venta / 1000, 0) AS INT64), 'k)')
            WHEN 4 THEN CONCAT('4. Decil 9 ($$', CAST(ROUND(min_q_venta / 1000, 0) AS INT64), 'k - $$', CAST(ROUND(max_q_venta / 1000, 0) AS INT64), 'k)')
            WHEN 5 THEN CONCAT('5. Decil 10 (> $$', CAST(ROUND(min_q_venta / 1000, 0) AS INT64), 'k)')
        END AS segmento_gasto_unimarc,

        -- Eje 2: Share of Wallet en canasta básica
            CASE
                WHEN sow_anual IS NULL THEN '5. Sin datos'
                WHEN sow_anual >= 0.70 THEN '1. Alto Sow (>= 70%)'
                WHEN sow_anual >= 0.40 THEN '2. Medio Sow (40% - 70%)'
                WHEN sow_anual >= 0.15 THEN '3. Bajo Sow (15% - 40%)'
                WHEN sow_anual < 0.15  THEN '4. Muy Bajo SoW (< 15%)'
            END AS segmento_sow,

            -- Eje 3: Tipo de canasta en Unimarc
            CASE
                WHEN pct_no_canasta_prom <= 0.30 THEN '1. Principalmente Canasta Básica (<= 30%)'
                WHEN pct_no_canasta_prom <= 0.50 THEN '2. Canasta Mixta (30% - 50%)'
                WHEN pct_no_canasta_prom > 0.50  THEN '3. Principalmente Necesaria / Prescindible (> 50%)'
                ELSE '4. Sin datos'
            END AS segmento_tipo_canasta,

            -- Eje 4: Recurrencia
            CASE
                WHEN meses_activos >= 10 THEN '1. Recurrente (10-12 meses)'
                WHEN meses_activos >= 6  THEN '2. Habitual (6-9 meses)'
                WHEN meses_activos >= 3  THEN '3. Ocasional (3-5 meses)'
                ELSE                          '4. Esporádico (1-2 meses)'
            END AS segmento_recurrencia
    FROM limites_quintil
    """  # noqa: E501
})


# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------

def main() -> None:
    usuario = 'sow'
    # parse input variables
    args = vars(parser.parse_args())
    gcp_project: str = args['project_id']
    execution_date: str = args['execution_date']
    store_banner: str = args['store_banner']

    execution_date = pd.to_datetime(execution_date[:8] + '01').strftime('%Y-%m-%d')
    gcp_project_cda  = 'cl-cda-prod'
    gcp_project_unidata = 'cl-cda-unidata-prod'

    logging.info(f'execution_date: {execution_date}')
    logging.info(f'store_banner: {store_banner}')

    # Set gbq client for all subsequent queries
    gbq_client = Client()

    logging.info('Ejecucion Query sow')
    sow = readBigQuery(SQL_QUERIES['sow'].substitute(
        gcp_project_cda = gcp_project_cda,
        gcp_project_unidata = gcp_project_unidata,
        gcp_project = gcp_project,
        store_banner = store_banner,
        execution_date = execution_date
    ),
    user = usuario,
    gbq_client = gbq_client
    )

    logging.info('Transformacion tipo datos df sow')
    sow['unidades_total'] = sow['unidades_total'].astype('Int64')
    sow['venta_unimarc_total'] = sow['venta_unimarc_total'].astype('Int64')
    sow['venta_unimarc_prom_mes'] = np.ceil(sow['venta_unimarc_prom_mes']).astype('Int64')
    sow['venta_canasta_total'] = sow['venta_canasta_total'].astype('Int64')
    sow['venta_no_canasta_total'] = sow['venta_no_canasta_total'].astype('Int64')
    sow['venta_canasta_prom_mes'] = np.ceil(sow['venta_canasta_prom_mes']).astype('Int64')
    sow['estimacion_canasta_total'] = np.ceil(sow['estimacion_canasta_total']).astype('Int64')
    sow['estimacion_canasta_prom_mes'] = np.ceil(sow['estimacion_canasta_prom_mes']).astype('Int64')  # noqa: E501
    sow['gasto_por_capturar'] = np.ceil(sow['gasto_por_capturar']).astype('Int64')

    sow['store_banner'] = store_banner
    sow['fecha_carga'] = '2026-10-01'

    logging.info('Ingesta de datos')
    deleteFromTable(
        table_ref=f'{gcp_project}.CONOCIMIENTO_CLIENTE.SEGMENTACION_CLIENTE_SOW',
        where_clause=f"""FECHA_CARGA = '{execution_date}'
            AND store_banner = '{store_banner}'""",
        gbq_client=gbq_client,
    )

    uploadFrame(
        sow,
        table_ddl_json_path=os.path.join('gbq_objects','sow.json'),
        project = gcp_project,
        gbq_client = gbq_client,
        if_exists = 'append'
    )

if __name__ == '__main__':
    main()


