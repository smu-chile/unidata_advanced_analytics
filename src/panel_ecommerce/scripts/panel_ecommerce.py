# Default
from __future__ import annotations

import logging
import argparse
from logging import config

# Pip
from google.cloud import bigquery  # noqa: F401
from google.cloud.bigquery import Client

# Own
import common.gcp_extended.bigquery as gbq_extended  # noqa: F401
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import createTableAsSelect


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


# -------------------------------------------------------------------------
# SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'transacciones_omnicanal':
    """
    SELECT
    A.TXN_KEY AS TXN_KEY,
    A.MARKET_BASKET_KEY,
    A.ITM_TXN_FCN_TP_DSC,
    STORE_ID,
    ORG_IP as Store_Banner,
    coalesce(e_comm.CANAL_VENTA,'SALA') as CANAL_VENTA,
        DATE(A.ITM_TXN_TMS) AS TRANSACTION_DATE,
        date(A.CONT_DATE_VALUE) AS FECHA_CONTABLE,
        TIME(A.ITM_TXN_TMS) AS TRANSACTION_TIME,DA.CALENDAR_YEAR,DA.CALENDAR_MONTH_NUMBER ,
        concat(CAST(DA.CALENDAR_YEAR as STRING), CASE
        WHEN DA.CALENDAR_MONTH_NUMBER < 10 THEN concat('0', CAST( DA.CALENDAR_MONTH_NUMBER as STRING))
        ELSE CAST( DA.CALENDAR_MONTH_NUMBER as STRING)
        END) AS MES,
    D.SKU_PRODUCT AS SKU_PRODUCT,
    C.EAN AS EAN,
    --UNIT PRIZE--
    ROUND(
            SUM(CASE
                WHEN (((D.NEG_ID = '14') OR (D.NEG_ID = '15')) AND (D.GRUPO_ID <> '210010103')) THEN 0 --estos productos no cuentan
                WHEN (A.WGHT_ITM <> 0 AND A.WGHT_ITM IS NOT NULL) THEN A.ITM_TXN_AMT/(A.WGHT_ITM/1000) --si no tiene nbr pd itm y tiene peso
                --WHEN ((A.WGHT_ITM < 0) AND (A.WGHT_ITM IS NOT NULL)) THEN -1
                WHEN (A.NBR_PD_ITM <> 0 and C.CONT_CONV_UMB IS NOT NULL) THEN  A.ITM_TXN_AMT /(CAST(C.CONT_CONV_UMB AS NUMERIC)  * A.NBR_PD_ITM)
                WHEN (A.NBR_PD_ITM <> 0 and C.CONT_CONV_UMB IS NULL) THEN  A.ITM_TXN_AMT/A.NBR_PD_ITM
        ELSE 0
            END),
        2) AS UNIT_PRICE,

        CASE -- ITEM_QTY_UMB => QUANTITY =>
            WHEN (((D.NEG_ID = '14') OR (D.NEG_ID = '15')) AND (D.GRUPO_ID <> '210010103')) THEN 0 --estos productos no cuentan
            WHEN (A.NBR_PD_ITM = 0 and A.WGHT_ITM > 0 AND A.WGHT_ITM IS NOT NULL) THEN 1
            --si no tiene nbr pd itm y tiene peso
    WHEN ((A.WGHT_ITM < 0) AND (A.WGHT_ITM IS NOT NULL)) THEN -1
            --si tiene nbr pd itm y hay un conv entonces es cont_conv_umb * nbr_pd_itm
            WHEN (A.NBR_PD_ITM <> 0 and C.CONT_CONV_UMB IS NOT NULL) THEN CAST(C.CONT_CONV_UMB AS NUMERIC)  * A.NBR_PD_ITM
            ELSE A.NBR_PD_ITM
        END AS QUANTITY,
    SUM(CASE
            WHEN (((D.NEG_ID = '14') OR (D.NEG_ID = '15')) AND (D.GRUPO_ID <> '210010103')) THEN 0
            ELSE A.ITM_TXN_AMT
        END) AS VALUE,
        SUM(CASE
            WHEN (A.WGHT_ITM IS NOT NULL) THEN A.WGHT_ITM / 1000
            when CONTENIDO_BRUTO is not null then cast(CONTENIDO_BRUTO as numeric)*A.NBR_PD_ITM
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
        --QUANTITY_SU
        CASE
            WHEN (((D.NEG_ID = '14') OR (D.NEG_ID = '15')) AND (D.GRUPO_ID <> '210010103')) THEN 0 --estos productos no cuentan
            WHEN (A.NBR_PD_ITM = 0 and A.WGHT_ITM > 0 AND A.WGHT_ITM IS NOT NULL) THEN 1 --si no tiene nbr pd itm y tiene peso
            WHEN ((A.WGHT_ITM < 0) AND (A.WGHT_ITM IS NOT NULL)) THEN -1
                --si tiene nbr pd itm y hay un conv entonces es cont_conv_umb * nbr_pd_itm
            WHEN (A.NBR_PD_ITM <> 0 and C.CONT_CONV_UMB IS NOT NULL) THEN CAST(C.CONT_CONV_UMB AS NUMERIC)  * A.NBR_PD_ITM / (coalesce(C.UMREZ,1) / coalesce(C.UMREN,1))
            ELSE A.NBR_PD_ITM
        END AS QUANTITY_SU,
        SUM(TAX_AMOUNT) AS TAX_AMOUNT,
        supplier_nm,
        supplier_id,
        grupo_id,
        grupo_dsc,
        cat_id,
        cat_dsc,
        sec_dsc,
        lin_desc,
        NEG_ID,
        NEG_DSC,
        brand_id,
        brand_desc

    FROM (
        `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_ITM_TXN` A
        LEFT OUTER JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_DATE` AS DA ON DATE(A.ITM_TXN_TMS)=da.date_value

        JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE_HIERARCHY` B
        ON (
            (A.STORE_KEY = B.STORE_KEY)
            AND (B.ORG_IP_ID IN ('01','08'))
        )
        LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_E_COMMERCE` e_comm
        USING(market_basket_key)
        JOIN (
            SELECT distinct
                PRODUCT_KEY,
                EAN,
                CONT_CONV_UMB,
                UNIDAD_DE_MEDIDA,
                CONTENIDO_BRUTO,
                UM_CONTENIDO,
                CAST(CONT_CONV_UMB AS NUMERIC) as UMREZ,
                CAST(DENOM_UMB AS NUMERIC) as UMREN
            FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT`
        ) C
        ON A.PRODUCT_KEY_1  = C.PRODUCT_KEY

        JOIN (
            SELECT distinct
                PRODUCT_KEY,
                SKU_PRODUCT,
                grupo_id,
                grupo_dsc,
                cat_id,
                cat_dsc,
                sec_dsc,
                lin_desc,
                NEG_ID,
                NEG_DSC

            FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT_HIERARCHY`
        ) D
        ON A.PRODUCT_KEY_1  = D.PRODUCT_KEY

        JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_FIN_DOC_TP_TYPE` E
        ON A.FNC_DOC_TP_KEY  = E.FIN_DOC_TP_KEY
        left JOIN (
        SELECT  distinct
                sku_product,
                supplier_nm,
                supplier_id,
                brand_id,
                brand_desc
                FROM
                `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_HIERARCHY` AS dim_sku
                INNER JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SUPPLIER` AS S_0 ON S_0.SUPPLIER_KEY = dim_sku.PROVEEDOR_PPAL_KEY
                GROUP BY 1, 2, 3, 4, 5
        ) sup USING (sku_product)
    )
    WHERE --CONT_DATE_VALUE
    A.ITM_TXN_TMS>='2024-12-01' and

    (
        (EXTRACT(YEAR FROM A.CONT_DATE_VALUE) = EXTRACT(YEAR FROM CURRENT_DATE())
        AND EXTRACT(MONTH FROM A.CONT_DATE_VALUE) <= EXTRACT(MONTH FROM CURRENT_DATE())
        )
        OR
        (EXTRACT(YEAR FROM A.CONT_DATE_VALUE) = EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR))
        AND EXTRACT(MONTH FROM A.CONT_DATE_VALUE) <= EXTRACT(MONTH FROM CURRENT_DATE())
        )
    )
    /*(
        (EXTRACT(YEAR FROM A.ITM_TXN_TMS) = EXTRACT(YEAR FROM CURRENT_DATE())
        AND EXTRACT(MONTH FROM A.ITM_TXN_TMS) <= EXTRACT(MONTH FROM CURRENT_DATE())
        )
        OR
        (EXTRACT(YEAR FROM A.ITM_TXN_TMS) = EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR))
        AND EXTRACT(MONTH FROM A.ITM_TXN_TMS) <= EXTRACT(MONTH FROM CURRENT_DATE())
        )
    )*/

        AND E.FNC_DOC_TP_DSC in ('BX','B','BE','FE','F','FX','NE')
        --and (N_COTIZA like '5000%' and e_comm.CANAL_VENTA="E-COMMERCE")--ECOMMERCE
        /*ANd CASE
            WHEN (E.FNC_DOC_TP_DSC = 'NE') THEN 'TN'
            WHEN ((E.FNC_DOC_TP_DSC = 'FE') OR (E.FNC_DOC_TP_DSC = 'FX')) THEN 'TF'
            ELSE E.FNC_DOC_TP_DSC
        END IN ('TN', 'TF', 'BX', 'B ', 'BE', 'F ', 'NC')*/
        AND A.itm_txn_fcn_tp_dsc = 'V'
        and sku_product not in ('000000000000630792','000000000000669484','000000000000669484','000000000000669485') --Membresias
        -- AND B.STORE_ID NOT IN ('0622')
        AND NEG_ID NOT IN ('14','15')
        --AND D.GRUPO_ID <> '210010103'
    GROUP BY DA.CALENDAR_YEAR,  DA.CALENDAR_MONTH_NUMBER,
        D.NEG_ID,
        D.GRUPO_ID,
        A.NBR_PD_ITM,
        C.CONT_CONV_UMB,
        A.WGHT_ITM,
        A.ITM_TXN_TMS,
        A.CONT_DATE_VALUE,
        A.TXN_KEY,
        B.STORE_ID,
        B.ORG_IP,
        e_comm.CANAL_VENTA,
        D.SKU_PRODUCT,
        C.EAN,
        A.MARKET_BASKET_KEY,
        A.ITM_TXN_FCN_TP_DSC,
        C.UNIDAD_DE_MEDIDA,
        C.UMREZ,
        C.UMREN,
        A.CUSTOMER_KEY,
        E.FNC_DOC_TP_DSC,
        supplier_nm,
        supplier_id,
        grupo_id,
        grupo_dsc,
        cat_id,
        cat_dsc,
        sec_dsc,
        lin_desc,
        brand_id,
        brand_desc,
        NEG_DSC

        --A.TAX_AMOUNT
    """, # noqa: E501

    'transacciones_omnicanal_hist':
    """
    SELECT
    A.TXN_KEY AS TXN_KEY,
    A.MARKET_BASKET_KEY,
    A.ITM_TXN_FCN_TP_DSC,
    STORE_ID,
    ORG_IP as Store_Banner,
    coalesce(e_comm.CANAL_VENTA,'SALA') as CANAL_VENTA,
        DATE(A.ITM_TXN_TMS) AS TRANSACTION_DATE,
        TIME(A.ITM_TXN_TMS) AS TRANSACTION_TIME,DA.CALENDAR_YEAR,DA.CALENDAR_MONTH_NUMBER ,
        concat(CAST(DA.CALENDAR_YEAR as STRING), CASE
        WHEN DA.CALENDAR_MONTH_NUMBER < 10 THEN concat('0', CAST( DA.CALENDAR_MONTH_NUMBER as STRING))
        ELSE CAST( DA.CALENDAR_MONTH_NUMBER as STRING)
        END) AS MES,
    D.SKU_PRODUCT AS SKU_PRODUCT,
    C.EAN AS EAN,
    --UNIT PRIZE--
    ROUND(
            SUM(CASE
                WHEN (((D.NEG_ID = '14') OR (D.NEG_ID = '15')) AND (D.GRUPO_ID <> '210010103')) THEN 0 --estos productos no cuentan
                WHEN (A.WGHT_ITM <> 0 AND A.WGHT_ITM IS NOT NULL) THEN A.ITM_TXN_AMT/(A.WGHT_ITM/1000) --si no tiene nbr pd itm y tiene peso
                --WHEN ((A.WGHT_ITM < 0) AND (A.WGHT_ITM IS NOT NULL)) THEN -1
                WHEN (A.NBR_PD_ITM <> 0 and C.CONT_CONV_UMB IS NOT NULL) THEN  A.ITM_TXN_AMT /(CAST(C.CONT_CONV_UMB AS NUMERIC)  * A.NBR_PD_ITM)
                WHEN (A.NBR_PD_ITM <> 0 and C.CONT_CONV_UMB IS NULL) THEN  A.ITM_TXN_AMT/A.NBR_PD_ITM
        ELSE 0
            END),
        2) AS UNIT_PRICE,

        CASE -- ITEM_QTY_UMB => QUANTITY =>
            WHEN (((D.NEG_ID = '14') OR (D.NEG_ID = '15')) AND (D.GRUPO_ID <> '210010103')) THEN 0      --estos productos no cuentan
            WHEN (A.NBR_PD_ITM = 0 and A.WGHT_ITM > 0 AND A.WGHT_ITM IS NOT NULL) THEN 1
            --si no tiene nbr pd itm y tiene peso
    WHEN ((A.WGHT_ITM < 0) AND (A.WGHT_ITM IS NOT NULL)) THEN -1
            --si tiene nbr pd itm y hay un conv entonces es cont_conv_umb * nbr_pd_itm
            WHEN (A.NBR_PD_ITM <> 0 and C.CONT_CONV_UMB IS NOT NULL) THEN CAST(C.CONT_CONV_UMB AS NUMERIC)  * A.NBR_PD_ITM
            ELSE A.NBR_PD_ITM
        END AS QUANTITY,
    SUM(CASE
            WHEN (((D.NEG_ID = '14') OR (D.NEG_ID = '15')) AND (D.GRUPO_ID <> '210010103')) THEN 0
            ELSE A.ITM_TXN_AMT
        END) AS VALUE,
        SUM(CASE
            WHEN (A.WGHT_ITM IS NOT NULL) THEN A.WGHT_ITM / 1000
            when CONTENIDO_BRUTO is not null then cast(CONTENIDO_BRUTO as numeric)*A.NBR_PD_ITM
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
        --QUANTITY_SU
        CASE
            WHEN (((D.NEG_ID = '14') OR (D.NEG_ID = '15')) AND (D.GRUPO_ID <> '210010103')) THEN 0 --estos productos no cuentan
            WHEN (A.NBR_PD_ITM = 0 and A.WGHT_ITM > 0 AND A.WGHT_ITM IS NOT NULL) THEN 1 --si no tiene nbr pd itm y tiene peso
            WHEN ((A.WGHT_ITM < 0) AND (A.WGHT_ITM IS NOT NULL)) THEN -1
                --si tiene nbr pd itm y hay un conv entonces es cont_conv_umb * nbr_pd_itm
            WHEN (A.NBR_PD_ITM <> 0 and C.CONT_CONV_UMB IS NOT NULL) THEN CAST(C.CONT_CONV_UMB AS NUMERIC)  * A.NBR_PD_ITM / (coalesce(C.UMREZ,1) / coalesce(C.UMREN,1))
            ELSE A.NBR_PD_ITM
        END AS QUANTITY_SU,
        SUM(TAX_AMOUNT) AS TAX_AMOUNT,
        supplier_nm,
        supplier_id,
        grupo_id,
        grupo_dsc,
        cat_id,
        cat_dsc,
        sec_dsc,
        lin_desc,
        NEG_ID,
        NEG_DSC,
        brand_id,
        brand_desc

    FROM (
        `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_ITM_TXN` A
        LEFT OUTER JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_DATE` AS DA ON DATE(A.ITM_TXN_TMS)=da.date_value

        JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE_HIERARCHY` B
        ON (
            (A.STORE_KEY = B.STORE_KEY)
            AND (B.ORG_IP_ID IN ('01','08'))
        )
        LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_E_COMMERCE` e_comm
        USING(market_basket_key)
        JOIN (
            SELECT
                PRODUCT_KEY,
                EAN,
                CONT_CONV_UMB,
                UNIDAD_DE_MEDIDA,
                CONTENIDO_BRUTO,
                UM_CONTENIDO,
                CAST(CONT_CONV_UMB AS NUMERIC) as UMREZ,
                CAST(DENOM_UMB AS NUMERIC) as UMREN
            FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT`
        ) C
        ON A.PRODUCT_KEY_1  = C.PRODUCT_KEY

        JOIN (
            SELECT distinct
                PRODUCT_KEY,
                SKU_PRODUCT,
                grupo_id,
                grupo_dsc,
                cat_id,
                cat_dsc,
                sec_dsc,
                lin_desc,
                NEG_ID,
                NEG_DSC

            FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT_HIERARCHY`
        ) D
        ON A.PRODUCT_KEY_1  = D.PRODUCT_KEY

        JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_FIN_DOC_TP_TYPE` E
        ON A.FNC_DOC_TP_KEY  = E.FIN_DOC_TP_KEY
        left JOIN (
        SELECT  distinct
                sku_product,
                supplier_nm,
                supplier_id,
                brand_id,
                brand_desc
                FROM
                `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_HIERARCHY` AS dim_sku
                INNER JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SUPPLIER` AS S_0 ON S_0.SUPPLIER_KEY = dim_sku.PROVEEDOR_PPAL_KEY
                GROUP BY 1, 2, 3, 4, 5
        ) sup USING (sku_product)
    )
    WHERE DATE(A.ITM_TXN_TMS) >=   DATE_SUB(
            DATE_TRUNC(CURRENT_DATE('America/Santiago'), MONTH),
            INTERVAL 26 MONTH)


        AND CASE
            WHEN (E.FNC_DOC_TP_DSC = 'NE') THEN 'TN'
            WHEN ((E.FNC_DOC_TP_DSC = 'FE') OR (E.FNC_DOC_TP_DSC = 'FX')) THEN 'TF'
            ELSE E.FNC_DOC_TP_DSC
        END IN ('TN', 'TF', 'BX', 'B ', 'BE', 'F ', 'NC')
        AND itm_txn_fcn_tp_dsc = 'V'
        and sku_product not in ('000000000000630792','000000000000669484','000000000000669484','000000000000669485')

    GROUP BY DA.CALENDAR_YEAR,  DA.CALENDAR_MONTH_NUMBER,
        D.NEG_ID,
        D.GRUPO_ID,
        A.NBR_PD_ITM,
        C.CONT_CONV_UMB,
        A.WGHT_ITM,
        A.ITM_TXN_TMS,
        A.TXN_KEY,
        B.STORE_ID,
        B.ORG_IP,
        e_comm.CANAL_VENTA,
        D.SKU_PRODUCT,
        C.EAN,
        A.MARKET_BASKET_KEY,
        A.ITM_TXN_FCN_TP_DSC,
        C.UNIDAD_DE_MEDIDA,
        C.UMREZ,
        C.UMREN,
        A.CUSTOMER_KEY,
        E.FNC_DOC_TP_DSC,
        supplier_nm,
        supplier_id,
        grupo_id,
        grupo_dsc,
        cat_id,
        cat_dsc,
        sec_dsc,
        lin_desc,
        brand_id,
        brand_desc,
        NEG_DSC
    """, # noqa: E501

    'transacciones_mensuales_por_canal_formato':
    """
    WITH
        -- 1. Obtenemos la base de combinaciones únicas sin locales
        locales_agg AS (
            SELECT DISTINCT
            Formato,
            CANAL_VENTA,
            CALENDAR_YEARMONTH,
            MONTH_DATE,
            CALENDAR_YEAR,
            CALENDAR_YEAR - 1 AS CALENDAR_YEAR_AA,
            CALENDAR_MONTH_NUMBER
            FROM
            `cl-cda-unidata-dev.DS_UNIDATA_ECOMMERCE.VW_LOCAL_MES_CANAL`
        ),

        -- 2. Calculamos las ventas del año actual, agrupando sin STORE_ID
        ventas_actual AS (
            SELECT
            Store_Banner AS Formato,
            CANAL_VENTA,
            MES,
            CALENDAR_YEAR,
            CALENDAR_MONTH_NUMBER,
            SUM(value) AS Venta_bruta,
            SUM(value - TAX_AMOUNT) AS Venta_neta,
            COUNT(DISTINCT customer_key) AS clientes,
            COUNT(DISTINCT TXN_KEY) AS transacciones,
            SUM(QUANTITY) AS Unidades,
            MAX(TRANSACTION_DATE) AS Fecha_Actualizacion
            FROM
            cl-bigdata-analytics-preprod.ECOMMERCE.TRANSACCIONES_OMNICANAL
            WHERE
            CALENDAR_YEAR >= 2026
            GROUP BY
            Store_Banner,
            CANAL_VENTA,
            MES,
            CALENDAR_YEAR,
            CALENDAR_MONTH_NUMBER
        ),

        -- 3. Calculamos las ventas del período comparable del año anterior, agrupando sin STORE_ID
        ventas_aa AS (
            SELECT
            Store_Banner AS Formato,
            CANAL_VENTA,
            MES,
            CALENDAR_YEAR,
            CALENDAR_MONTH_NUMBER,
            MIN(TRANSACTION_DATE) AS Inicio_periodo_comparable_AA,
            MAX(TRANSACTION_DATE) AS Fin_periodo_comparable_AA,
            SUM(value) AS Venta_bruta,
            SUM(value - TAX_AMOUNT) AS Venta_neta,
            COUNT(DISTINCT customer_key) AS clientes,
            COUNT(DISTINCT TXN_KEY) AS transacciones,
            SUM(QUANTITY) AS Unidades
            FROM
            cl-bigdata-analytics-preprod.ECOMMERCE.TRANSACCIONES_OMNICANAL AS o
            LEFT JOIN `cl-cda-unidata-dev.DS_UNIDATA_ECOMMERCE.VW_PERIODO_COMPARABLE_AA` AS aa ON aa.Fecha = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            WHERE
            -- Año Anterior completo hasta ultimo mes cerrado del año en curso
            (
                TRANSACTION_DATE >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()) - 1, 1, 1)
                AND TRANSACTION_DATE < DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH)
            )
            OR -- Periodo comparable año anterior de mes en curso
            (
                TRANSACTION_DATE >= aa.fecha_inicio_AA
                AND TRANSACTION_DATE <= aa.fecha_comparable_diaria_AA
            )
            GROUP BY
            Store_Banner,
            CANAL_VENTA,
            MES,
            CALENDAR_YEAR,
            CALENDAR_MONTH_NUMBER
        )

        -- 4. Unimos los resultados de los CTEs
        SELECT
        locales.Formato,
        -- STORE_ID ha sido eliminado
        locales.CANAL_VENTA,
        locales.CALENDAR_YEARMONTH AS MES,
        locales.MONTH_DATE,
        locales.CALENDAR_YEAR,
        locales.CALENDAR_YEAR_AA,
        locales.CALENDAR_MONTH_NUMBER,
        a.Venta_bruta,
        a.Venta_neta,
        a.clientes,
        a.transacciones,
        a.Unidades,
        a.Fecha_Actualizacion,
        aa.Inicio_periodo_comparable_AA,
        aa.Fin_periodo_comparable_AA,
        aa.Venta_bruta AS Venta_bruta_AA,
        aa.Venta_neta AS Venta_neta_AA,
        aa.clientes AS clientes_AA,
        aa.transacciones AS transacciones_AA,
        aa.Unidades AS Unidades_AA,
            -- Peso del last miler dentro del total digital del mes sala
            CASE
                WHEN (locales.CANAL_VENTA IN ('RAPPI', 'UBER EATS')) THEN
                A.Venta_neta / SUM(CASE WHEN locales.CANAL_VENTA IN ('RAPPI', 'UBER EATS') THEN A.Venta_neta ELSE 0 END) OVER (PARTITION BY locales.MONTH_DATE,locales.CANAL_VENTA) else 0
            END AS Porcentaje_Peso_Venta_Last_Milers_Mensual
        FROM
        locales_agg AS locales
        LEFT JOIN ventas_actual AS a ON locales.formato = a.formato
        AND locales.canal_venta = a.canal_venta
        AND locales.CALENDAR_YEAR = a.calendar_year
        AND locales.CALENDAR_MONTH_NUMBER = a.CALENDAR_MONTH_NUMBER
        LEFT JOIN ventas_aa AS aa ON locales.formato = aa.formato
        AND locales.canal_venta = aa.canal_venta
        AND locales.CALENDAR_YEAR_AA = aa.calendar_year
        AND locales.CALENDAR_MONTH_NUMBER = aa.CALENDAR_MONTH_NUMBER
        WHERE
        COALESCE(a.venta_bruta, 0) + COALESCE(aa.venta_bruta, 0) <> 0
    """, # noqa: E501

    'transacciones_mensuales_omnicanal':
    """
    SELECT
    locales.Formato,
    locales.STORE_ID,
    locales.CANAL_VENTA,
    CALENDAR_YEARMONTH AS MES,
    MONTH_DATE,
    locales.CALENDAR_YEAR,
    locales.CALENDAR_YEAR_AA,
    locales.CALENDAR_MONTH_NUMBER,
    a.Venta_bruta,
    a.Venta_neta,
    a.clientes,
    a.transacciones,
    a.Unidades,
    a.Fecha_Actualizacion,
    Inicio_periodo_comparable_AA,
    Fin_periodo_comparable_AA,
    AA.Venta_bruta AS Venta_bruta_AA,
    AA.Venta_neta AS Venta_neta_AA,
    AA.clientes AS clientes_AA,
    AA.transacciones AS transacciones_AA,
    AA.Unidades AS Unidades_AA
    FROM `cl-cda-unidata-dev.DS_UNIDATA_ECOMMERCE.VW_LOCAL_MES_CANAL` AS locales
    LEFT JOIN
    (
        SELECT
        Store_Banner AS Formato,
        CANAL_VENTA,
        STORE_ID,
        -- TRANSACTION_DATE,
        MES,
        CALENDAR_YEAR,
        CALENDAR_YEAR - 1 AS CALENDAR_YEAR_AA,
        CALENDAR_MONTH_NUMBER,
        -- EXTRACT(DAY FROM TRANSACTION_DATE) AS dia,
        sum(value) AS Venta_bruta,
        sum(value - TAX_AMOUNT) AS Venta_neta,
        COUNT(DISTINCT customer_key) AS clientes,
        COUNT(DISTINCT TXN_KEY) AS transacciones,
        sum(QUANTITY) AS Unidades,
        max(TRANSACTION_DATE) AS Fecha_Actualizacion
        FROM `cl-bigdata-analytics-preprod.ECOMMERCE.TRANSACCIONES_OMNICANAL`
        WHERE CALENDAR_YEAR >= 2026
        GROUP BY
        Store_Banner, CANAL_VENTA, STORE_ID, MES, CALENDAR_YEAR,
        CALENDAR_MONTH_NUMBER
    ) A
    ON
        locales.Store_id = a.store_id
        AND locales.CALENDAR_MONTH_NUMBER = A.CALENDAR_MONTH_NUMBER
        AND locales.CALENDAR_YEAR = a.calendar_year
        AND locales.formato = a.formato
        AND locales.canal_venta = a.canal_venta
    -- AND locales.CALENDAR_DAY_NUMBER = a.dia
    LEFT JOIN
    (
        SELECT
        Store_Banner AS Formato,
        CANAL_VENTA,
        STORE_ID,
        min(TRANSACTION_DATE) as Inicio_periodo_comparable_AA,
        max(TRANSACTION_DATE) as Fin_periodo_comparable_AA,
        MES,
        CALENDAR_YEAR,
        CALENDAR_MONTH_NUMBER,
        -- EXTRACT(DAY FROM TRANSACTION_DATE) AS dia,
        sum(value) AS Venta_bruta,
        sum(value - TAX_AMOUNT) AS Venta_neta,
        COUNT(DISTINCT customer_key) AS clientes,
        COUNT(DISTINCT TXN_KEY) AS transacciones,
        sum(QUANTITY) AS Unidades
        FROM `cl-bigdata-analytics-preprod.ECOMMERCE.TRANSACCIONES_OMNICANAL` o
        left join `cl-cda-unidata-dev.DS_UNIDATA_ECOMMERCE.VW_PERIODO_COMPARABLE_AA` aa on aa.Fecha=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        WHERE
        -- Año Anterior completo hasta ultimo mes cerrado del año en curso
        (
        TRANSACTION_DATE >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()) - 1, 1, 1)
        AND TRANSACTION_DATE < DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH)
        )

        OR -- Periodo comparable año anterior de mes en curso
        (
        TRANSACTION_DATE >= aa.fecha_inicio_AA

        AND TRANSACTION_DATE <= aa.fecha_comparable_diaria_AA
        )

        GROUP BY
        Store_Banner, CANAL_VENTA, STORE_ID, MES, CALENDAR_YEAR,
        CALENDAR_MONTH_NUMBER
    ) AA
    ON
        locales.Store_id = aA.store_id
        AND locales.CALENDAR_MONTH_NUMBER = AA.CALENDAR_MONTH_NUMBER
        AND locales.CALENDAR_YEAR_AA = aA.calendar_year
        AND locales.formato = aa.formato
        AND locales.canal_venta = aa.canal_venta
    -- AND locales.CALENDAR_DAY_NUMBER = aa.dia
    WHERE coalesce(a.venta_bruta, 0) + coalesce(aa.venta_bruta, 0) <> 0
    """, # noqa: E501

    'transacciones_mes_canal':
    """
    SELECT
    A.Formato,
    CASE
        WHEN A.CANAL_VENTA = 'CORNER SHOP' THEN 'UBER'
        ELSE A.CANAL_VENTA
        END AS CANAL_VENTA,
    A.MES,
    MONTH_DATE,
    A.CALENDAR_YEAR,
    A.CALENDAR_YEAR_AA,
    A.CALENDAR_MONTH_NUMBER,
    a.Venta_bruta,
    a.Venta_neta,
    a.clientes,
    a.transacciones,
    a.Unidades,
    a.Fecha_Actualizacion,
    AA.Venta_bruta AS Venta_bruta_AA,
    AA.Venta_neta AS Venta_neta_AA,
    AA.clientes AS clientes_AA,
    AA.transacciones AS transacciones_AA,
    AA.Unidades AS Unidades_AA
    FROM
    (
        SELECT
        Store_Banner AS Formato,
        CANAL_VENTA,
        MES,
        DATE_TRUNC(TRANSACTION_DATE, MONTH) AS month_date,
        CALENDAR_YEAR,
        CALENDAR_YEAR - 1 AS CALENDAR_YEAR_AA,
        CALENDAR_MONTH_NUMBER,
        sum(value) AS Venta_bruta,
        sum(value - TAX_AMOUNT) AS Venta_neta,
        COUNT(DISTINCT customer_key) AS clientes,
        COUNT(DISTINCT TXN_KEY) AS transacciones,
        sum(QUANTITY) AS Unidades,
        max(TRANSACTION_DATE) AS Fecha_Actualizacion
        FROM `cl-cda-unidata-prod.DS_UNIDATA_ECOMMERCE.TRANSACCIONES_OMNICANAL_HIST`
        WHERE
        (
            -- 1) Últimos 12 meses completos
            TRANSACTION_DATE >=   DATE_SUB(
            DATE_TRUNC(CURRENT_DATE('America/Santiago'), MONTH),
            INTERVAL 12 MONTH)
        -- AND TRANSACTION_DATE < CURRENT_DATE()--
        )
        /* OR (
            -- 2) Mes en curso hasta ayer (MTD)
            TRANSACTION_DATE >= DATE_TRUNC(CURRENT_DATE('America/Santiago'), MONTH)
            AND TRANSACTION_DATE < CURRENT_DATE('America/Santiago')  -- AYER
        )*/
        GROUP BY
        Store_Banner, CANAL_VENTA, MES, DATE_TRUNC(TRANSACTION_DATE, MONTH),CALENDAR_YEAR, CALENDAR_MONTH_NUMBER
    ) A
    LEFT JOIN
    (
        SELECT
        Store_Banner AS Formato,
        CANAL_VENTA,
        MES,
        CALENDAR_YEAR,
        CALENDAR_MONTH_NUMBER,
        -- EXTRACT(DAY FROM TRANSACTION_DATE) AS dia,
        sum(value) AS Venta_bruta,
        sum(value - TAX_AMOUNT) AS Venta_neta,
        COUNT(DISTINCT customer_key) AS clientes,
        COUNT(DISTINCT TXN_KEY) AS transacciones,
        sum(QUANTITY) AS Unidades
        FROM `cl-cda-unidata-prod.DS_UNIDATA_ECOMMERCE.TRANSACCIONES_OMNICANAL_HIST`
        WHERE
        (
            -- 1) Los 11 meses anteriores completos (Año anterior)
            TRANSACTION_DATE >= DATE_SUB(
            DATE_TRUNC(CURRENT_DATE('America/Santiago'), MONTH),
            INTERVAL 26 MONTH)
            AND TRANSACTION_DATE < DATE_SUB(
            DATE_TRUNC(CURRENT_DATE('America/Santiago'), MONTH),
            INTERVAL 12 MONTH))
        OR (
            -- 2) Mes homólogo del año anterior (PY-MTD)
            TRANSACTION_DATE >= DATE_SUB(
            DATE_TRUNC(CURRENT_DATE('America/Santiago'), MONTH),
            INTERVAL 12 MONTH)
            AND TRANSACTION_DATE < DATE_SUB(
            CURRENT_DATE('America/Santiago'), INTERVAL 1 YEAR))
        GROUP BY
        Store_Banner, CANAL_VENTA, MES, CALENDAR_YEAR, CALENDAR_MONTH_NUMBER
    ) AA
    ON
        A.CALENDAR_MONTH_NUMBER = AA.CALENDAR_MONTH_NUMBER
        AND A.CALENDAR_YEAR_AA = aA.calendar_year
        AND A.formato = aa.formato
        AND A.canal_venta = aa.canal_venta
    """, # noqa: E501

    'transacciones_diarias_omnicanal':
    """
    SELECT
    -- Atributos del local y canal desde la vista de calendario
    locales.Formato,                 -- Banner/segmento del local (ej. Supermercado, Alvi, etc.)
    locales.STORE_ID,                -- Identificador único del local
    locales.CANAL_VENTA,             -- Canal (e-commerce, presencial, etc.)

    -- Construcción de la fecha transaccional desde componentes de calendario
    DATE(
        locales.CALENDAR_YEAR,
        locales.CALENDAR_MONTH_NUMBER,
        locales.CALENDAR_DAY_NUMBER
    ) AS TRANSACTION_DATE,

    -- Llaves de tiempo
    locales.CALENDAR_YEARMONTH AS MES,  -- AñoMes (YYYYMM)
    locales.CALENDAR_YEAR,              -- Año actual (del registro en calendario)
    locales.CALENDAR_YEAR_AA,           -- Año anterior (del registro en calendario)
    locales.CALENDAR_DAY_NUMBER AS dia, -- Día del mes (1-31)
    comp_aa.fecha_comparable_diaria_AA AS dia_AA_comparable,
    -- Métricas del año actual (A)
    a.Venta_bruta,
    a.Venta_neta,
    a.clientes,
    a.transacciones,
    a.Unidades,
    a.Fecha_Actualizacion,  -- Última fecha presente en transacciones


    -- Métricas del año anterior (AA)
    AA.Venta_bruta AS Venta_bruta_AA,
    AA.Venta_neta AS Venta_neta_AA,
    AA.clientes    AS clientes_AA,
    AA.transacciones AS transacciones_AA,
    AA.Unidades    AS Unidades_AA

    FROM `cl-cda-unidata-dev.DS_UNIDATA_ECOMMERCE.VW_LOCAL_DIA_CANAL` AS locales
    left join `cl-cda-unidata-dev.DS_UNIDATA_ECOMMERCE.VW_PERIODO_COMPARABLE_AA` comp_aa on comp_aa.Fecha=locales.TRANSACTION_DATE

    -- JOIN: Métricas agregadas del año actual (filtradas por año en WHERE)
    LEFT JOIN (
    SELECT
        Store_Banner AS Formato,   -- Normaliza nombre de banner al alias usado afuera
        CANAL_VENTA,
        STORE_ID,
        TRANSACTION_DATE,
        MES,
        CALENDAR_YEAR,
        CALENDAR_YEAR - 1 AS CALENDAR_YEAR_AA,  -- Deriva año anterior
        CALENDAR_MONTH_NUMBER,
        EXTRACT(DAY FROM TRANSACTION_DATE) AS dia,


        -- Agregaciones de negocio
        SUM(value)                       AS Venta_bruta,
        SUM(value - TAX_AMOUNT)          AS Venta_neta,      -- Neta = Bruta - IVA/impuestos
        COUNT(DISTINCT customer_key)     AS clientes,        -- Clientes únicos
        COUNT(DISTINCT TXN_KEY)          AS transacciones,   -- Transacciones únicas
        SUM(QUANTITY)                    AS Unidades,        -- Unidades vendidas
        MAX(TRANSACTION_DATE)            AS Fecha_Actualizacion -- Última fecha con datos
    FROM `cl-bigdata-analytics-preprod.ECOMMERCE.TRANSACCIONES_OMNICANAL`


    --  Filtro de año: actualmente fijo a 2026 en adelante
    -- Si quieres YTD dinámico, reemplazar por BETWEEN inicio de año y hoy.
    WHERE CALENDAR_YEAR >= 2026

    GROUP BY
        Store_Banner, CANAL_VENTA, STORE_ID, TRANSACTION_DATE, MES, CALENDAR_YEAR,
        CALENDAR_MONTH_NUMBER
    ) AS A
    ON
    -- Empareja por local, canal, formato y la misma fecha (año/mes/día)
    locales.Store_id             = A.store_id
    AND locales.CALENDAR_MONTH_NUMBER = A.CALENDAR_MONTH_NUMBER
    AND locales.CALENDAR_YEAR    = A.calendar_year
    AND locales.formato          = A.formato
    AND locales.canal_venta      = A.canal_venta
    AND locales.CALENDAR_DAY_NUMBER = A.dia

    -- JOIN: Métricas del año anterior (AA) y también incluye año actual según tu WHERE comentado
    LEFT JOIN (
    SELECT
        Store_Banner AS Formato,
        CANAL_VENTA,
        STORE_ID,
        TRANSACTION_DATE,
        MES,
        CALENDAR_YEAR,
        CALENDAR_MONTH_NUMBER,
        EXTRACT(DAY FROM TRANSACTION_DATE) AS dia,

        -- Agregaciones
        SUM(value)                   AS Venta_bruta,
        SUM(value - TAX_AMOUNT)      AS Venta_neta,
        COUNT(DISTINCT customer_key) AS clientes,
        COUNT(DISTINCT TXN_KEY)      AS transacciones,
        SUM(QUANTITY)                AS Unidades

    FROM `cl-bigdata-analytics-preprod.ECOMMERCE.TRANSACCIONES_OMNICANAL`
    --left join `cl-cda-unidata-dev.DS_UNIDATA_ECOMMERCE.VW_PERIODO_COMPARABLE_AA` aa on aa.Fecha=TRANSACTION_DATE

    -- Rango temporal combinado:
    -- 1) Año actual desde el 1 de enero hasta hoy
    -- 2) Año anterior desde el 1 de enero hasta la misma fecha (hoy - 1 año)
        WHERE
        -- Año Anterior completo hasta ultimo mes cerrado del año en curso
        (
        TRANSACTION_DATE >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()) - 1, 1, 1)
        AND TRANSACTION_DATE <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH)
        )

    /*  OR -- Periodo comparable año anterior de mes en curso
        (
        TRANSACTION_DATE >= aa.fecha_inicio_AA

        AND TRANSACTION_DATE <= aa.fecha_comparable_diaria_AA
        ) */
    GROUP BY
        Store_Banner, CANAL_VENTA, STORE_ID, TRANSACTION_DATE, MES, CALENDAR_YEAR,
        CALENDAR_MONTH_NUMBER
    ) AS AA
    ON
    -- Empareja con el calendario usando el año anterior (CALENDAR_YEAR_AA)
    locales.Store_id             = AA.store_id
    AND locales.CALENDAR_MONTH_NUMBER = AA.CALENDAR_MONTH_NUMBER
    AND locales.CALENDAR_YEAR_AA = AA.calendar_year
    AND locales.formato          = AA.formato
    AND locales.canal_venta      = AA.canal_venta
    AND EXTRACT(DAY FROM comp_aa.fecha_comparable_diaria_AA) = AA.dia
    """ # noqa: E501
})


# -------------------------------------------------------------------------
# Main Function
# -------------------------------------------------------------------------
def main():
    # ----------
    # Parameters
    # ----------
    args = vars(parser.parse_args())

    # Environment
    gcp_project: str = args['gcp_project']
    execution_date: str = args['execution_date']

    # Constants
    gbq_client = Client()
    table_base_ref_1 = f'{gcp_project}.ECOMMERCE.TRANSACCIONES_OMNICANAL'
    table_base_ref_2 = f'{gcp_project}.ECOMMERCE.TRANSACCIONES_OMNICANAL_HIST'
    table_base_ref_3 = f'{gcp_project}.ECOMMERCE.TRANSACCIONES_MENSUALES_POR_CANAL_FORMATO'
    table_base_ref_4 = f'{gcp_project}.ECOMMERCE.TRANSACCIONES_MENSUALES_OMNICANAL'
    table_base_ref_5 = f'{gcp_project}.ECOMMERCE.TRANSACCIONES_MES_CANAL'
    table_base_ref_6 = f'{gcp_project}.ECOMMERCE.TRANSACCIONES_DIARIAS_OMNICANAL'

    logging.info(f'execution_date = {execution_date}')

    logging.info('transacciones_omnicanal')

    createTableAsSelect(
        query=SQL_QUERIES['transacciones_omnicanal'].substitute(
        ),
        table_ref=table_base_ref_1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )

    logging.info('transacciones_omnicanal_hist')

    createTableAsSelect(
        query=SQL_QUERIES['transacciones_omnicanal_hist'].substitute(
        ),
        table_ref=table_base_ref_2,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )

    logging.info('transacciones_mensuales_por_canal_formato')

    createTableAsSelect(
        query=SQL_QUERIES['transacciones_mensuales_por_canal_formato'].substitute(
        ),
        table_ref=table_base_ref_3,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )

    logging.info('transacciones_mensuales_omnicanal')

    createTableAsSelect(
        query=SQL_QUERIES['transacciones_mensuales_omnicanal'].substitute(
        ),
        table_ref=table_base_ref_4,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )

    logging.info('transacciones_mes_canal')

    createTableAsSelect(
        query=SQL_QUERIES['transacciones_mes_canal'].substitute(
        ),
        table_ref=table_base_ref_5,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )

    createTableAsSelect(
        query=SQL_QUERIES['transacciones_diarias_omnicanal'].substitute(
        ),
        table_ref=table_base_ref_6,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )


if __name__ == '__main__':
    main()
