# Default
from __future__ import annotations

# Pip
import os  # noqa: F401
import re  # noqa: F401
import json  # noqa: F401
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

import pandas as pd
from google.cloud import bigquery  # noqa: F401
from google.cloud.bigquery import Client

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
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
        AND (B.ORG_IP_ID IN ('01', '06'))
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
        AND A.MARKET_BASKET_KEY IN (
            SELECT DISTINCT MARKET_BASKET_KEY
            FROM `${gcp_project_cda}.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_E_COMMERCE`
            WHERE CANAL_VENTA IN ('E-COMMERCE')
        )
        AND B.ORG_IP = '${store_banner}'
        AND A.CUSTOMER_HEX NOT IN ('3588d47a76aac91fcf2a3c2f55f6a351')
        AND D.NEG_DSC NOT IN ('SERVICIOS COMERCIALES', 'NO RETAIL', 'None')
    GROUP BY
        D.NEG_ID, D.GRUPO_ID, A.NBR_PD_ITM, C.CONT_CONV_UMB, A.WGHT_ITM, A.ITM_TXN_TMS, A.TXN_KEY,
        B.STORE_ID, D.SKU_PRODUCT, CAT_DSC, LIN_DESC, SEC_DSC, NEG_DSC, C.EAN, A.MARKET_BASKET_KEY,
        A.ITM_TXN_FCN_TP_DSC, C.UNIDAD_DE_MEDIDA, C.UMREZ, C.UMREN, A.CUSTOMER_KEY, E.FNC_DOC_TP_DSC
    )

    select * from raw_sales
    """  # noqa: E501
})


# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------

def main() -> None:
    usuario = 'infaltables_ecommerce_store_id'  # noqa: F841
    # parse input variables
    args = vars(parser.parse_args())
    gcp_project: str = args['project_id']
    execution_date: str = args['execution_date']
    store_banner: str = args['store_banner']

    if store_banner == 'Super 10':
        upper_store_banner = store_banner.replace(' ', '_').upper()
    else:
        upper_store_banner = store_banner.upper()

    execution_date = pd.to_datetime(execution_date[:8] + '01').strftime('%Y-%m-%d')


    logging.info(f'execution_date: {execution_date}')
    logging.info(f'store_banner: {store_banner}')
    logging.info(f'upper_store_banner: {upper_store_banner}')

    # Set gbq client for all subsequent queries
    gbq_client = Client()

    logging.info('Creacion tabla infaltables raw sales')
    createTableAsSelect(
        query=SQL_QUERIES['raw_sales'].substitute(
            gcp_project_cda = 'cl-cda-prod',
            execution_date = execution_date,
            store_banner = store_banner
        ),
        table_ref=f'{gcp_project}.TMP.TMP_INFALTABLES_RAW_SALES_ECOMMERCE_STORE_ID_{upper_store_banner}',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )


if __name__ == '__main__':
    main()

