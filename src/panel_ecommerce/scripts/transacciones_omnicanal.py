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
    table_base_ref=f'{gcp_project}.ECOMMERCE.TRANSACCIONES_OMNICANAL'

    logging.info(f'execution_date = {execution_date}')

    createTableAsSelect(
        query=SQL_QUERIES['transacciones_omnicanal'].substitute(
        ),
        table_ref=table_base_ref,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gbq_client=gbq_client,
    )

if __name__ == '__main__':
    main()
