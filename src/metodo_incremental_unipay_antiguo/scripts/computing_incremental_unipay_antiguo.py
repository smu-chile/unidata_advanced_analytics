
# Default
from __future__ import annotations

import os
import logging  # noqa: F401
import argparse
from logging import config
from datetime import datetime  # noqa: F401

# Pip
import numpy as np
import pandas as pd
import pendulum
from google.cloud import bigquery  # noqa: F401
from google.cloud.bigquery import Client
from dateutil.relativedelta import relativedelta  # noqa: F401

# Own
import common.gcp_extended.bigquery as gbq_extended  # noqa: F401
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (  # noqa: E402
    uploadFrame,
    readBigQuery,
    deleteFromTable,
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



# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({    # Region: Explicación de query

 'query_principal':
"""
 SELECT * FROM `cl-bigdata-analytics-preprod.UNIPAY.TABLA_MENSUAL_INCREMENTAL_ANTIGUO`

""",# noqa: E501

'query_costo_promocional':
"""
DECLARE ANOMES_CERRADO INT64 DEFAULT ${anomes_cerrado};

WITH PROMOS_UNIPAY AS (
    -- QUERY DESCUENTOS --
    SELECT
          CASE WHEN DSH.ORG_IP_ID IN ('01') AND FBE.MARKET_BASKET_KEY IS NULL THEN 'Unimarc'
                WHEN DSH.ORG_IP_ID IN ('01') AND FBE.MARKET_BASKET_KEY IS NOT NULL THEN 'Ecommerce'
                      WHEN DSH.ORG_IP_ID IN ('02','09') THEN 'Super 10'
                      WHEN DSH.ORG_IP_ID IN ('08') THEN 'Alvi'
                      ELSE 'Sin formato' END AS store_banner,
          SUM(CASE WHEN FIT.ITM_TXN_AMT>0 AND GEO.NAME IS NOT NULL THEN DIS.DCN_AMT END) AS COSTO_PROMOCIONAL_BRUTO


        FROM
          (SELECT ITEM_TRANSACTION_KEY,ITM_TXN_AMT,TAX_AMOUNT,TXN_KEY,CUSTOMER_KEY,CUSTOMER_HEX,NBR_PD_ITM,WGHT_ITM,STORE_KEY,PRODUCT_KEY_1,DATE_KEY,MARKET_BASKET_KEY,ITM_TXN_FCN_TP_DSC,FNC_DOC_TP_HEX FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_ITM_TXN`
          WHERE DATE_TRUNC(ITM_TXN_TMS, MONTH) = PARSE_DATETIME('%Y%m', CAST(ANOMES_CERRADO AS STRING))
          )   FIT -- FILTRO DE FECHA

          JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE_HIERARCHY`    DSH ON DSH.STORE_KEY = FIT.STORE_KEY
          JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT_HIERARCHY`  DPH ON DPH.PRODUCT_KEY = FIT.PRODUCT_KEY_1
          JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT`            DP  ON DP.PRODUCT_KEY = FIT.PRODUCT_KEY_1
          JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_DATE`               DD  ON DD.DATE_KEY = FIT.DATE_KEY
          JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_CALENDAR_YEAR_WEEK` DY  ON DY.CALENDAR_YEAR_WEEK_KEY = DD.CALENDAR_YEAR_WEEK_KEY
          --JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MKT_BSKT` bs   ON bs.MARKET_BASKET_KEY = FIT.MARKET_BASKET_KEY


          LEFT JOIN
          `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_VTA_DIRECTA` VD
          ON
              FIT.MARKET_BASKET_KEY=VD.MARKET_BASKET_KEY
            ---------------------------------------NUEVA MARCA-------------------------------------------------
          LEFT JOIN `cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_FACT_MONTH_CUSTOMER_ALVI_TYPE_CON_RECONVERTIDOS` NEW_MARCA
            ON
              NEW_MARCA.CUSTOMER_KEY = FIT.CUSTOMER_KEY
              AND DD.CALENDAR_YEAR * 100 + DD.CALENDAR_MONTH_NUMBER = CAST(FORMAT_DATE('%Y%m',DATE_ADD(PARSE_DATE('%Y%m',CAST(NEW_MARCA.MONTH_ID AS STRING)), INTERVAL -1 MONTH)) AS INT64)

                  LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_E_COMMERCE` FBE ON FBE.MARKET_BASKET_KEY=FIT.MARKET_BASKET_KEY AND FBE.CANAL_VENTA ='E-COMMERCE'


          LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_ITEM_TRANSACTION_DISCT` dis ON dis.ITEM_TRANSACTION_KEY = fit.ITEM_TRANSACTION_KEY
            LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PROMOTIONAL_GROUP` PROM ON dis.PROM_CODE_KEY = PROM.PROM_GROUP_KEY

            LEFT JOIN (
              select
                ID AS ID_GEO
                , FORMATO
                , NAME
                , DESCRIPTION
                , TYPEID
                , STARTDATE
                , ENDDATE
              FROM  `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_GP_PROMOTIONS`
              WHERE  FECHA_CARGA in (SELECT MAX(FECHA_CARGA) FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_GP_PROMOTIONS`)
                AND (
                UPPER(NAME) LIKE '%DCTO UNIPAY%'
                OR UPPER(NAME) LIKE '%UNIPAY DCTO%'
              )
              AND ENDDATE >= '2025-01-01 00:00:00'
              group by 1,2,3,4,5,6,7

            ) GEO ON PROM.PROM_GROUP_ID = GEO.ID_GEO


                      LEFT JOIN
                            (SELECT MARKET_BASKET_KEY,TNDR_TP_ID,TNDR_TP_DSC
                                  FROM (SELECT A.MARKET_BASKET_KEY, A.PYMT_AMT, T.TNDR_TP_ID,TNDR_TP_DSC,
                                  RANK() OVER(PARTITION BY MARKET_BASKET_KEY ORDER BY PYMT_AMT DESC,TNDR_TP_ID ASC) AS RANKING
                                  FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_PAYMENT` A
                                  INNER JOIN
                                  `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_TENDER_TYPE` T ON A.TNDR_TP_KEY = T.TENDER_TYPE_KEY
                                  INNER JOIN
                                  `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MKT_BSKT` P USING(MARKET_BASKET_KEY)
                                  WHERE
                                  DATE_TRUNC(P.DATE_VALUE, MONTH) = PARSE_DATETIME('%Y%m', CAST(ANOMES_CERRADO AS STRING))
                                  AND
                                  P.FNC_DOC_TP_DSC NOT IN ('TF','TN'))a
                                  WHERE RANKING=1) A
                                  ON A.MARKET_BASKET_KEY= FIT.MARKET_BASKET_KEY

                          LEFT JOIN (
                                  SELECT MARKET_BASKET_KEY,TNDR_TP_ID,TNDR_TP_DSC,
                                  CONCAT(STORE_HEX,'-',DATE_HEX,'-',TIME_HEX,'-',POS_HEX) AS TXN_KEY
                                  FROM (SELECT A.MARKET_BASKET_KEY,A.STORE_HEX,A.DATE_HEX,A.TIME_HEX,A.POS_HEX,A.PYMT_AMT,T.TNDR_TP_ID,TNDR_TP_DSC,
                                  RANK() OVER(PARTITION BY MARKET_BASKET_KEY ORDER BY PYMT_AMT DESC,TNDR_TP_ID ASC) AS RANKING
                                  FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_PAYMENT` A
                                  INNER JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_TENDER_TYPE` T ON TNDR_TP_KEY = T.TENDER_TYPE_KEY
                                  INNER JOIN`cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MKT_BSKT` P USING(MARKET_BASKET_KEY)
                                  WHERE  DATE_TRUNC(P.DATE_VALUE, MONTH) = PARSE_DATETIME('%Y%m', CAST(ANOMES_CERRADO AS STRING))
                                  AND P.FNC_DOC_TP_DSC NOT IN ('BX','B ','BE'))a
                                  WHERE RANKING=1
                          ) B ON B.TXN_KEY=FIT.TXN_KEY


        WHERE
          FIT.ITM_TXN_FCN_TP_DSC = 'V'
          AND FIT.CUSTOMER_HEX NOT IN ('3588d47a76aac91fcf2a3c2f55f6a351')
          AND FIT.FNC_DOC_TP_HEX IN ('5756ebdc189492f0ad8e05e633217018','3ad6ff06d7bc49ae6f05b15354c3af0a','a2f3a5cc2e8b1292bc6629beac500720','4a209440364b13aa8cd293a37cee6ee1','2fae0e1971b412541215bec30dcedf01','cfe71cea05fb5fa5cb5b5f2a72d616af','e784c5e99b4e72f9e4d85a3f244246a9','b7ff659d1213e5fe6a36d081943123a2')
          AND DPH.NEG_ID NOT IN ('14','15')
          AND DSH.STORE_ID NOT IN ('0622')
          AND DSH.ORG_IP_ID IN ('01','02','08','09')
          AND DD.CALENDAR_YEAR * 100 + DD.CALENDAR_MONTH_NUMBER = ANOMES_CERRADO
        GROUP BY
          1
)
    SELECT
     store_banner
    ,ANOMES_CERRADO AS PERIODO
    ,COSTO_PROMOCIONAL_BRUTO
    FROM PROMOS_UNIPAY
""",# noqa: E501

'query_venta_bruta':
"""
DECLARE ANOMES_CERRADO INT64 DEFAULT ${anomes_cerrado};

 SELECT
                    CASE WHEN DSH.ORG_IP_ID IN ('01') AND FBE.MARKET_BASKET_KEY IS NULL THEN 'Unimarc'
                    WHEN DSH.ORG_IP_ID IN ('02','09') THEN 'Super 10'
                    WHEN DSH.ORG_IP_ID IN ('08') THEN 'Alvi'
                    ELSE 'Ecommerce' END AS store_banner,
                    ANOMES_CERRADO AS PERIODO,
                    SUM (CASE WHEN (A.TNDR_TP_ID IN ('35','68','78') OR B.TNDR_TP_ID IN ('35','68','78')) THEN (FIT.ITM_TXN_AMT) END) AS VENTA_BRUTA,
                    COUNT (DISTINCT CASE WHEN (A.TNDR_TP_ID IN ('35','68','78') OR B.TNDR_TP_ID IN ('35','68','78')) THEN (FIT.CUSTOMER_KEY) END) AS CLIENTES


          FROM
                  (SELECT ITM_TXN_AMT,TAX_AMOUNT,TXN_KEY,CUSTOMER_KEY,CUSTOMER_HEX,NBR_PD_ITM,WGHT_ITM,STORE_KEY,PRODUCT_KEY_1,DATE_KEY,    MARKET_BASKET_KEY,ITM_TXN_FCN_TP_DSC,FNC_DOC_TP_HEX FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_ITM_TXN`
                  WHERE DATE_TRUNC(ITM_TXN_TMS, MONTH) = PARSE_DATETIME('%Y%m', CAST(ANOMES_CERRADO AS STRING))
                  )   FIT -- FILTRO DE FECHA

                  JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE_HIERARCHY`    DSH ON DSH.STORE_KEY = FIT.STORE_KEY
                  JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT_HIERARCHY`  DPH ON DPH.PRODUCT_KEY = FIT.PRODUCT_KEY_1
                  JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT`            DP  ON DP.PRODUCT_KEY = FIT.PRODUCT_KEY_1
                  JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_DATE`               DD  ON DD.DATE_KEY = FIT.DATE_KEY
                  JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_CALENDAR_YEAR_WEEK` DY  ON DY.CALENDAR_YEAR_WEEK_KEY = DD.CALENDAR_YEAR_WEEK_KEY

        LEFT JOIN
        `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_VTA_DIRECTA` VD
        ON
            FIT.MARKET_BASKET_KEY=VD.MARKET_BASKET_KEY
          ---------------------------------------NUEVA MARCA-------------------------------------------------
        LEFT JOIN `cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_FACT_MONTH_CUSTOMER_ALVI_TYPE_CON_RECONVERTIDOS` NEW_MARCA
          ON
            NEW_MARCA.CUSTOMER_KEY = FIT.CUSTOMER_KEY
            AND DD.CALENDAR_YEAR * 100 + DD.CALENDAR_MONTH_NUMBER = CAST(FORMAT_DATE('%Y%m',DATE_ADD(PARSE_DATE('%Y%m',CAST(NEW_MARCA.MONTH_ID AS STRING)), INTERVAL -1 MONTH)) AS INT64)

                LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_E_COMMERCE` FBE ON FBE.MARKET_BASKET_KEY=FIT.MARKET_BASKET_KEY AND FBE.CANAL_VENTA ='E-COMMERCE'


                  LEFT JOIN
                  (SELECT MARKET_BASKET_KEY,TNDR_TP_ID,TNDR_TP_DSC
                        FROM (SELECT A.MARKET_BASKET_KEY, A.PYMT_AMT, T.TNDR_TP_ID,TNDR_TP_DSC,
                        RANK() OVER(PARTITION BY MARKET_BASKET_KEY ORDER BY PYMT_AMT DESC,TNDR_TP_ID ASC) AS RANKING
                        FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_PAYMENT` A
                        INNER JOIN
                        `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_TENDER_TYPE` T ON A.TNDR_TP_KEY = T.TENDER_TYPE_KEY
                        INNER JOIN
                        `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MKT_BSKT` P USING(MARKET_BASKET_KEY)
                        WHERE
                        DATE_TRUNC(P.DATE_VALUE, MONTH) = PARSE_DATETIME('%Y%m', CAST(ANOMES_CERRADO AS STRING))
                        AND
                        P.FNC_DOC_TP_DSC NOT IN ('TF','TN'))a
                        WHERE RANKING=1) A
                        ON A.MARKET_BASKET_KEY= FIT.MARKET_BASKET_KEY

                LEFT JOIN (
                        SELECT MARKET_BASKET_KEY,TNDR_TP_ID,TNDR_TP_DSC,
                        CONCAT(STORE_HEX,'-',DATE_HEX,'-',TIME_HEX,'-',POS_HEX) AS TXN_KEY
                        FROM (SELECT A.MARKET_BASKET_KEY,A.STORE_HEX,A.DATE_HEX,A.TIME_HEX,A.POS_HEX,A.PYMT_AMT,T.TNDR_TP_ID,TNDR_TP_DSC,
                        RANK() OVER(PARTITION BY MARKET_BASKET_KEY ORDER BY PYMT_AMT DESC,TNDR_TP_ID ASC) AS RANKING
                        FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_PAYMENT` A
                        INNER JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_TENDER_TYPE` T ON TNDR_TP_KEY = T.TENDER_TYPE_KEY
                        INNER JOIN`cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MKT_BSKT` P USING(MARKET_BASKET_KEY)
                        WHERE  DATE_TRUNC(P.DATE_VALUE, MONTH) = PARSE_DATETIME('%Y%m', CAST(ANOMES_CERRADO AS STRING))
                        AND P.FNC_DOC_TP_DSC NOT IN ('BX','B ','BE'))a
                        WHERE RANKING=1
                ) B ON B.TXN_KEY=FIT.TXN_KEY

                WHERE
                  FIT.ITM_TXN_FCN_TP_DSC = 'V'
                  AND FIT.FNC_DOC_TP_HEX IN ('5756ebdc189492f0ad8e05e633217018','3ad6ff06d7bc49ae6f05b15354c3af0a','a2f3a5cc2e8b1292bc6629beac500720','4a209440364b13aa8cd293a37cee6ee1','2fae0e1971b412541215bec30dcedf01','cfe71cea05fb5fa5cb5b5f2a72d616af','e784c5e99b4e72f9e4d85a3f244246a9','b7ff659d1213e5fe6a36d081943123a2')
                  AND DPH.NEG_ID NOT IN ('14','15')
                  AND DSH.STORE_ID NOT IN ('0622')
                  AND (A.TNDR_TP_ID IN ('35','68','78') OR B.TNDR_TP_ID IN ('35','68','78'))
                  AND DD.CALENDAR_YEAR * 100 + DD.CALENDAR_MONTH_NUMBER = ANOMES_CERRADO
                GROUP BY
                  1,2
                ORDER BY 1 ASC
"""# noqa: E501
})


# -------------------------------------------------------------------------
# Reglas de balanceo
# -------------------------------------------------------------------------

REGLAS_BALANCEO = {

    'Unimarc': (
        'SHABITS',
        'ZONA'
    ),

    'Super 10': (
        'SHABITS',
        'ZONA'
    ),

    'Alvi': (
        'SHABITS',
        'ZONA'
    )

}


CONFIG_INCREMENTAL = {

    'Unimarc': {
        'target': 'CLIENTE TH USA',
        'control': 'CLIENTE FORMATO'
    },

    'Super 10': {
        'target': 'CLIENTE TH USA',
        'control': 'CLIENTE TH NO USA'
    },

    'Alvi': {
        'target': 'CLIENTE TH USA',
        'control': 'CLIENTE FORMATO'
    }

}


# ------------------------------------------------------------------------
# Balanceo de clientes
# ------------------------------------------------------------------------

def balancear_targets_full(
    df,
    var1,
    var2,
    target_base='CLIENTE TH USA',
    random_state=42
):
    """Balancea los grupos utilizando como referencia el target_base."""

    np.random.seed(random_state)  # noqa: NPY002

    base_df = df.drop_duplicates(
        subset=['CUSTOMER_KEY']
    )

    base = base_df[
        base_df['TARGET_CLIENTE'] == target_base
    ]

    dist = (
        base
        .groupby([var1, var2])['CUSTOMER_KEY']
        .count()
        .reset_index(name='n')
    )

    dist['pct'] = dist['n'] / dist['n'].sum()

    total_por_target = (
        base_df
        .groupby('TARGET_CLIENTE')['CUSTOMER_KEY']
        .count()
        .to_dict()
    )

    ids = []

    for target, total in total_por_target.items():

        df_target = base_df[
            base_df['TARGET_CLIENTE'] == target
        ]

        for _, row in dist.iterrows():

            g1 = row[var1]
            g2 = row[var2]

            n_sample = round(
                row['pct'] * total
            )

            pool = (
                df_target[
                    (df_target[var1] == g1)
                    &
                    (df_target[var2] == g2)
                ]
                .sort_values('CUSTOMER_KEY')
            )

            if pool.empty:
                continue

            sample_ids = pool.sample(
                n=min(n_sample, len(pool)),
                random_state=random_state
            )['CUSTOMER_KEY']

            ids.extend(
                sample_ids.tolist()
            )

    return (
        df[
            df['CUSTOMER_KEY'].isin(ids)
        ]
        .reset_index(drop=True)
    )


def balancear_clientes(df_principal):
    """Balancea los clientes según las reglas definidas."""

    dfs_balanceados = []

    for banner, (var1, var2) in REGLAS_BALANCEO.items():

        df_tmp = df_principal[
            df_principal['STORE_BANNER'] == banner
        ].copy()

        if df_tmp.empty:
            continue

        df_balanceado = balancear_targets_full(
            df=df_tmp,
            var1=var1,
            var2=var2
        )

        dfs_balanceados.append(
            df_balanceado
        )

    if not dfs_balanceados:
        msg = 'No fue posible balancear ningún segmento.'
        raise ValueError(
            msg
        )

    return pd.concat(
        dfs_balanceados,
        ignore_index=True
    )


def calcular_tablas_gasto(df_balanceado):
    """Calcula las tablas de gasto promedio por formato."""

    tablas_gasto = {}

    for banner in REGLAS_BALANCEO:

        df_tmp = df_balanceado[
            df_balanceado['STORE_BANNER'] == banner
        ].copy()

        if df_tmp.empty:
            continue

        clientes_target = (
            df_tmp
            .groupby('TARGET_CLIENTE')['CUSTOMER_KEY']
            .nunique()
        )

        ventas = (
            df_tmp
            .groupby(
                [
                    'MEDIO_DE_PAGO',
                    'TARGET_CLIENTE'
                ]
            )['VENTA_NETA']
            .sum()
            .reset_index()
        )

        ventas['CLIENTES_TARGET'] = (
            ventas['TARGET_CLIENTE']
            .map(clientes_target)
        )

        ventas['GASTO_PROMEDIO'] = (
            ventas['VENTA_NETA']
            /
            ventas['CLIENTES_TARGET']
        )

        tablas_gasto[banner] = (
            ventas
            .pivot_table(
                index='MEDIO_DE_PAGO',
                columns='TARGET_CLIENTE',
                values='GASTO_PROMEDIO'
            )
            .round(0)
        )

    return tablas_gasto


def calcular_factor_incremental(
    tabla_gasto,
    store_banner
):
    """Calcula el factor incremental utilizando el modelo antiguo."""

    config = CONFIG_INCREMENTAL[
        store_banner
    ]

    target = config['target']
    control = config['control']

    tabla = (
        tabla_gasto
        .reset_index()
        .copy()
    )

    tabla[target] = tabla[target].fillna(0)
    tabla[control] = tabla[control].fillna(0)

    tabla['DIFF'] = (
        tabla[target]
        -
        tabla[control]
    )

    tabla['DIFF'] = np.where(
        (tabla['MEDIO_DE_PAGO'] != 'Unipay')
        &
        (tabla['DIFF'] > 0),
        0,
        tabla['DIFF']
    )

    a = tabla['DIFF'].sum()

    b = (
        tabla.loc[
            tabla['MEDIO_DE_PAGO'] == 'Unipay',
            'DIFF'
        ]
        .iloc[0]
    )

    factor = np.nan

    if b != 0:
        factor = a / b

    return pd.DataFrame(
        {
            'STORE_BANNER': [store_banner],
            'B': [b],
            'A': [a],
            'FACTOR_INCREMENTAL': [factor],
            'FACTOR_INCREMENTAL_PCT': [
                round(
                    factor * 100,
                    1
                )
            ]
        }
    )


def calcular_incremental(tablas_gasto):
    """Calcula el factor incremental."""

    resultados = []

    for banner, tabla_gasto in tablas_gasto.items():

        resultados.append(

            calcular_factor_incremental(
                tabla_gasto=tabla_gasto,
                store_banner=banner
            )

        )

    return pd.concat(
        resultados,
        ignore_index=True
    )


def construir_resultado(
    df_incremental,
    df_venta_bruta,
    df_costo_promocional
):
    """Construye el dataframe final de resultados."""

    df_venta = df_venta_bruta.rename(
        columns={
            'store_banner': 'STORE_BANNER'
        }
    )

    df_costo = df_costo_promocional.rename(
        columns={
            'store_banner': 'STORE_BANNER'
        }
    )

    df_venta['VENTA_NETA'] = (
        df_venta['VENTA_BRUTA'] / 1.19
    )

    df_costo['COSTO_PROMOCIONAL_NETO'] = (
        df_costo['COSTO_PROMOCIONAL_BRUTO'] / 1.19
    )

    df_resultado = df_incremental.merge(

    df_venta[
        [
            'PERIODO',
            'STORE_BANNER',
            'VENTA_NETA'
        ]
    ],

    on=[
        'STORE_BANNER'
    ],

    how='left'

    )

    df_resultado['VENTA_INCREMENTAL_NETA'] = (
        df_resultado['VENTA_NETA']
        *
        df_resultado['FACTOR_INCREMENTAL']
    )

    df_resultado = df_resultado.merge(

    df_costo[
        [
            'STORE_BANNER',
            'COSTO_PROMOCIONAL_NETO'
        ]
    ],

    on=[
        'STORE_BANNER'
    ],

    how='left'

    )

    df_resultado['COSTO_PROMOCIONAL_NETO'] = (
        df_resultado['COSTO_PROMOCIONAL_NETO']
        .fillna(0)
    )

    df_resultado[
        [
            'VENTA_NETA',
            'VENTA_INCREMENTAL_NETA',
            'COSTO_PROMOCIONAL_NETO'
        ]
    ] = (
        df_resultado[
            [
                'VENTA_NETA',
                'VENTA_INCREMENTAL_NETA',
                'COSTO_PROMOCIONAL_NETO'
            ]
        ]
        .round(0)
    )

    return df_resultado[
        [
            'PERIODO',
            'STORE_BANNER',
            'FACTOR_INCREMENTAL_PCT',
            'VENTA_NETA',
            'VENTA_INCREMENTAL_NETA',
            'COSTO_PROMOCIONAL_NETO'
        ]
    ]


# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    """Ejecuta el cálculo incremental Unipay."""

    usuario = 'unipay_incremental_antiguo'

    # ---------------------------------------------------------------------
    # Parse input variables
    # ---------------------------------------------------------------------

    args = vars(parser.parse_args())

    execution_date: str = args['execution_date']
    project_id: str = args['project_id']

    logging.info(
        f'execution_date: {execution_date}'
    )

    # ---------------------------------------------------------------------
    # Configuración inicial
    # ---------------------------------------------------------------------

    gbq_client = Client()

    execution_date = pendulum.parse(
        execution_date
    )

    anomes_cerrado = (
        execution_date
        .subtract(months=1)
        .format('YYYYMM')
    )

    logging.info(
        f'Periodo a procesar: {anomes_cerrado}'
    )

    # ---------------------------------------------------------------------
    # Query principal
    # ---------------------------------------------------------------------

    query_principal = SQL_QUERIES[
        'query_principal'
    ].substitute(
        anomes_cerrado=anomes_cerrado
    )

    logging.info(
        'Inicia consulta principal...'
    )

    df_principal = readBigQuery(
        query=query_principal,
        user=usuario,
        gbq_client=gbq_client
    )

    logging.info(
        'Termina consulta principal.'
    )

    # ---------------------------------------------------------------------
    # Query venta bruta
    # ---------------------------------------------------------------------

    query_venta_bruta = SQL_QUERIES[
        'query_venta_bruta'
    ].substitute(
        anomes_cerrado=anomes_cerrado
    )

    logging.info(
        'Inicia consulta venta bruta...'
    )

    df_venta_bruta = readBigQuery(
        query=query_venta_bruta,
        user=usuario,
        gbq_client=gbq_client
    )

    logging.info(
        'Termina consulta venta bruta.'
    )

    # ---------------------------------------------------------------------
    # Query costo promocional
    # ---------------------------------------------------------------------

    query_costo_promocional = SQL_QUERIES[
        'query_costo_promocional'
    ].substitute(
        anomes_cerrado=anomes_cerrado
    )

    logging.info(
        'Inicia consulta costo promocional...'
    )

    df_costo_promocional = readBigQuery(
        query=query_costo_promocional,
        user=usuario,
        gbq_client=gbq_client
    )

    logging.info(
        'Termina consulta costo promocional.'
    )

    # ---------------------------------------------------------------------
    # Balanceo de clientes
    # ---------------------------------------------------------------------

    df_balanceado = balancear_clientes(
        df_principal
    )

    # ---------------------------------------------------------------------
    # Tablas de gasto
    # ---------------------------------------------------------------------

    tablas_gasto = calcular_tablas_gasto(
        df_balanceado
    )

    # ---------------------------------------------------------------------
    # Factor incremental
    # ---------------------------------------------------------------------

    df_incremental = calcular_incremental(
        tablas_gasto
    )

    # ---------------------------------------------------------------------
    # Resultado final
    # ---------------------------------------------------------------------

    df_resultado = construir_resultado(
        df_incremental=df_incremental,
        df_venta_bruta=df_venta_bruta,
        df_costo_promocional=df_costo_promocional
    )

    # ---------------------------------------------------------------------
    # Configuración de salida
    # ---------------------------------------------------------------------

    esquema = 'UNIPAY'
    tabla = 'UNIPAY_INCREMENTAL_ANTIGUO'

    path_table = (
        f'{project_id}.{esquema}.{tabla}'
    )

    createTableFromJSON(
        table_ddl_json_path=os.path.join(
            'gbq_objects',
            'ingest_incremental_unipay_antiguo.json'
        ),
        project=project_id,
        gbq_client=gbq_client,
        if_exists='ignore'
    )

    deleteFromTable(
        table_ref=path_table,
        where_clause=(
            f"PERIODO = '{anomes_cerrado}'"
        ),
        gbq_client=gbq_client
    )

    uploadFrame(
        df_resultado,
        table_ddl_json_path=os.path.join(
            'gbq_objects',
            'ingest_incremental_unipay_antiguo.json'
        ),
        project=project_id,
        gbq_client=gbq_client,
        if_exists='append'
    )

    logging.info(
        'Proceso finalizado correctamente.'
    )


if __name__ == '__main__':
    main()
