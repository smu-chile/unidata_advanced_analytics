
# Default
from __future__ import annotations

import os
import json
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
 SELECT * FROM `cl-bigdata-analytics-preprod.UNIPAY.TABLA_MENSUAL_INCREMENTAL`
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
                     CASE WHEN NEW_MARCA.CUSTOMER_TYPE_DET IN ('COMERCIANTE_DECLARADO','COMERCIANTE_INFERIDO') AND DSH.ORG_IP_ID IN ('08')
                            THEN 'COMERCIANTE'
                            ELSE 'PERSONA' END AS TIPO_CLIENTE,
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
          1,2
)
    SELECT
     store_banner
    ,ANOMES_CERRADO AS PERIODO
    ,TIPO_CLIENTE
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
                    CASE WHEN NEW_MARCA.CUSTOMER_TYPE_DET IN ('COMERCIANTE_DECLARADO','COMERCIANTE_INFERIDO') AND DSH.ORG_IP_ID IN ('08')
                          THEN 'COMERCIANTE'
                          ELSE 'PERSONA' END AS TIPO_CLIENTE,
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
                  1,2,3
                ORDER BY 1 ASC
"""# noqa: E501
})



# -------------------------------------------------------------------------
# Reglas de balanceo
# -------------------------------------------------------------------------

REGLAS_BALANCEO = {

    ('Unimarc', 'PERSONA'): (
        'SHABITS',
        'RANGO_EDAD'
    ),

    ('Ecommerce', 'PERSONA'): (
        'CLASIFICACION_CLIENTE',
        'ZONA'
    ),

    ('Super 10', 'PERSONA'): (
        'SHABITS',
        'RANGO_EDAD'
    ),

    ('Alvi', 'PERSONA'): (
        'SHABITS',
        'ZONA'
    ),

    ('Alvi', 'COMERCIANTE'): (
        'SHABITS',
        'RANGO_EDAD'
    )

}


# ------------------------------------------------------------------------
# Balanceo de clientes
# ------------------------------------------------------------------------

def balancear_targets_full(  # noqa: D417
    df,
    var1,
    var2,
    target_base='CLIENTE TH USA',
    random_state=42,
    targets=None
):
    """Balancea los grupos utilizando como referencia el target_base.

    Este es un balanceo APROXIMADO por cuota/proporción: a cada grupo
    target se le intenta asignar, en cada estrato (var1, var2), la
    misma proporción que tiene target_base. No garantiza igualdad
    exacta si el pool de algún grupo es insuficiente en un estrato
    (en ese caso simplemente toma lo que hay disponible).

    Se usa aquí solo para grupos que NO requieren igualdad garantizada
    con target_base (ej. 'CLIENTE TH NO USA', que no participa del
    cálculo del factor incremental). Para el par
    CLIENTE FORMATO <-> CLIENTE TH USA se usa
    `balancear_formato_vs_thusa`, que sí garantiza igualdad exacta.

    Parameters
    ----------
    targets : list[str] | None
        Si se especifica, solo se balancean esos valores de
        TARGET_CLIENTE (además de target_base, que siempre se
        recalcula como referencia). Si es None, se balancean todos
        los targets presentes en el dataframe.
    """

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

    if targets is not None:
        # OJO: no se agrega target_base aquí a propósito. Cuando esta
        # función se usa solo para balancear grupos "secundarios"
        # (ej. CLIENTE TH NO USA), target_base (CLIENTE TH USA) NO debe
        # volver a muestrearse aquí, porque ya fue emparejado de forma
        # exacta contra CLIENTE FORMATO en `balancear_formato_vs_thusa`.
        # Si se vuelve a muestrear aquí se generaría un segundo
        # subconjunto de TH USA distinto al ya emparejado, y al
        # concatenar ambos resultados se duplicarían/mezclarían
        # clientes TH USA, rompiendo la igualdad ya lograda.
        total_por_target = {
            k: v
            for k, v in total_por_target.items()
            if k in set(targets)
        }

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


def balancear_formato_a_thusa(
    df,
    var1,
    var2,
    grupo_control='CLIENTE FORMATO',
    grupo_comparativo='CLIENTE TH USA',
    random_state=42
):
    """Ajusta CLIENTE FORMATO para igualar la distribución de TH USA.

    A diferencia del emparejamiento por mínimo común (que recorta a
    AMBOS grupos), aquí CLIENTE TH USA queda 100% intacto — no se le
    quita ningún cliente — y es CLIENTE FORMATO el que se
    submuestrea para que su distribución (%) por estrato (var1, var2)
    quede EXACTAMENTE igual a la de TH USA.

    Para que esto sea posible sin "quedarse corto" en ningún estrato
    (lo que generaría un desbalance, como pasaba en la versión
    original), primero se calcula el N máximo que FORMATO puede
    alcanzar respetando las proporciones de TH USA en TODOS los
    estratos a la vez:

    N_max = floor( min_estrato( pool_FORMATO_estrato / pct_THUSA_estrato))

    Es decir, el estrato más "escaso" en FORMATO (relativo a su peso
    en TH USA) determina cuántos clientes de FORMATO se pueden usar
    en total sin romper la proporción. Luego se muestrea
    n_estrato = round(pct_THUSA_estrato * N_max) clientes de FORMATO
    por estrato.

    Caso borde: si TH USA tiene clientes en un estrato donde FORMATO
    no tiene NINGUNO, ese estrato es matemáticamente imposible de
    igualar (no hay de dónde sacar clientes de FORMATO). Ese estrato
    se excluye del cálculo de N_max, se samplea el resto igual, y
    quedará marcado como IGUALADO=False en `auditar_balanceo` para
    que se vea explícitamente en el reporte — no se oculta el
    problema.
    """

    np.random.seed(random_state)  # noqa: NPY002

    base_df = df.drop_duplicates(
        subset=['CUSTOMER_KEY']
    )

    df_control = base_df[
        base_df['TARGET_CLIENTE'] == grupo_control
    ]

    df_comparativo = base_df[
        base_df['TARGET_CLIENTE'] == grupo_comparativo
    ]

    dist_comparativo = (
        df_comparativo
        .groupby([var1, var2])['CUSTOMER_KEY']
        .nunique()
        .reset_index(name='n')
    )

    total_comparativo = dist_comparativo['n'].sum()

    dist_comparativo['pct'] = (
        dist_comparativo['n'] / total_comparativo
    )

    pool_control = (
        df_control
        .groupby([var1, var2])['CUSTOMER_KEY']
        .nunique()
        .reset_index(name='pool')
    )

    dist = dist_comparativo.merge(
        pool_control, on=[var1, var2], how='left'
    )
    dist['pool'] = dist['pool'].fillna(0)

    # Estratos donde FORMATO no tiene pool no pueden usarse para
    # calcular el N_max (no hay de dónde tomar clientes ahí).
    dist_utilizable = dist[dist['pool'] > 0].copy()

    if dist_utilizable.empty:
        msg = (
            'FORMATO no tiene clientes en ningún estrato en común '
            'con TH USA; no es posible igualar la distribución.'
        )
        raise ValueError(msg)

    dist_utilizable['n_max_estrato'] = (
        dist_utilizable['pool'] / dist_utilizable['pct']
    )

    n_max = int(
        np.floor(dist_utilizable['n_max_estrato'].min())
    )

    if n_max <= 0:
        msg = (
            'El pool de FORMATO es insuficiente para replicar la '
            'distribución de TH USA en al menos un estrato.'
        )
        raise ValueError(msg)

    ids_control = []

    for _, row in dist_utilizable.iterrows():

        g1 = row[var1]
        g2 = row[var2]

        n_sample = round(
            row['pct'] * n_max
        )

        if n_sample <= 0:
            continue

        pool = (
            df_control[
                (df_control[var1] == g1)
                &
                (df_control[var2] == g2)
            ]
            .sort_values('CUSTOMER_KEY')
        )

        # Salvaguarda numérica por redondeo (no debería recortar,
        # ya que n_max se calculó justamente para que esto no pase).
        n_sample = min(n_sample, len(pool))

        sample_ids = pool.sample(
            n=n_sample,
            random_state=random_state
        )['CUSTOMER_KEY']

        ids_control.extend(
            sample_ids.tolist()
        )

    ids_finales = (
        ids_control
        + df_comparativo['CUSTOMER_KEY'].tolist()
    )

    return (
        df[
            df['CUSTOMER_KEY'].isin(ids_finales)
        ]
        .reset_index(drop=True)
    )


# ------------------------------------------------------------------------
# Auditoría de balanceo (control vs comparativo)
# ------------------------------------------------------------------------

TOLERANCIA_PP = 1.0  # puntos porcentuales de diferencia máxima aceptada


def _tabla_distribucion(df_grupo, variables):
    """Cuenta y % de clientes únicos por combinación de variables."""

    tabla = (
        df_grupo
        .drop_duplicates(subset=['CUSTOMER_KEY'])
        .groupby(variables)['CUSTOMER_KEY']
        .nunique()
        .reset_index(name='Q')
    )

    total = tabla['Q'].sum()

    tabla['PCT'] = (
        (tabla['Q'] / total * 100).round(2)
        if total > 0 else 0.0
    )

    return tabla


def auditar_balanceo(
    df_grupo,
    banner,
    tipo,
    var1,
    var2,
    grupo_control='CLIENTE FORMATO',
    grupo_comparativo='CLIENTE TH USA',
    tolerancia_pp=TOLERANCIA_PP
):
    """Compara la distribución de control vs comparativo.

    Retorna dos dataframes:
      - tabla_univariante: Q y % por cada variable por separado
        (var1, var2), control vs comparativo.
      - tabla_bivariante: Q y % por la combinación (var1, var2),
        control vs comparativo.
    """

    df_control = df_grupo[
        df_grupo['TARGET_CLIENTE'] == grupo_control
    ]

    df_comparativo = df_grupo[
        df_grupo['TARGET_CLIENTE'] == grupo_comparativo
    ]

    filas_univariante = []

    for var in (var1, var2):

        t_control = _tabla_distribucion(
            df_control, [var]
        ).rename(
            columns={'Q': 'Q_CONTROL', 'PCT': 'PCT_CONTROL'}
        )

        t_comparativo = _tabla_distribucion(
            df_comparativo, [var]
        ).rename(
            columns={'Q': 'Q_COMPARATIVO', 'PCT': 'PCT_COMPARATIVO'}
        )

        merged = t_control.merge(
            t_comparativo, on=var, how='outer'
        ).fillna(0)

        merged['DIFF_PCT'] = (
            merged['PCT_CONTROL'] - merged['PCT_COMPARATIVO']
        ).round(2)

        merged['IGUALADO'] = (
            merged['DIFF_PCT'].abs() <= tolerancia_pp
        )

        merged = merged.rename(columns={var: 'CATEGORIA'})
        merged.insert(0, 'VARIABLE', var)
        merged.insert(0, 'TIPO_CLIENTE', tipo)
        merged.insert(0, 'STORE_BANNER', banner)

        filas_univariante.append(merged)

    tabla_univariante = pd.concat(
        filas_univariante, ignore_index=True
    )

    t_control_biv = _tabla_distribucion(
        df_control, [var1, var2]
    ).rename(
        columns={'Q': 'Q_CONTROL', 'PCT': 'PCT_CONTROL'}
    )

    t_comparativo_biv = _tabla_distribucion(
        df_comparativo, [var1, var2]
    ).rename(
        columns={'Q': 'Q_COMPARATIVO', 'PCT': 'PCT_COMPARATIVO'}
    )

    tabla_bivariante = t_control_biv.merge(
        t_comparativo_biv, on=[var1, var2], how='outer'
    ).fillna(0)

    tabla_bivariante['DIFF_PCT'] = (
        tabla_bivariante['PCT_CONTROL']
        - tabla_bivariante['PCT_COMPARATIVO']
    ).round(2)

    tabla_bivariante['IGUALADO'] = (
        tabla_bivariante['DIFF_PCT'].abs() <= tolerancia_pp
    )

    tabla_bivariante = tabla_bivariante.rename(
        columns={var1: 'VAR1_VALOR', var2: 'VAR2_VALOR'}
    )
    tabla_bivariante.insert(0, 'VAR2_NOMBRE', var2)
    tabla_bivariante.insert(0, 'VAR1_NOMBRE', var1)
    tabla_bivariante.insert(0, 'TIPO_CLIENTE', tipo)
    tabla_bivariante.insert(0, 'STORE_BANNER', banner)

    return tabla_univariante, tabla_bivariante


def guardar_auditoria_json(
    tabla_univariante,
    tabla_bivariante,
    anomes_cerrado,
    output_dir='.'
):
    """Guarda la auditoría de balanceo en un único JSON con dos tablas."""

    salida = {
        'anomes_cerrado': anomes_cerrado,
        'tolerancia_pp': TOLERANCIA_PP,
        'grupo_control': 'CLIENTE FORMATO',
        'grupo_comparativo': 'CLIENTE TH USA',
        'tabla_univariante': json.loads(
            tabla_univariante.to_json(orient='records')
        ),
        'tabla_bivariante': json.loads(
            tabla_bivariante.to_json(orient='records')
        )
    }

    path_salida = os.path.join(
        output_dir,
        f'auditoria_balanceo_unipay_{anomes_cerrado}.json'
    )

    with open(path_salida, 'w', encoding='utf-8') as f:
        json.dump(salida, f, ensure_ascii=False, indent=2)

    logging.info(
        f'Auditoría de balanceo guardada en: {path_salida}'
    )

    return path_salida


def balancear_clientes(df_principal):
    """Balancea los clientes según las reglas definidas.

    - CLIENTE FORMATO se ajusta para igualar la distribución de
      CLIENTE TH USA (TH USA queda intacto, sin reducir su N).
    - CLIENTE TH NO USA: se balancea de forma aproximada por
      proporción respecto a CLIENTE TH USA (no participa del
      cálculo del factor incremental, solo se reporta).

    Retorna una tupla (df_balanceado, tabla_univariante, tabla_bivariante)
    con el dataframe balanceado y las tablas de auditoría consolidadas
    para todos los (STORE_BANNER, TIPO_CLIENTE).
    """

    dfs_balanceados = []
    tablas_univariante = []
    tablas_bivariante = []

    for (banner, tipo), (var1, var2) in REGLAS_BALANCEO.items():

        df_tmp = df_principal[
            (df_principal['STORE_BANNER'] == banner)
            &
            (df_principal['TIPO_CLIENTE'] == tipo)
        ].copy()

        if df_tmp.empty:
            continue

        df_formato_thusa = balancear_formato_a_thusa(
            df=df_tmp,
            var1=var1,
            var2=var2
        )

        df_th_no_usa = balancear_targets_full(
            df=df_tmp,
            var1=var1,
            var2=var2,
            target_base='CLIENTE TH USA',
            targets=['CLIENTE TH NO USA']
        )

        df_balanceado_grupo = pd.concat(
            [df_formato_thusa, df_th_no_usa],
            ignore_index=True
        )

        dfs_balanceados.append(
            df_balanceado_grupo
        )

        tabla_uni, tabla_biv = auditar_balanceo(
            df_grupo=df_balanceado_grupo,
            banner=banner,
            tipo=tipo,
            var1=var1,
            var2=var2
        )

        tablas_univariante.append(tabla_uni)
        tablas_bivariante.append(tabla_biv)

    if not dfs_balanceados:
        msg = 'No fue posible balancear ningún segmento.'
        raise ValueError(
            msg
        )

    df_balanceado = pd.concat(
        dfs_balanceados,
        ignore_index=True
    )

    tabla_univariante = pd.concat(
        tablas_univariante, ignore_index=True
    )

    tabla_bivariante = pd.concat(
        tablas_bivariante, ignore_index=True
    )

    return df_balanceado, tabla_univariante, tabla_bivariante


def calcular_tablas_gasto(df_balanceado):
    """Calcula las tablas de gasto promedio por formato."""

    tablas_gasto = {}

    for banner, tipo in REGLAS_BALANCEO:

        df_tmp = df_balanceado[
            (df_balanceado['STORE_BANNER'] == banner)
            &
            (df_balanceado['TIPO_CLIENTE'] == tipo)
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

        tablas_gasto[(banner, tipo)] = (
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
    store_banner,
    tipo_cliente
):
    """Calcula el factor incremental para un formato."""

    tabla = tabla_gasto.reset_index().copy()

    unipay_th = (
        tabla.loc[
            tabla['MEDIO_DE_PAGO'] == 'Unipay',
            'CLIENTE TH USA'
        ]
        .iloc[0]
    )

    tabla = tabla[
        tabla['MEDIO_DE_PAGO'] != 'Unipay'
    ].copy()

    tabla['DIFF'] = (
        tabla['CLIENTE TH USA']
        -
        tabla['CLIENTE FORMATO']
    )

    negativas = (
        tabla.loc[
            tabla['DIFF'] < 0,
            'DIFF'
        ].sum()
    )

    a = unipay_th + negativas

    factor = a / unipay_th

    return pd.DataFrame(
        {
            'STORE_BANNER': [store_banner],
            'TIPO_CLIENTE': [tipo_cliente],
            'UNIPAY_TH': [unipay_th],
            'A': [a],
            'FACTOR_INCREMENTAL': [factor],
            'FACTOR_INCREMENTAL_PCT': [
                round(factor * 100, 1)
            ]
        }
    )


def calcular_incremental(tablas_gasto):
    """Calcula el factor incremental para todos los formatos."""

    resultados = []

    for (banner, tipo), tabla_gasto in tablas_gasto.items():

        resultados.append(

            calcular_factor_incremental(
                tabla_gasto=tabla_gasto,
                store_banner=banner,
                tipo_cliente=tipo
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
                'TIPO_CLIENTE',
                'VENTA_NETA'
            ]
        ],
        on=[
            'STORE_BANNER',
            'TIPO_CLIENTE'
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
                'TIPO_CLIENTE',
                'COSTO_PROMOCIONAL_NETO'
            ]
        ],
        on=[
            'STORE_BANNER',
            'TIPO_CLIENTE'
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
            'TIPO_CLIENTE',
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

    usuario = 'unipay_incremental'

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

    (
        df_balanceado,
        tabla_univariante,
        tabla_bivariante
    ) = balancear_clientes(
        df_principal
    )

    # PERIODO se agrega recién aquí (no viene de balancear_clientes)
    # para poder historizar estas tablas en BigQuery igual que la
    # tabla principal (delete + append por PERIODO).
    tabla_univariante['PERIODO'] = anomes_cerrado
    tabla_bivariante['PERIODO'] = anomes_cerrado

    # uploadFrame castea por POSICIÓN según el orden del JSON de
    # esquema (mismo comportamiento que ya usa construir_resultado()
    # para la tabla principal), así que hay que reordenar las
    # columnas del DataFrame para que calcen exacto con el orden
    # declarado en los ingest_*.json. Si no se hace esto, un valor de
    # una columna puede terminar casteado con el tipo de OTRA columna
    # (ej. PERIODO cayendo en la posición de IGUALADO) y pyarrow
    # tira ArrowInvalid al no poder parsear el valor.
    tabla_univariante = tabla_univariante[
        [
            'PERIODO',
            'STORE_BANNER',
            'TIPO_CLIENTE',
            'VARIABLE',
            'CATEGORIA',
            'Q_CONTROL',
            'PCT_CONTROL',
            'Q_COMPARATIVO',
            'PCT_COMPARATIVO',
            'DIFF_PCT',
            'IGUALADO'
        ]
    ]

    tabla_bivariante = tabla_bivariante[
        [
            'PERIODO',
            'STORE_BANNER',
            'TIPO_CLIENTE',
            'VAR1_NOMBRE',
            'VAR2_NOMBRE',
            'VAR1_VALOR',
            'VAR2_VALOR',
            'Q_CONTROL',
            'PCT_CONTROL',
            'Q_COMPARATIVO',
            'PCT_COMPARATIVO',
            'DIFF_PCT',
            'IGUALADO'
        ]
    ]

    path_auditoria = guardar_auditoria_json(
        tabla_univariante=tabla_univariante,
        tabla_bivariante=tabla_bivariante,
        anomes_cerrado=anomes_cerrado
    )

    logging.info(
        f'Auditoría de balanceo (local): {path_auditoria}'
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
    tabla = 'UNIPAY_INCREMENTAL'

    path_table = (
        f'{project_id}.{esquema}.{tabla}'
    )

    createTableFromJSON(
        table_ddl_json_path=os.path.join(
            'gbq_objects',
            'ingest_incremental_unipay.json'
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
            'ingest_incremental_unipay.json'
        ),
        project=project_id,
        gbq_client=gbq_client,
        if_exists='append'
    )

    # ---------------------------------------------------------------------
    # Subida de tablas de auditoría de balanceo (control vs comparativo)
    # ---------------------------------------------------------------------

    tabla_uni_bq = 'UNIPAY_AUDITORIA_BALANCEO_UNIVARIANTE'
    tabla_biv_bq = 'UNIPAY_AUDITORIA_BALANCEO_BIVARIANTE'

    path_tabla_uni_bq = (
        f'{project_id}.{esquema}.{tabla_uni_bq}'
    )
    path_tabla_biv_bq = (
        f'{project_id}.{esquema}.{tabla_biv_bq}'
    )

    createTableFromJSON(
        table_ddl_json_path=os.path.join(
            'gbq_objects',
            'ingest_auditoria_balanceo_univariante.json'
        ),
        project=project_id,
        gbq_client=gbq_client,
        if_exists='ignore'
    )

    createTableFromJSON(
        table_ddl_json_path=os.path.join(
            'gbq_objects',
            'ingest_auditoria_balanceo_bivariante.json'
        ),
        project=project_id,
        gbq_client=gbq_client,
        if_exists='ignore'
    )

    deleteFromTable(
        table_ref=path_tabla_uni_bq,
        where_clause=(
            f"PERIODO = '{anomes_cerrado}'"
        ),
        gbq_client=gbq_client
    )

    deleteFromTable(
        table_ref=path_tabla_biv_bq,
        where_clause=(
            f"PERIODO = '{anomes_cerrado}'"
        ),
        gbq_client=gbq_client
    )

    uploadFrame(
        tabla_univariante,
        table_ddl_json_path=os.path.join(
            'gbq_objects',
            'ingest_auditoria_balanceo_univariante.json'
        ),
        project=project_id,
        gbq_client=gbq_client,
        if_exists='append'
    )

    uploadFrame(
        tabla_bivariante,
        table_ddl_json_path=os.path.join(
            'gbq_objects',
            'ingest_auditoria_balanceo_bivariante.json'
        ),
        project=project_id,
        gbq_client=gbq_client,
        if_exists='append'
    )

    logging.info(
        'Auditoría de balanceo subida a BigQuery: '
        f'{path_tabla_uni_bq} / {path_tabla_biv_bq}'
    )

    logging.info(
        'Proceso finalizado correctamente.'
    )


if __name__ == '__main__':
    main()
