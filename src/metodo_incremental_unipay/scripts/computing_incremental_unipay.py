
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


def auditar_balanceo(  # noqa: D417
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

    Parameters
    ----------
    grupo_comparativo : str | list | tuple
        Puede ser un único valor de TARGET_CLIENTE (ej.
        'CLIENTE TH USA') o una lista/tupla de valores, en cuyo caso
        se comparan combinados (ej. ('CLIENTE TH USA',
        'CLIENTE TH NO USA') para comparar contra todos los
        tarjetahabientes juntos, replicando cómo se calibra
        `balancear_formato_a_th_combinado`).

    Retorna dos dataframes:
      - tabla_univariante: Q y % por cada variable por separado
        (var1, var2), control vs comparativo.
      - tabla_bivariante: Q y % por la combinación (var1, var2),
        control vs comparativo.
    """

    df_control = df_grupo[
        df_grupo['TARGET_CLIENTE'] == grupo_control
    ]

    if isinstance(grupo_comparativo, (list, tuple, set)):  # noqa: UP038
        df_comparativo = df_grupo[
            df_grupo['TARGET_CLIENTE'].isin(grupo_comparativo)
        ]
    else:
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


def guardar_json_generico(
    datos,
    nombre_archivo,
    anomes_cerrado,
    output_dir='.'
):
    """Guarda un DataFrame como JSON local (records), de forma
    genérica — para respaldos que no necesitan la estructura
    específica de `guardar_auditoria_json` o
    `guardar_tablas_gasto_json`.
    """

    registros = json.loads(
        datos.to_json(orient='records')
    )

    salida = {
        'anomes_cerrado': anomes_cerrado,
        'registros': registros
    }

    path_salida = os.path.join(
        output_dir,
        nombre_archivo
    )

    with open(path_salida, 'w', encoding='utf-8') as f:
        json.dump(salida, f, ensure_ascii=False, indent=2)

    logging.info(
        f'JSON guardado en: {path_salida}'
    )

    return path_salida


def guardar_tablas_gasto_json(  # noqa: D417
    tablas_gasto,
    df_incremental,
    anomes_cerrado,
    metodo,
    output_dir='.'
):
    """Guarda las tablas de gasto promedio + el factor incremental
    resultante, en un único JSON consultable directamente.

    Por cada (STORE_BANNER, TIPO_CLIENTE) queda:
      - tabla_gasto_promedio: la tabla de `calcular_tablas_gasto`
        (gasto promedio por MEDIO_DE_PAGO x TARGET_CLIENTE) tal cual
        se usó para el cálculo — para poder revisar a mano de dónde
        salió el % incremental sin tener que ir a BigQuery ni
        reconstruir el pivot en memoria.
      - factor_incremental: la fila correspondiente de
        `calcular_incremental` (incluye UNIPAY_TH, A,
        FACTOR_INCREMENTAL, FACTOR_INCREMENTAL_PCT — el detalle
        completo del cálculo, no solo el % final).

    Parameters
    ----------
    metodo : str
        Etiqueta para el nombre de archivo (ej. 'legacy',
        'historico', 'historico_sin_tope'), para no pisar el JSON de
        un método con el de otro.
    """

    segmentos = {}

    for (banner, tipo), tabla in tablas_gasto.items():

        clave = f'{banner}|{tipo}'

        tabla_records = json.loads(
            tabla.reset_index().to_json(orient='records')
        )

        fila_incremental = df_incremental[
            (df_incremental['STORE_BANNER'] == banner)
            &
            (df_incremental['TIPO_CLIENTE'] == tipo)
        ]

        incremental_records = json.loads(
            fila_incremental.to_json(orient='records')
        )

        detalle_incremental = (
            incremental_records[0]
            if incremental_records
            else {}
        )

        segmentos[clave] = {
            'store_banner': banner,
            'tipo_cliente': tipo,
            'tabla_gasto_promedio': tabla_records,
            'factor_incremental': detalle_incremental
        }

    salida = {
        'anomes_cerrado': anomes_cerrado,
        'metodo': metodo,
        'segmentos': segmentos
    }

    path_salida = os.path.join(
        output_dir,
        f'tablas_gasto_{metodo}_{anomes_cerrado}.json'
    )

    with open(path_salida, 'w', encoding='utf-8') as f:
        json.dump(salida, f, ensure_ascii=False, indent=2)

    logging.info(
        f'Tablas de gasto ({metodo}) guardadas en: {path_salida}'
    )

    return path_salida


def construir_tabla_gasto_larga(
    tablas_gasto,
    df_incremental,
    anomes_cerrado
):
    """Arma la tabla de gasto por medio de pago, comparando
    DIRECTAMENTE CLIENTE FORMATO vs CLIENTE TH USA — los dos únicos
    grupos que realmente participan en `calcular_factor_incremental`
    (CLIENTE TH NO USA se descarta acá; se audita por separado en las
    tablas `_ORIGINAL`/`_HISTORICO`, pero no interviene en este
    cálculo).

    Una fila por (STORE_BANNER, TIPO_CLIENTE, MEDIO_DE_PAGO), con
    GASTO_FORMATO y GASTO_TH_USA lado a lado, más DIFERENCIA y una
    interpretación en texto — replicando exactamente la lógica de
    `calcular_factor_incremental`:

      DIFERENCIA = GASTO_TH_USA - GASTO_FORMATO (excepto en la fila
      de Unipay, que es el punto de partida del cálculo, no una
      resta)

      Si DIFERENCIA < 0 (FORMATO gasta más que TH USA en ese medio):
      se interpreta como sustitución — esa plata se resta del gasto
      Unipay al calcular el % incremental.

    Pensada para dar transparencia directa del cálculo a quien
    consulte la tabla en BigQuery, sin tener que reconstruirlo a
    mano.
    """

    filas = []

    for (banner, tipo), tabla in tablas_gasto.items():

        tabla_comparacion = (
            tabla
            .reset_index()[
                ['MEDIO_DE_PAGO', 'CLIENTE FORMATO', 'CLIENTE TH USA']
            ]
            .rename(
                columns={
                    'CLIENTE FORMATO': 'GASTO_FORMATO',
                    'CLIENTE TH USA': 'GASTO_TH_USA'
                }
            )
        )

        tabla_comparacion['DIFERENCIA'] = (
            tabla_comparacion['GASTO_TH_USA']
            -
            tabla_comparacion['GASTO_FORMATO']
        )

        def _interpretar(row):
            if row['MEDIO_DE_PAGO'] == 'Unipay':
                return (
                    'Gasto base de Unipay en TH USA — punto de '
                    'partida del cálculo (no es una resta)'
                )
            if pd.isna(row['DIFERENCIA']):
                return 'Sin datos suficientes para comparar'
            if row['DIFERENCIA'] < 0:
                return (
                    'FORMATO gasta más que TH USA en este medio — '
                    'se resta como sustitución'
                )
            return (
                'TH USA gasta igual o más — no se resta '
                '(no indica sustitución)'
            )

        tabla_comparacion['INTERPRETACION'] = (
            tabla_comparacion.apply(_interpretar, axis=1)
        )

        tabla_comparacion['ES_SUSTITUCION'] = (
            (tabla_comparacion['MEDIO_DE_PAGO'] != 'Unipay')
            &
            (tabla_comparacion['DIFERENCIA'] < 0)
        )

        tabla_comparacion.insert(0, 'TIPO_CLIENTE', tipo)
        tabla_comparacion.insert(0, 'STORE_BANNER', banner)

        fila_incremental = df_incremental[
            (df_incremental['STORE_BANNER'] == banner)
            &
            (df_incremental['TIPO_CLIENTE'] == tipo)
        ]

        factor_pct = (
            fila_incremental['FACTOR_INCREMENTAL_PCT'].iloc[0]
            if not fila_incremental.empty
            else None
        )

        tabla_comparacion['FACTOR_INCREMENTAL_PCT'] = factor_pct

        filas.append(tabla_comparacion)

    tabla_gasto_comparada = pd.concat(filas, ignore_index=True)

    # .insert() falla si la columna ya existe — se saca primero por
    # si acaso, para no depender de que nunca vaya a venir PERIODO
    # desde el origen de tablas_gasto.
    if 'PERIODO' in tabla_gasto_comparada.columns:
        tabla_gasto_comparada = tabla_gasto_comparada.drop(
            columns=['PERIODO']
        )

    tabla_gasto_comparada.insert(0, 'PERIODO', anomes_cerrado)

    # GASTO_FORMATO/GASTO_TH_USA pueden venir NaN (ej. FORMATO nunca
    # usa Unipay) — se deja como null explícito, no se rellena con 0.
    return tabla_gasto_comparada[
        [
            'PERIODO',
            'STORE_BANNER',
            'TIPO_CLIENTE',
            'MEDIO_DE_PAGO',
            'GASTO_FORMATO',
            'GASTO_TH_USA',
            'DIFERENCIA',
            'ES_SUSTITUCION',
            'INTERPRETACION',
            'FACTOR_INCREMENTAL_PCT'
        ]
    ]


def construir_detalle_clientes_balanceados(df_balanceado, anomes_cerrado):
    """Detalle cliente por cliente de los 3 grupos ya balanceados:
    CLIENTE FORMATO, CLIENTE TH USA y CLIENTE TH NO USA — los que
    efectivamente se usan en el balanceo (aunque TH NO USA no entre
    a la fórmula de `calcular_factor_incremental`, sí forma parte
    del grupo de referencia contra el que se calibra FORMATO, así
    que queda incluido para trazabilidad completa).

    Una fila por (CUSTOMER_KEY, MEDIO_DE_PAGO) — igual que vienen los
    datos crudos — con el tipo de cliente, el medio de pago, el monto
    (VENTA_NETA) y todas las variables de elección usadas para el
    balanceo (ZONA, SHABITS, RANGO_EDAD, CLASIFICACION_CLIENTE).

    Sirve para poder trazar/auditar exactamente qué clientes
    concretos entraron al cálculo de un periodo dado, no solo los
    agregados.
    """

    df_detalle = df_balanceado[
        df_balanceado['TARGET_CLIENTE'].isin(
            ['CLIENTE FORMATO', 'CLIENTE TH USA', 'CLIENTE TH NO USA']
        )
    ].copy()

    # df_balanceado ya trae PERIODO desde query_principal (ANOMES_CERRADO
    # AS PERIODO). Se saca y se vuelve a insertar con el anomes_cerrado
    # "canónico" del DAG, en la posición 0 — tanto para evitar el
    # ValueError de .insert() sobre una columna que ya existe, como para
    # asegurar que el valor sea exactamente el mismo que usan las demás
    # tablas del periodo (por si alguna vez difirieran).
    if 'PERIODO' in df_detalle.columns:
        df_detalle = df_detalle.drop(columns=['PERIODO'])

    df_detalle.insert(0, 'PERIODO', anomes_cerrado)

    columnas = [
        'PERIODO',
        'CUSTOMER_KEY',
        'STORE_BANNER',
        'TIPO_CLIENTE',
        'TARGET_CLIENTE',
        'MEDIO_DE_PAGO',
        'VENTA_NETA',
        'ZONA',
        'SHABITS',
        'RANGO_EDAD',
        'CLASIFICACION_CLIENTE'
    ]

    # Solo se incluyen las columnas que efectivamente vienen en
    # query_principal — si algún banner no trae CLASIFICACION_CLIENTE
    # o ZONA, por ejemplo, no se cae, simplemente no aparece.
    columnas_presentes = [
        c for c in columnas if c in df_detalle.columns
    ]

    return df_detalle[columnas_presentes].reset_index(drop=True)


def balancear_formato_a_th_combinado(
    df,
    var1,
    var2,
    grupo_control='CLIENTE FORMATO',
    grupos_referencia=('CLIENTE TH USA', 'CLIENTE TH NO USA'),
    random_state=42,
    n_objetivo=10_000
):
    """Replica el método manual histórico (script Excel/ETL antiguo).

    A diferencia de `balancear_formato_a_thusa` (que calibra FORMATO
    contra CLIENTE TH USA específicamente), esta función calibra
    FORMATO para que su distribución (var1, var2) se parezca a la de
    **CLIENTE TH USA + CLIENTE TH NO USA combinados** — es decir,
    todos los tarjetahabientes juntos, tal como hacía
    `etl_grupo_control()` en el proceso manual (agrupaba por
    TIPO_BASE == 'TarjetaHabiente' sin distinguir si usaban o no la
    tarjeta).

    ADVERTENCIA METODOLÓGICA (documentada a propósito, no oculta):
    `calcular_factor_incremental` sigue restando específicamente
    contra CLIENTE TH USA (no contra el grupo combinado usado aquí
    para calibrar). Esto reproduce intencionalmente el mismo
    descalce que tenía el proceso histórico: el grupo de control se
    calibra contra un grupo de referencia (TH USA + TH NO USA)
    distinto al grupo contra el que finalmente se compara en el
    cálculo del incremental (TH USA solo). Se implementa así porque
    es la directriz de negocio solicitada — replicar el método
    histórico independiente de si es estadísticamente correcto —,
    no porque se considere el método recomendado. Para la versión
    correctamente calibrada, ver `balancear_formato_a_thusa`.

    n_objetivo por defecto es 10.000, replicando el tamaño de
    muestra fijo que usaba el proceso manual (`Q_ALEATORIO` con
    `.mul(10_000)`). Si el pool real no alcanza, se usa el máximo
    posible (igual que en `balancear_formato_a_thusa`).
    """

    np.random.seed(random_state)  # noqa: NPY002

    base_df = df.drop_duplicates(
        subset=['CUSTOMER_KEY']
    )

    df_control = base_df[
        base_df['TARGET_CLIENTE'] == grupo_control
    ]

    df_referencia = base_df[
        base_df['TARGET_CLIENTE'].isin(grupos_referencia)
    ]

    dist_referencia = (
        df_referencia
        .groupby([var1, var2])['CUSTOMER_KEY']
        .nunique()
        .reset_index(name='n')
    )

    total_referencia = dist_referencia['n'].sum()

    dist_referencia['pct'] = (
        dist_referencia['n'] / total_referencia
    )

    pool_control = (
        df_control
        .groupby([var1, var2])['CUSTOMER_KEY']
        .nunique()
        .reset_index(name='pool')
    )

    dist = dist_referencia.merge(
        pool_control, on=[var1, var2], how='left'
    )
    dist['pool'] = dist['pool'].fillna(0)

    dist_utilizable = dist[dist['pool'] > 0].copy()

    if dist_utilizable.empty:
        msg = (
            'FORMATO no tiene clientes en ningún estrato en común '
            'con TH USA + TH NO USA; no es posible calibrar.'
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
            'distribución combinada en al menos un estrato.'
        )
        raise ValueError(msg)

    # Si se pidió un N objetivo, se respeta ese tope salvo que el
    # pool real no alcance. Si n_objetivo es None, se usa el N
    # máximo alcanzable (sin tope).
    n_final = (
        n_max
        if n_objetivo is None
        else min(n_objetivo, n_max)
    )

    ids_control = []

    for _, row in dist_utilizable.iterrows():

        g1 = row[var1]
        g2 = row[var2]

        n_sample = round(
            row['pct'] * n_final
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
        + df_referencia['CUSTOMER_KEY'].tolist()
    )

    return (
        df[
            df['CUSTOMER_KEY'].isin(ids_finales)
        ]
        .reset_index(drop=True)
    )


def balancear_clientes_metodo_historico(df_principal, n_objetivo=10_000):
    """Balanceo que replica el método manual histórico, por segmento.

    Usa `balancear_formato_a_th_combinado` (FORMATO calibrado contra
    TH USA + TH NO USA combinados) para cada (STORE_BANNER,
    TIPO_CLIENTE) de `REGLAS_BALANCEO`, con N fijo (10.000 por
    defecto, igual que el proceso manual).

    El dataframe resultante se usa después con el mismo
    `calcular_tablas_gasto` / `calcular_incremental` de siempre, que
    compara contra CLIENTE TH USA — reproduciendo a propósito el
    descalce del método histórico (ver advertencia en
    `balancear_formato_a_th_combinado`).

    LAS TABLAS DE AUDITORÍA QUE RETORNA ESTA FUNCIÓN comparan FORMATO
    contra el grupo COMBINADO (TH USA + TH NO USA) — es decir, contra
    lo que efectivamente se calibró, replicando la comparación
    Grupo Control vs TarjetaHabiente del proceso Excel (debería dar
    ~0.00-0.01pp de diferencia, igual que se confirmó con datos
    reales). Esto documenta que el MECANISMO de calibración funciona
    correctamente.

    OJO — esto NO es la misma comparación que importa para el
    resultado del incremental: `calcular_incremental` sigue restando
    contra CLIENTE TH USA solo, no contra el grupo combinado. Por
    eso, aunque estas tablas den ~0.00pp (balanceo bien calibrado
    contra el grupo combinado), el `FACTOR_INCREMENTAL_PCT` resultante
    en `UNIPAY_INCREMENTAL_HISTORICO` puede seguir siendo distinto al
    del método corregido — es el descalce intencional del método
    histórico, que ya no se audita aquí como tabla, pero se sigue
    chequeando internamente (ver el `logging.warning` más abajo).

    Retorna una tupla (df_balanceado, tabla_univariante, tabla_bivariante).
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

        df_balanceado_grupo = balancear_formato_a_th_combinado(
            df=df_tmp,
            var1=var1,
            var2=var2,
            n_objetivo=n_objetivo
        )

        dfs_balanceados.append(
            df_balanceado_grupo
        )

        # Tabla que se persiste: FORMATO vs grupo COMBINADO (TH USA +
        # TH NO USA) — contra lo que efectivamente se calibró.
        # Debería dar ~0.00-0.01pp, replicando Control vs
        # TarjetaHabiente del proceso Excel.
        tabla_uni, tabla_biv = auditar_balanceo(
            df_grupo=df_balanceado_grupo,
            banner=banner,
            tipo=tipo,
            var1=var1,
            var2=var2,
            grupo_comparativo=(
                'CLIENTE TH USA', 'CLIENTE TH NO USA'
            )
        )

        if not tabla_biv['IGUALADO'].all():
            logging.warning(
                f'[{banner}/{tipo}] El mecanismo de calibración del '
                'método histórico NO calzó contra el grupo combinado '
                '(se esperaba ~0.00pp). Revisar '
                'balancear_formato_a_th_combinado.'
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


# Combinaciones (STORE_BANNER, MEDIO_DE_PAGO) que no tienen sentido de
# negocio y se excluyen del cálculo de gasto/incremental (no solo se
# muestran en 0 — se sacan por completo, porque incluirlas distorsiona
# la interpretación de "sustitución de canal"). Ejemplo: Ecommerce es
# 100% online, no debería tener transacciones en Efectivo — si
# aparecen, son un artefacto de datos, no gasto real de ese canal.
EXCLUSIONES_MEDIO_PAGO = {
    'Ecommerce': ['Efectivo'],
}


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

        # Excluir combinaciones banner + medio de pago sin sentido de
        # negocio (ver EXCLUSIONES_MEDIO_PAGO) ANTES de sumar venta,
        # para que ni la tabla de gasto ni el cálculo del incremental
        # las vean.
        medios_excluidos = EXCLUSIONES_MEDIO_PAGO.get(banner, [])
        if medios_excluidos:
            df_tmp = df_tmp[
                ~df_tmp['MEDIO_DE_PAGO'].isin(medios_excluidos)
            ]

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

        tabla = (
            ventas
            .pivot_table(
                index='MEDIO_DE_PAGO',
                columns='TARGET_CLIENTE',
                values='GASTO_PROMEDIO'
            )
            .round(0)
        )

        # FORMATO por definición no usa Unipay (si lo usara, sería
        # TH USA) — cualquier valor no-cero ahí es artefacto de datos
        # (ej. clasificación tardía). Se fuerza a 0 explícitamente.
        # OJO: esto es puramente cosmético — calcular_factor_incremental
        # nunca lee el valor de FORMATO en la fila Unipay (solo usa
        # CLIENTE TH USA ahí), así que este parche NO cambia el
        # FACTOR_INCREMENTAL_PCT, solo corrige lo que se reporta.
        if (
            'Unipay' in tabla.index
            and
            'CLIENTE FORMATO' in tabla.columns
        ):
            tabla.loc['Unipay', 'CLIENTE FORMATO'] = 0.0

        tablas_gasto[(banner, tipo)] = tabla

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
    # Balanceo de clientes — MÉTODO ÚNICO VALIDADO
    # ---------------------------------------------------------------------
    # Tras varias iteraciones comparando métodos (legacy, corregido,
    # réplica histórica con y sin tope de N), el método validado y
    # único que queda en producción es el HISTÓRICO SIN TOPE:
    #
    #   - CLIENTE FORMATO se calibra contra CLIENTE TH USA +
    #     CLIENTE TH NO USA combinados (replicando el proceso manual
    #     histórico), usando el N máximo alcanzable (sin topar a
    #     10.000).
    #   - CLIENTE TH USA queda 100% intacto.
    #   - El cálculo del FACTOR_INCREMENTAL_PCT sigue restando
    #     específicamente contra CLIENTE TH USA (no contra el grupo
    #     combinado), como siempre.
    #
    # Este es ahora el único balanceo que alimenta UNIPAY_INCREMENTAL,
    # UNIPAY_AUDITORIA_BALANCEO_UNIVARIANTE/BIVARIANTE y
    # UNIPAY_GASTO_PROMEDIO.

    (
        df_balanceado,
        tabla_univariante,
        tabla_bivariante
    ) = balancear_clientes_metodo_historico(
        df_principal,
        n_objetivo=None
    )

    tabla_univariante['PERIODO'] = anomes_cerrado
    tabla_bivariante['PERIODO'] = anomes_cerrado

    # uploadFrame castea por POSICIÓN según el orden del JSON de
    # esquema, así que hay que reordenar las columnas del DataFrame
    # para que calcen exacto con el orden declarado en los
    # ingest_*.json.
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
        f'Auditoría de balanceo (respaldo local): {path_auditoria}'
    )

    # ---------------------------------------------------------------------
    # Detalle de clientes balanceados (FORMATO + TH USA)
    # ---------------------------------------------------------------------

    detalle_clientes = construir_detalle_clientes_balanceados(
        df_balanceado=df_balanceado,
        anomes_cerrado=anomes_cerrado
    )

    path_detalle_clientes = guardar_json_generico(
        datos=detalle_clientes,
        nombre_archivo=f'detalle_clientes_balanceados_{anomes_cerrado}.json',
        anomes_cerrado=anomes_cerrado
    )

    logging.info(
        f'Detalle de clientes balanceados (respaldo local): '
        f'{path_detalle_clientes}'
    )

    # ---------------------------------------------------------------------
    # Tablas de gasto y factor incremental
    # ---------------------------------------------------------------------

    tablas_gasto = calcular_tablas_gasto(
        df_balanceado
    )

    df_incremental = calcular_incremental(
        tablas_gasto
    )

    path_tablas_gasto = guardar_tablas_gasto_json(
        tablas_gasto=tablas_gasto,
        df_incremental=df_incremental,
        anomes_cerrado=anomes_cerrado,
        metodo='historico_sin_tope'
    )

    logging.info(
        f'Tablas de gasto + factor incremental (respaldo local): '
        f'{path_tablas_gasto}'
    )

    tabla_gasto_larga = construir_tabla_gasto_larga(
        tablas_gasto=tablas_gasto,
        df_incremental=df_incremental,
        anomes_cerrado=anomes_cerrado
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

    logging.info(
        f'Incremental subido a BigQuery: {path_table}'
    )

    # ---------------------------------------------------------------------
    # Subida de tablas de auditoría de balanceo
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

    # ---------------------------------------------------------------------
    # Subida de la tabla de gasto promedio
    # ---------------------------------------------------------------------

    tabla_gasto_bq = 'UNIPAY_GASTO_PROMEDIO'

    path_tabla_gasto_bq = (
        f'{project_id}.{esquema}.{tabla_gasto_bq}'
    )

    createTableFromJSON(
        table_ddl_json_path=os.path.join(
            'gbq_objects',
            'ingest_gasto_promedio_unipay.json'
        ),
        project=project_id,
        gbq_client=gbq_client,
        if_exists='ignore'
    )

    deleteFromTable(
        table_ref=path_tabla_gasto_bq,
        where_clause=(
            f"PERIODO = '{anomes_cerrado}'"
        ),
        gbq_client=gbq_client
    )

    uploadFrame(
        tabla_gasto_larga,
        table_ddl_json_path=os.path.join(
            'gbq_objects',
            'ingest_gasto_promedio_unipay.json'
        ),
        project=project_id,
        gbq_client=gbq_client,
        if_exists='append'
    )

    logging.info(
        f'Tabla de gasto promedio subida a BigQuery: {path_tabla_gasto_bq}'
    )

    # ---------------------------------------------------------------------
    # Subida del detalle de clientes balanceados
    # ---------------------------------------------------------------------

    tabla_detalle_bq = 'UNIPAY_DETALLE_CLIENTES_BALANCEADOS'

    path_tabla_detalle_bq = (
        f'{project_id}.{esquema}.{tabla_detalle_bq}'
    )

    createTableFromJSON(
        table_ddl_json_path=os.path.join(
            'gbq_objects',
            'ingest_detalle_clientes_balanceados.json'
        ),
        project=project_id,
        gbq_client=gbq_client,
        if_exists='ignore'
    )

    deleteFromTable(
        table_ref=path_tabla_detalle_bq,
        where_clause=(
            f"PERIODO = '{anomes_cerrado}'"
        ),
        gbq_client=gbq_client
    )

    uploadFrame(
        detalle_clientes,
        table_ddl_json_path=os.path.join(
            'gbq_objects',
            'ingest_detalle_clientes_balanceados.json'
        ),
        project=project_id,
        gbq_client=gbq_client,
        if_exists='append'
    )

    logging.info(
        'Detalle de clientes balanceados subido a BigQuery: '
        f'{path_tabla_detalle_bq}'
    )

    logging.info(
        'Proceso finalizado correctamente.'
    )


if __name__ == '__main__':
    main()
