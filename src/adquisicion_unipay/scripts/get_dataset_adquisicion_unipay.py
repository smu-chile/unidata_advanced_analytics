# Default
from __future__ import annotations

import os
import logging
import argparse
import tempfile
from logging import config

# Pip
import numpy as np
import pandas as pd
import pyarrow as pa
import pendulum
import pyarrow.parquet as pq

# Own
from google.cloud import (
    storage,
    bigquery,  # noqa: F401
)
from google.cloud.bigquery import Client

import common.gcp_extended.bigquery as gbq_extended  # noqa: F401
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import readBigQuery


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
SQL_QUERIES = QueryDict({
    # Clientes preaprobados en el periodo establecido
    'preaprobados':
    """
    SELECT CUSTOMER_ID CUSTOMER_KEY,CUPO,GRUPO_RIESGO,PERIODO
    FROM `${gcp_proyect}.${schema}.VW_I_UNICARD_PREAPROBADOS`
    WHERE PERIODO = ${periodo}
    """,

    # Cantidad de tarjetas Unipay que ha aperturado el cliente
    'tarjetas':
    """
    SELECT CUSTOMER_ID CUSTOMER_KEY, COUNT(*) N_TARJETAS
    FROM `${gcp_proyect}.${schema}.VW_I_UNICARD_CARD`
    WHERE FORMAT_DATE('%Y%m',PARSE_DATE('%Y-%m-%d',SUBSCRIPTION_DATE)) < '${periodo}'
    AND CUSTOMER_ID IN (SELECT CUSTOMER_ID
    FROM `${gcp_proyect}.${schema}.VW_I_UNICARD_PREAPROBADOS`
    WHERE PERIODO = ${periodo})
    GROUP BY CUSTOMER_ID
    """,

    # Numero de veces que el cliente lleva siendo preaprobado
    # y la cantidad de cupos distintos que le han ofrecido
    'n_preaprobado_cupos':
    """
    WITH SOLICITARON AS (
    SELECT CUSTOMER_ID,CARD_ID,
    SUBSCRIPTION_DATE,ACTIVATION_DATE,TERMINATION_DATE
    FROM `${gcp_proyect}.${schema}.VW_I_UNICARD_CARD`
    WHERE CUSTOMER_ID IN (SELECT CUSTOMER_ID
    FROM `${gcp_proyect}.${schema}.VW_I_UNICARD_PREAPROBADOS`
    WHERE PERIODO = ${periodo})
    AND FORMAT_DATE('%Y%m', PARSE_DATE('%Y-%m-%d',SUBSCRIPTION_DATE)) <= '${periodo_n1}'
    ),
    PERIODO AS (
    SELECT CARD_ID,PERIOD
    FROM (
        SELECT CARD_ID,PERIOD,
        ROW_NUMBER() OVER (PARTITION BY CARD_ID ORDER BY PERIOD DESC) AS RW
        FROM `${gcp_proyect}.${schema}.VW_I_UNICARD_CARD_STATUS`
        WHERE CARD_ID IN (SELECT CARD_ID FROM SOLICITARON)
    ) t
    ),
    PREAPROBADOS as (
    SELECT CUSTOMER_ID,CUPO,GRUPO_RIESGO,PERIODO
    FROM `${gcp_proyect}.${schema}.VW_I_UNICARD_PREAPROBADOS`
    WHERE PERIODO = ${periodo}
    ),
    PRE_CON_TARJETAS_CERRADAS as (
    SELECT a.CUSTOMER_ID,a.CARD_ID,
    a.SUBSCRIPTION_DATE,a.ACTIVATION_DATE,a.TERMINATION_DATE,
    b.PERIOD as PERIODO
    FROM SOLICITARON a
    LEFT JOIN PERIODO b
    ON a.CARD_ID = b.CARD_ID
    WHERE b.PERIOD <= ${periodo_n1}
    AND a.CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM PREAPROBADOS)
    ),
    PRE_CON_TARJETAS_CERRADAS_PERIODOS AS (
    SELECT a.CUSTOMER_ID,a.CARD_ID,
    CAST(FORMAT_DATE(
    '%Y%m', DATE_ADD(PARSE_DATE(
    '%Y%m', CAST(a.PERIODO AS STRING)), INTERVAL 1 MONTH)) AS INT64) PERIODO_CIERRE,
    b.PERIODO PERIODO_PRE
    FROM PRE_CON_TARJETAS_CERRADAS a
    INNER JOIN `${gcp_proyect}.${schema}.VW_I_UNICARD_PREAPROBADOS` b
    ON a.CUSTOMER_ID = b.CUSTOMER_ID
    WHERE b.PERIODO > CAST(FORMAT_DATE(
    '%Y%m', DATE_ADD(PARSE_DATE(
    '%Y%m', CAST(a.PERIODO AS STRING)), INTERVAL 1 MONTH)) AS INT64)
    ),
    PRE_CERRADAS_PERIODO_VARIABLES AS (
    SELECT CUSTOMER_ID,CARD_ID,PERIODO_CIERRE,PERIODO_PRE
    FROM (
    SELECT CUSTOMER_ID,CARD_ID,PERIODO_CIERRE,PERIODO_PRE,
    ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY PERIODO_PRE asc) rw
    FROM PRE_CON_TARJETAS_CERRADAS_PERIODOS
    ) t
    WHERE rw = 1
    )
    SELECT a.CUSTOMER_ID CUSTOMER_KEY,
    COUNT(DISTINCT b.PERIODO) N_PREAPROBADO, COUNT(DISTINCT b.CUPO) N_CUPOS
    FROM PRE_CERRADAS_PERIODO_VARIABLES a
    INNER JOIN `${gcp_proyect}.${schema}.VW_I_UNICARD_PREAPROBADOS` b
    ON a.CUSTOMER_ID = b.CUSTOMER_ID
    WHERE b.PERIODO >= a.PERIODO_PRE
    AND b.PERIODO <= ${periodo}
    GROUP BY a.CUSTOMER_ID

    UNION ALL

    SELECT CUSTOMER_ID CUSTOMER_KEY,
    COUNT(DISTINCT PERIODO) N_PREAPROBADO, COUNT(DISTINCT CUPO) N_CUPOS
    FROM `${gcp_proyect}.${schema}.VW_I_UNICARD_PREAPROBADOS`
    WHERE CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM PREAPROBADOS)
    AND CUSTOMER_ID NOT IN (SELECT CUSTOMER_ID FROM PRE_CON_TARJETAS_CERRADAS)
    AND PERIODO <= ${periodo}
    GROUP BY CUSTOMER_ID
    """,

    # Shabit Unimarc del cliente en el mes anterior al periodo establecido
    'shabit_unimarc':
    """
    SELECT LAST_MONTHID,CUSTOMER_KEY,SHABIT_UNIMARC
    FROM (
    SELECT MONTHID LAST_MONTHID,CUSTOMER_KEY,SHABIT AS SHABIT_UNIMARC,
    ROW_NUMBER() OVER (PARTITION BY CUSTOMER_KEY ORDER BY MONTHID desc) rw
    FROM `${gcp_proyect}.${schema_1}.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_SHABITS_UNIDATA`
    WHERE CUSTOMER_KEY IN (SELECT CUSTOMER_ID
    FROM `${gcp_proyect}.${schema_2}.VW_I_UNICARD_PREAPROBADOS`
    WHERE PERIODO = ${periodo})
    AND MONTHID <= ${periodo_n2}
    AND SHABIT IS NOT NULL
    ) t
    WHERE rw = 1
    """,

    # Shabit Alvi del cliente en el mes anterior al periodo establecido
    'shabits_alvi':
    """
    SELECT LAST_MONTHID,CUSTOMER_KEY,SHABIT_ALVI
    FROM (
    SELECT MONTHID LAST_MONTHID,CUSTOMER_KEY,SHABIT AS SHABIT_ALVI,
    ROW_NUMBER() OVER (PARTITION BY CUSTOMER_KEY ORDER BY MONTHID desc) rw
    FROM `${gcp_proyect}.${schema_1}.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_SHABITS_UNIDATA_ALVI`
    WHERE CUSTOMER_KEY IN (SELECT CUSTOMER_ID
    FROM `${gcp_proyect}.${schema_2}.VW_I_UNICARD_PREAPROBADOS`
    WHERE PERIODO = ${periodo})
    AND MONTHID <= ${periodo_n2}
    AND SHABIT IS NOT NULL
    ) t
    WHERE rw = 1
    """,

    # Compras de los clientes preaprobados en los formatos Unimarc, Alvi,
    # M10 y S10, mediante los distintos tipos de pago disponibles, en un
    # lapso de tiempo de 6 meses hacia atras desde el mes anterior al
    # periodo establecido
    'compras_formatos':
    """
    SELECT CUSTOMER_KEY,MARKET_BASKET_KEY,DATE_VALUE,
    TNDR_TP_ID,TNDR_TP_DSC,TOT_SALE_AMT,ORG_IP_ID,
    CASE
    WHEN TNDR_TP_ID IN ('42','44','45','46','60','61','62','64','65',
                            '66','67','69','70','79','83','84','85','86',
                            '89','90','91','92','93','94','95','96','97',
                            '35','68','78') THEN 'CREDITO'
    WHEN TNDR_TP_ID IN ('11') THEN 'EFECTIVO'
    WHEN TNDR_TP_ID IN ('63','82') THEN 'DEBITO'
    ELSE 'RESTO'
    END AS MEDIO_PAGO
    FROM (SELECT P.CUSTOMER_KEY,A.MARKET_BASKET_KEY,T.TNDR_TP_ID,
    T.TNDR_TP_DSC,P.TOT_SALE_AMT,D.DATE_VALUE,S.ORG_IP_ID,
    RANK() OVER(PARTITION BY MARKET_BASKET_KEY ORDER BY PYMT_AMT DESC,TNDR_TP_ID ASC) AS RANKING
    FROM `${gcp_proyect_1}.${schema_1}.DW_VW_FACT_PAYMENT` A
    INNER JOIN `${gcp_proyect_1}.${schema_1}.DW_VW_DIM_TENDER_TYPE` T
    ON A.TNDR_TP_KEY = T.TENDER_TYPE_KEY
    INNER JOIN `${gcp_proyect_1}.${schema_1}.DW_VW_FACT_MKT_BSKT` P
    USING(MARKET_BASKET_KEY)
    INNER JOIN `${gcp_proyect_1}.${schema_1}.DW_VW_DIM_DATE` D
    ON D.DATE_KEY = A.DATE_KEY
    INNER JOIN `${gcp_proyect_1}.${schema_1}.DW_VW_DIM_STORE_HIERARCHY` S
    ON A.STORE_KEY = S.STORE_KEY
    WHERE D.DATE_VALUE >= '${fecha_ini}'
    AND D.DATE_VALUE < '${fecha_fin}'
    AND P.FNC_DOC_TP_DSC IN ('TN','TF','BX','B','BE','F','NC','NE','FX','FE')
    AND P.ITM_TXN_FCN_TP_DSC = 'V'
    AND S.ORG_IP_ID IN ('01','02','08','09')
    AND P.CUSTOMER_KEY in (SELECT CUSTOMER_ID
    FROM `${gcp_proyect_2}.${schema_2}.VW_I_UNICARD_PREAPROBADOS`
    WHERE PERIODO = ${periodo})
    )a
    WHERE RANKING = 1
    """,

    # Cantidad de productos que el cliente compro en promocion en cada
    # basket, en un lapso de tiempo de 6 meses atras desde el mes anterior
    # al periodo establecido
    'promociones':
    """
    SELECT CUSTOMER_KEY,MARKET_BASKET_KEY,
    SUM(VALUE) TOT_SALE_AMT,
    SUM(QUANTITY) N_PRODUCTOS,
    COUNT(DISTINCT SKU_PRODUCT) N_PRODUCTOS_DISTINTOS,
    COUNT(CASE WHEN DISCOUNT_VALUE > 0 THEN 1 END) N_PROMOCIONES,
    FROM `${gcp_proyect_1}.${schema_1}.VW_SALES_ITEM`
    WHERE TRANSACTION_DATE >= '${fecha_ini}'
    AND TRANSACTION_DATE < '${fecha_fin}'
    AND CUSTOMER_KEY IN (SELECT CUSTOMER_ID
    FROM `${gcp_proyect_2}.${schema_2}.VW_I_UNICARD_PREAPROBADOS`
    WHERE PERIODO = ${periodo})
    AND ITM_TXN_FCN_TP_DSC = 'V'
    AND TRANSACTION_TYPE IN ('TN','TF','BX','B','BE','F','NC','NE','FX','FE')
    AND QUANTITY > 0
    AND VALUE > 0
    GROUP BY CUSTOMER_KEY,MARKET_BASKET_KEY,TRANSACTION_DATE
    """,

    # Datos demograficos del cliente
    'datos_demograficos':
    """
    SELECT a.customer_key CUSTOMER_KEY,
    a.pda_customer_key CUSTOMER_ID,
    UPPER(LTRIM(SUBSTR(a.customer_nk,11,9),'0')) AS RUTDV,
    b.Cant_hijos,
    b.ISE,
    b.Renta_HH,
    b.ESTADO_CIVIL,
    b.Cant_vehiculos,
    b.Cant_vehiculos_Fam,
    b.Cant_BBRR,
    b.Cant_BBRR_Fam,
    c.BIRTHDATE
    FROM `${gcp_proyect_1}.${schema_1}.VW_CDA_CST_DEID` a
    LEFT JOIN `${gcp_proyect_1}.${schema_2}.DIM_GENERAL` b
    ON UPPER(LTRIM(SUBSTR(a.customer_nk,11,9),'0')) = REGEXP_REPLACE(b.RUTID, r'^0+', '')
    LEFT JOIN `${gcp_proyect_2}.${schema_3}.TMP_FECHA_NACIMIENTO_ADQUISICION` c
    ON a.pda_customer_key = c.CUSTOMER_ID
    WHERE a.customer_key in (SELECT CUSTOMER_ID
    FROM `${gcp_proyect_1}.${schema_4}.VW_I_UNICARD_PREAPROBADOS`
    WHERE PERIODO = ${periodo})
    AND a.customer_nk LIKE 'CST%'
    AND LENGTH(a.customer_nk) = 19
    AND (LENGTH(TRANSLATE(SUBSTRING(a.customer_nk,8,11),'0123456789','')) = 0
    AND LENGTH(TRANSLATE(SUBSTRING(a.customer_nk,19,1),'0123456789K','')) = 0)
    """,

    # Fecha de nacimiento del cliente
    'fecha_nacimiento':
    """
    SELECT CUSTOMER_ID,BIRTHDATE
    FROM ${gcp_proyect}.${schema}.TMP_FECHA_NACIMIENTO_ADQUISICION
    """

})


# -------------------------------------------------------------------------
# Functions
# -------------------------------------------------------------------------
"""
Funcion que realiza un merge entre las tablas compras_formatos
y promociones que se obtienen en las queries, ademas, se calcula la
variable DIST_PROMOS, que representa el porcentaje de productos que compro
en promocion el cliente. medido a partir del total de productos distintos
que compro en la basket

Parametros:
- compras_formatos: tabla obtenida a partir de la query compras_formatos
- promociones: tabla obtenida a partir de la query promociones

Retorno: Dataframe
"""
def compras_promociones(
        compras_formatos: pd.DataFrame,
        promociones: pd.DataFrame
) -> pd.DataFrame:
    compras_formatos_copy = compras_formatos.set_index('MARKET_BASKET_KEY')[['CUSTOMER_KEY',
                                                         'DATE_VALUE','TNDR_TP_ID',
                                                         'ORG_IP_ID','MEDIO_PAGO']]

    promociones_copy = promociones.set_index('MARKET_BASKET_KEY')[['TOT_SALE_AMT',
                                                     'N_PRODUCTOS','N_PRODUCTOS_DISTINTOS',
                                                     'N_PROMOCIONES']]

    compras_promociones_formatos = compras_formatos_copy.join(promociones_copy,how='inner')

    compras_promociones_formatos['PERIODO'] = pd.to_datetime(
        compras_promociones_formatos['DATE_VALUE']).dt.strftime('%Y%m')

    compras_promociones_formatos['DIST_PROMOS'] = round(
        (compras_promociones_formatos['N_PROMOCIONES'] / \
         compras_promociones_formatos['N_PRODUCTOS_DISTINTOS']) * 100,0)

    return compras_promociones_formatos

"""
Función que obtiene el medio o los medios de pago preferidos y utilizados
por el cliente dentro de un rango de tiempo definido. Los medios de pago
considerados son: crédito, efectivo, débito y resto

Parametros:
- compras_promociones_formatos: Dataframe obtenido en la funcion
    compras_promociones
- idmes: periodo (en formato YYYYMM) para realizar el calculo
- idmes_n1: Mes anterior al periodo establecido (en formato YYYYMM)
    para calcular el dataset. Se utiliza la variable periodo_n1
    creada en la linea ""
- tiempo: variable utilizada para dar nombre a las variables del DataFrame
    de retorno. valores permitidos: 1M,3M,6M

Retorno: Dataframe
"""
def medios_pago(
        compras_promociones_formatos: pd.DataFrame,
        idmes: str,
        idmes_n1: str,
        tiempo: str
) -> pd.DataFrame:
    if idmes == idmes_n1:
        aux = compras_promociones_formatos.loc[
            compras_promociones_formatos['PERIODO'] == idmes
        ]
    else:
        aux = compras_promociones_formatos.loc[
            compras_promociones_formatos['PERIODO'] >= idmes
        ]

    conteo = (
        aux.groupby(['CUSTOMER_KEY','MEDIO_PAGO'], sort=False)
          .size()
          .reset_index(name='N_USOS')
    )

    conteo['TOTAL_USOS'] = conteo.groupby('CUSTOMER_KEY')['N_USOS'].transform('max')

    preferidos = conteo[conteo['N_USOS'] == conteo['TOTAL_USOS']]

    medio_preferido = (
        preferidos.groupby('CUSTOMER_KEY', sort=False)['MEDIO_PAGO']
        .agg('-'.join)
        .reset_index(name='MEDIO_PREFERIDO')
    )

    medios_usados = (
        conteo.groupby('CUSTOMER_KEY', sort=False)['MEDIO_PAGO']
        .agg('-'.join)
        .reset_index(name='MEDIOS_USADOS')
    )

    medios_pago = medio_preferido.merge(
        medios_usados,
        on='CUSTOMER_KEY',
        how='inner',
        sort=False
    )

    return medios_pago.rename(columns={'MEDIO_PREFERIDO':'MEDIO_PAGO_PREFERIDO_'+tiempo,
                                            'MEDIOS_USADOS':'MEDIOS_PAGO_USADOS_'+tiempo})


def variables_compras_formatos(
        compras_promociones_formatos: pd.DataFrame,
        idmes: str,
        idmes_n1: str,
        tiempo: str
) -> pd.DataFrame:
    if idmes == idmes_n1:
        aux = compras_promociones_formatos.loc[
            compras_promociones_formatos['PERIODO'] == idmes
        ]
    else:
        aux = compras_promociones_formatos.loc[
            compras_promociones_formatos['PERIODO'] >= idmes
        ]

    variables_compras = (
        aux.groupby('CUSTOMER_KEY', sort=False)
        .agg({
            'TOT_SALE_AMT': ['sum', 'mean'],
            'N_PRODUCTOS': ['sum', 'mean'],
            'N_PRODUCTOS_DISTINTOS': ['sum', 'mean'],
            'DIST_PROMOS': 'mean',
            'CUSTOMER_KEY': 'count'
        })
    )

    variables_compras.columns = [
        'TOT_SALE_AMT_'+tiempo, 'PROM_TOT_SALE_AMT_'+tiempo,
        'N_PRODUCTOS_'+tiempo, 'PROM_N_PRODUCTOS_'+tiempo,
        'N_PRODUCTOS_DISTINTOS_'+tiempo, 'PROM_N_PRODUCTOS_DISTINTOS_'+tiempo,
        'PROM_DIST_PROMOS_'+tiempo, 'N_VISITAS_'+tiempo
    ]

    return variables_compras.reset_index()

def subir_dataset_cloud_storage(df: pd.DataFrame, bucket_name: str, blob_path: str):
    table = pa.Table.from_pandas(df, preserve_index=False)
    with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp:
        pq.write_table(table, tmp.name, compression='snappy')
        temp_path = tmp.name

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    blob.chunk_size = 8 * 1024 * 1024
    blob.upload_from_filename(temp_path, content_type='application/octet-stream', timeout=600)

    os.remove(temp_path)


# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------

def main():
    usuario = 'dataset_preaprobados'  # noqa: F841
    # parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']  # noqa: F841
    logging.info(f'execution_date: {execution_date}')

    # Set gbq client for all subsequent queries
    gbq_client = Client()

    # Variables de tiempo utilizadas en las queries
    fecha = pendulum.parse(execution_date)
    periodo = fecha.add(months=1).strftime('%Y%m')
    periodo_n1 = fecha.strftime('%Y%m')
    periodo_n2 = fecha.subtract(months=1).strftime('%Y%m')
    periodo_n3 = fecha.subtract(months=2).strftime('%Y%m')
    periodo_n6 = fecha.subtract(months=5).strftime('%Y%m')
    fecha_ini = fecha.subtract(months=5).start_of('month').strftime('%Y-%m-%d')
    fecha_fin = fecha.add(months=1).start_of('month').strftime('%Y-%m-%d')

    logging.info(' ')
    logging.info('--------------------')
    logging.info(f'Se inicia el proceso para el periodo: {periodo}')
    logging.info(f'Con el parametro periodo_n1: {periodo_n1}')
    logging.info(f'Con el parametro fecha inicial: {fecha_ini}')
    logging.info(f'Con el parametro fecha final: {fecha_fin}')
    logging.info('--------------------')

    logging.info(' ')
    logging.info('--------------------')
    logging.info('Inicia la ejecucion de las queries')
    logging.info('--------------------')

    logging.info(' ')
    logging.info('Inicia la query de preaprobados')

    preaprobados = readBigQuery(SQL_QUERIES['preaprobados'].substitute(
    gcp_proyect = 'cl-cda-unidata-prod',
    schema = 'DS_PROD_UNI_SSFF',
    periodo = periodo,
    periodo_n1 = periodo_n1
    ),
    user = 'abravom',
    gbq_client = gbq_client
)

    logging.info('Finaliza la query de preaprobados')

    logging.info(' ')
    logging.info('Inicia la query de tarjetas')

    tarjetas = readBigQuery(SQL_QUERIES['tarjetas'].substitute(
    gcp_proyect = 'cl-cda-unidata-prod',
    schema = 'DS_PROD_UNI_SSFF',
    periodo = periodo,
    periodo_n1 = periodo_n1
    ),
    user = 'abravom',
    gbq_client = gbq_client
)

    logging.info('Finaliza la query de tarjetas')

    logging.info(' ')
    logging.info('Inicia la query de n_preaprobado_cupos')

    n_preaprobado_cupos = readBigQuery(SQL_QUERIES['n_preaprobado_cupos'].substitute(
    gcp_proyect = 'cl-cda-unidata-prod',
    schema = 'DS_PROD_UNI_SSFF',
    periodo = periodo,
    periodo_n1 = periodo_n1
    ),
    user = 'abravom',
    gbq_client = gbq_client
)

    logging.info('Finaliza la query de n_preaprobado_cupos')


    logging.info(' ')
    logging.info('Inicia la query de shabit_unimarc')

    shabit_unimarc = readBigQuery(SQL_QUERIES['shabit_unimarc'].substitute(
    gcp_proyect = 'cl-cda-unidata-prod',
    schema_1 = 'DS_PROD_CLIENTES_IC',
    schema_2 = 'DS_PROD_UNI_SSFF',
    periodo = periodo,
    periodo_n1 = periodo_n1,
    periodo_n2 = periodo_n2
    ),
    user = 'abravom',
    gbq_client = gbq_client
)

    logging.info('Finaliza la query de shabit_unimarc')

    logging.info(' ')
    logging.info('Inicia la query de shabit_alvi')

    shabit_alvi = readBigQuery(SQL_QUERIES['shabits_alvi'].substitute(
    gcp_proyect = 'cl-cda-unidata-prod',
    schema_1 = 'DS_PROD_CLIENTES_IC',
    schema_2 = 'DS_PROD_UNI_SSFF',
    periodo = periodo,
    periodo_n1 = periodo_n1,
    periodo_n2 = periodo_n2
    ),
    user = 'abravom',
    gbq_client = gbq_client
)

    logging.info('Finaliza la query de shabit_alvi')


    logging.info(' ')
    logging.info('Inicia la query de compras_formatos')

    compras_formatos = readBigQuery(SQL_QUERIES['compras_formatos'].substitute(
    gcp_proyect_1 = 'cl-cda-prod',
    gcp_proyect_2 = 'cl-cda-unidata-prod',
    schema_1 = 'DS_CDA_VW_SMU',
    schema_2 = 'DS_PROD_UNI_SSFF',
    periodo = periodo,
    periodo_n1 = periodo_n1,
    fecha_ini = fecha_ini,
    fecha_fin = fecha_fin
    ),
    user = 'abravom',
    gbq_client = gbq_client
)

    logging.info('Finaliza la query de compras_formatos')


    logging.info(' ')
    logging.info('Inicia la query de promociones')

    promociones = readBigQuery(SQL_QUERIES['promociones'].substitute(
    gcp_proyect_1 = 'cl-bigdata-analytics-preprod',
    gcp_proyect_2 = 'cl-cda-unidata-prod',
    schema_1 = 'ML_LAB',
    schema_2 = 'DS_PROD_UNI_SSFF',
    periodo = periodo,
    periodo_n1 = periodo_n1,
    fecha_ini = fecha_ini,
    fecha_fin = fecha_fin
    ),
    user = 'abravom',
    gbq_client = gbq_client
)

    logging.info('Finaliza la query de promociones')

    logging.info(' ')
    logging.info('Inicia la query de datos_demograficos')

    datos_demograficos = readBigQuery(SQL_QUERIES['datos_demograficos'].substitute(
    gcp_proyect_1 = 'cl-cda-unidata-prod',
    gcp_proyect_2 = 'cl-bigdata-analytics',
    schema_1 = 'DS_PROD_CLIENTES_IC',
    schema_2 = 'DS_PROD_CLIENTES_EQUIFAX',
    schema_3 = 'ML_LAB',
    schema_4 = 'DS_PROD_UNI_SSFF',
    periodo = periodo
    ),
    user = 'abravom',
    gbq_client = gbq_client
)

    logging.info('Finaliza la query de datos_demograficos')

    logging.info(' ')
    logging.info('--------------------')
    logging.info('Termina la ejecucion de las queries')
    logging.info('--------------------')

    logging.info(' ')
    logging.info('--------------------')
    logging.info('Inicia el proceso de ajuste de las tablas obtenidas en las queries')
    logging.info('--------------------')

    logging.info(' ')
    logging.info('Inicia ajuste tabla datos_demograficos')

    datos_demograficos['AÑO_NACIMIENTO'] = pd.to_datetime(
        datos_demograficos['BIRTHDATE']).dt.strftime('%Y').astype('float64')

    datos_demograficos = datos_demograficos.drop('BIRTHDATE', axis=1)
    datos_demograficos.columns = datos_demograficos.columns.str.upper()

    logging.info('Finaliza ajuste tabla datos_demograficos')

    logging.info(' ')
    logging.info('Inicia ajuste tabla shabit_unimarc')

    conditions = [
    (shabit_unimarc['LAST_MONTHID'] == int(periodo_n3)) &
    (shabit_unimarc['SHABIT_UNIMARC'] == 'EN FUGA'),
    (shabit_unimarc['LAST_MONTHID'] == int(periodo_n3)) &
    (shabit_unimarc['SHABIT_UNIMARC'] == 'VIP Platino'),
    (shabit_unimarc['LAST_MONTHID'] < int(periodo_n3))
    ]

    choices = ['FUGADO','EN FUGA','FUGADO']
    shabit_unimarc['SHABIT_UNIMARC'] = np.select(
        conditions, choices, default=shabit_unimarc['SHABIT_UNIMARC'])

    logging.info('Finaliza ajuste tabla shabit_unimarc')

    logging.info(' ')
    logging.info('Inicia ajuste tabla shabit_alvi')

    shabit_alvi.loc[
    (shabit_alvi['LAST_MONTHID'] < int(periodo_n2)) &
    (shabit_alvi['SHABIT_ALVI'] == 'EN_FUGA'),
    'SHABIT_ALVI'
    ] = 'FUGADO'

    logging.info('Finaliza ajuste tabla shabit_alvi')

    logging.info(' ')
    logging.info('--------------------')
    logging.info('Finaliza el proceso de ajuste de las tablas obtenidas en las queries')
    logging.info('--------------------')


    logging.info(' ')
    logging.info('--------------------')
    logging.info('Inicia el proceso de creacion de tablas mediante las tablas de las queries')
    logging.info('--------------------')

    logging.info(' ')
    logging.info('Inicia creacion tabla compras_promociones_formatos')
    compras_promociones_formatos = compras_promociones(
        compras_formatos=compras_formatos,
        promociones=promociones
    )

    logging.info('Finaliza creacion tabla compras_promociones_formatos')

    # Inicia el proceso para obtener los medios de pago utilizados y
    # preferidos durante 1, 3 y 6 meses atras
    # a partir del periodo establecido
    logging.info(' ')
    logging.info('Inicia creacion tabla medios_pago_1m')

    medios_pago_1m = medios_pago(
        compras_promociones_formatos=compras_promociones_formatos,
        idmes=periodo_n1,
        idmes_n1=periodo_n1,
        tiempo='1M'
    )

    logging.info('Finaliza creacion tabla medios_pago_1m')

    logging.info(' ')
    logging.info('Inicia creacion tabla medios_pago_3m')

    medios_pago_3m = medios_pago(
        compras_promociones_formatos=compras_promociones_formatos,
        idmes=periodo_n3,
        idmes_n1=periodo_n1,
        tiempo='3M'
    )

    logging.info('Finaliza creacion tabla medios_pago_3m')

    logging.info(' ')
    logging.info('Inicia creacion tabla medios_pago_6m')

    medios_pago_6m = medios_pago(
        compras_promociones_formatos=compras_promociones_formatos,
        idmes=periodo_n6,
        idmes_n1=periodo_n1,
        tiempo='6M'
    )

    logging.info('Finaliza creacion tabla medios_pago_6m')


    # Inicia el proceso para obtener variables mediante las compras
    # realizadas por los clientes durante 1, 3 y 6 meses atras a partir
    # del periodo establecido,estas variables abarcan totales,promedios,etc
    logging.info(' ')
    logging.info('Inicia creacion tabla variables_compras_1m')

    variables_compras_1m = variables_compras_formatos(
        compras_promociones_formatos=compras_promociones_formatos,
        idmes=periodo_n1,
        idmes_n1=periodo_n1,
        tiempo='1M'
    )

    logging.info('Finaliza creacion tabla variables_compras_1m')

    logging.info(' ')
    logging.info('Inicia creacion tabla variables_compras_3m')

    variables_compras_3m = variables_compras_formatos(
        compras_promociones_formatos=compras_promociones_formatos,
        idmes=periodo_n3,
        idmes_n1=periodo_n1,
        tiempo='3M'
    )

    logging.info('Finaliza creacion tabla variables_compras_3m')

    logging.info(' ')
    logging.info('Inicia creacion tabla variables_compras_6m')

    variables_compras_6m = variables_compras_formatos(
        compras_promociones_formatos=compras_promociones_formatos,
        idmes=periodo_n6,
        idmes_n1=periodo_n1,
        tiempo='6M'
    )

    logging.info('Finaliza creacion tabla variables_compras_6m')

    logging.info(' ')
    logging.info('--------------------')
    logging.info('Finaliza el proceso de creacion de tablas mediante las tablas de las queries')
    logging.info('--------------------')


    logging.info(' ')
    logging.info('--------------------')
    logging.info('Inicia el proceso de creacion del dataset para realizar la prediccion')
    logging.info('--------------------')

    dataset = preaprobados

    # Datos demograficos de los clientes
    dataset = dataset.merge(datos_demograficos[['CUSTOMER_KEY','CANT_HIJOS',
                                    'ISE','RENTA_HH','ESTADO_CIVIL',
                                    'CANT_VEHICULOS','CANT_VEHICULOS_FAM',
                                    'CANT_BBRR','CANT_BBRR_FAM',
                                    'AÑO_NACIMIENTO','RUTDV']],
                                    how='left',
                                    on='CUSTOMER_KEY',
                                    sort = False
                                    )

    # Numero de tarjetas Unipay que ha solicitado
    dataset = dataset.merge(
        tarjetas[['CUSTOMER_KEY', 'N_TARJETAS']],
        on='CUSTOMER_KEY',
        how='left',
        sort = False)

    dataset['N_TARJETAS'] = dataset['N_TARJETAS'].fillna(0)

    # Numero de meses que lleva siendo preaprobado
    dataset = dataset.merge(
        n_preaprobado_cupos[['CUSTOMER_KEY', 'N_PREAPROBADO','N_CUPOS']],
        on='CUSTOMER_KEY',
        how='left',
        sort = False)

    # Shabit Unimarc
    dataset = dataset.merge(
        shabit_unimarc[['CUSTOMER_KEY', 'SHABIT_UNIMARC']],
        on='CUSTOMER_KEY',
        how='left',
        sort = False)

    dataset['SHABIT_UNIMARC'] = dataset['SHABIT_UNIMARC'].fillna('SIN_SHABIT')

    # Shabit Alvi
    dataset = dataset.merge(
        shabit_alvi[['CUSTOMER_KEY', 'SHABIT_ALVI']],
        on='CUSTOMER_KEY',
        how='left',
        sort = False)

    dataset['SHABIT_ALVI'] = dataset['SHABIT_ALVI'].fillna('SIN_SHABIT')

    # Compras formatos mes anterior, 3 y 6 meses atras
    dataset = dataset.merge(
        variables_compras_1m[['CUSTOMER_KEY','TOT_SALE_AMT_1M',
                              'N_PRODUCTOS_1M','N_PRODUCTOS_DISTINTOS_1M',
                              'N_VISITAS_1M','PROM_TOT_SALE_AMT_1M',
                              'PROM_N_PRODUCTOS_1M','PROM_DIST_PROMOS_1M']],
        on='CUSTOMER_KEY',
        how='left',
        sort = False)

    dataset = dataset.merge(
        variables_compras_3m[['CUSTOMER_KEY','TOT_SALE_AMT_3M',
                              'N_PRODUCTOS_3M','N_PRODUCTOS_DISTINTOS_3M',
                              'N_VISITAS_3M','PROM_TOT_SALE_AMT_3M',
                              'PROM_N_PRODUCTOS_3M','PROM_DIST_PROMOS_3M']],
        on='CUSTOMER_KEY',
        how='left',
        sort = False)

    dataset = dataset.merge(
        variables_compras_6m[['CUSTOMER_KEY','TOT_SALE_AMT_6M',
                              'N_PRODUCTOS_6M','N_PRODUCTOS_DISTINTOS_6M',
                              'N_VISITAS_6M','PROM_TOT_SALE_AMT_6M',
                              'PROM_N_PRODUCTOS_6M','PROM_DIST_PROMOS_6M']],
        on='CUSTOMER_KEY',
        how='left',
        sort = False)

    dataset['TOT_SALE_AMT_1M'] = dataset['TOT_SALE_AMT_1M'].fillna(0.0)
    dataset['N_PRODUCTOS_1M'] = dataset['N_PRODUCTOS_1M'].fillna(0.0)
    dataset['N_PRODUCTOS_DISTINTOS_1M'] = dataset['N_PRODUCTOS_DISTINTOS_1M'].fillna(0.0)
    dataset['N_VISITAS_1M'] = dataset['N_VISITAS_1M'].fillna(0.0)
    dataset['PROM_TOT_SALE_AMT_1M'] = round(dataset['PROM_TOT_SALE_AMT_1M'].fillna(0.0),0)
    dataset['PROM_N_PRODUCTOS_1M'] = round(dataset['PROM_N_PRODUCTOS_1M'].fillna(0.0),0)
    dataset['PROM_DIST_PROMOS_1M'] = round(dataset['PROM_DIST_PROMOS_1M'].fillna(0.0),0)

    dataset['TOT_SALE_AMT_3M'] = dataset['TOT_SALE_AMT_3M'].fillna(0.0)
    dataset['N_PRODUCTOS_3M'] = dataset['N_PRODUCTOS_3M'].fillna(0.0)
    dataset['N_PRODUCTOS_DISTINTOS_3M'] = dataset['N_PRODUCTOS_DISTINTOS_3M'].fillna(0.0)
    dataset['N_VISITAS_3M'] = dataset['N_VISITAS_3M'].fillna(0.0)
    dataset['PROM_TOT_SALE_AMT_3M'] = round(dataset['PROM_TOT_SALE_AMT_3M'].fillna(0.0),0)
    dataset['PROM_N_PRODUCTOS_3M'] = round(dataset['PROM_N_PRODUCTOS_3M'].fillna(0.0),0)
    dataset['PROM_DIST_PROMOS_3M'] = round(dataset['PROM_DIST_PROMOS_3M'].fillna(0.0),0)

    dataset['TOT_SALE_AMT_6M'] = dataset['TOT_SALE_AMT_6M'].fillna(0.0)
    dataset['N_PRODUCTOS_6M'] = dataset['N_PRODUCTOS_6M'].fillna(0.0)
    dataset['N_PRODUCTOS_DISTINTOS_6M'] = dataset['N_PRODUCTOS_DISTINTOS_6M'].fillna(0.0)
    dataset['N_VISITAS_6M'] = dataset['N_VISITAS_6M'].fillna(0.0)
    dataset['PROM_TOT_SALE_AMT_6M'] = round(dataset['PROM_TOT_SALE_AMT_6M'].fillna(0.0),0)
    dataset['PROM_N_PRODUCTOS_6M'] = round(dataset['PROM_N_PRODUCTOS_6M'].fillna(0.0),0)
    dataset['PROM_DIST_PROMOS_6M'] = round(dataset['PROM_DIST_PROMOS_6M'].fillna(0.0),0)

    # Medios de pago usados y preferidos
    # en el mes anterior, 3 y 6 meses atras
    dataset = dataset.merge(medios_pago_1m,on='CUSTOMER_KEY',how='left',sort = False)
    dataset = dataset.merge(medios_pago_3m,on='CUSTOMER_KEY',how='left',sort = False)
    dataset = dataset.merge(medios_pago_6m,on='CUSTOMER_KEY',how='left',sort = False)

    dataset['MEDIO_PAGO_PREFERIDO_1M'] = dataset['MEDIO_PAGO_PREFERIDO_1M'].fillna('NINGUNO')
    dataset['MEDIOS_PAGO_USADOS_1M'] = dataset['MEDIOS_PAGO_USADOS_1M'].fillna('NINGUNO')
    dataset['MEDIO_PAGO_PREFERIDO_3M'] = dataset['MEDIO_PAGO_PREFERIDO_3M'].fillna('NINGUNO')
    dataset['MEDIOS_PAGO_USADOS_3M'] = dataset['MEDIOS_PAGO_USADOS_3M'].fillna('NINGUNO')
    dataset['MEDIO_PAGO_PREFERIDO_6M'] = dataset['MEDIO_PAGO_PREFERIDO_6M'].fillna('NINGUNO')
    dataset['MEDIOS_PAGO_USADOS_6M'] = dataset['MEDIOS_PAGO_USADOS_6M'].fillna('NINGUNO')

    dataset = dataset[['PERIODO'] + [x for x in dataset.columns if x != 'PERIODO']]

    logging.info(' ')
    logging.info('--------------------')
    logging.info('Finaliza el proceso de creacion del dataset para realizar la prediccion')
    logging.info('--------------------')

    logging.info(' ')
    logging.info('Inicia el proceso para subir el dataset a cloud storage')

    subir_dataset_cloud_storage(
    df=dataset,
    bucket_name='cl-bigdata-analytics-preprod-us-sandbox-datasets',
    blob_path='UNIPAY/ADQUISICION_UNIPAY/DATASETS/DATASET_ADQUISICION_'+str(periodo)+'.parquet'
    )

    logging.info('Finaliza el proceso para subir el dataset a cloud storage')

    logging.info(' ')
    logging.info('Flujo ejecutado de forma exitosa')

if __name__ == '__main__':
    main()
