# Default
from __future__ import annotations

import os
import logging
import argparse
from logging import config

# Pip
import numpy as np
import pandas as pd
import pendulum  # noqa: F401
from google.cloud import bigquery  # noqa: F401
from google.cloud.bigquery import Client
from pandas.tseries.offsets import MonthEnd

# Own
import common.gcp_extended.bigquery as gbq_extended  # noqa: F401
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict  # noqa: F401
from common.gcp_extended.bigquery import (
    uploadFrame,
    readBigQuery,
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
parser.add_argument(
    '--periodo', type=int,
    help='period to calculate'
)
parser.add_argument(
    '--periodo_n1', type=int,
    help='period to get last status'
)
parser.add_argument(
    '--fecha_ini', type=str,
    help='initial date'
)
parser.add_argument(
    '--fecha_fin', type=str,
    help='final date'
)


# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    # Transacciones realizadas en el periodo establecido
    # por los clientes con tarjeta unipay
    'compras':
    """
    SELECT MARKET_BASKET_KEY,
    CUSTOMER_KEY,
    TNDR_TP_ID,
    TOT_SALE_AMT,
    DATE_VALUE
    FROM `${gcp_proyect}.${schema}.TMP_DATA_LIFECYCLE_UNIPAY_SALES_ALL`
    WHERE DATE_VALUE >= '${fecha_ini}'
    AND DATE_VALUE < '${fecha_fin}'
    """,

    # Transacciones mediante la tarjeta unipay
    # realizadas por los clientes
    'compras_unipay':
    """
    SELECT MARKET_BASKET_KEY,
    CUSTOMER_KEY,
    TNDR_TP_ID,
    TOT_SALE_AMT,
    DATE_VALUE
    FROM `${gcp_proyect}.${schema}.TMP_DATA_LIFECYCLE_UNIPAY_SALES_UNIPAY`
    WHERE DATE_VALUE < '${fecha_fin}'
    """,

    # Clientes con tarjeta unipay en el periodo establecido
    'tarjetas_unipay':
    """
    SELECT CUSTOMER_KEY,
    CARD_ID,
    SUBSCRIPTION_DATE,
    ACTIVATION_DATE,
    TERMINATION_DATE,
    PERIODO,
    CREDIT_LIMIT
    FROM ${gcp_proyect}.${schema}.TMP_DATA_LIFECYCLE_UNIPAY_CARDS
    """,

    # Estado ciclo de vida unipay en el periodo
    # anterior al establecido
    'estados':
    """
    SELECT CUSTOMER_KEY,
    CARD_ID,
    STATUS,
    MONTHID
    FROM ${gcp_proyect}.${schema}.LIFECYCLE_UNIPAY_STATUS
    WHERE MONTHID = CAST(${periodo_n1} AS STRING)
    AND STATUS != 'closed'
    """
})

# -------------------------------------------------------------------------
# Functions
# -------------------------------------------------------------------------

# Funcion para obtener el nuevo estado, esta funcion es para los clientes
# en el cual su estado anterior equivale a grow o reward
# Parametros:
# status_n1: estado periodo anterior
# sow: share of wallet del cliente
# sow_prom: share of wallet definido por el cupo de la tarjeta
# ciclo: ciclo de compra del cliente
# ciclo_75: ciclo definido por el cupo de la tarjeta

def cambio_estado(  # noqa: RET503
        status_n1: str,
        sow: float,
        sow_prom: float,
        ciclo: float,
        ciclo_75: float
        ) -> str:  # noqa: D103, E501
    if (status_n1 == 'grow'
        and sow >= sow_prom
        and ciclo <= ciclo_75):
        return 'reward'
    if (status_n1 == 'grow'
        and sow < sow_prom
        and ciclo > ciclo_75):
        return 'retain'
    if (status_n1 == 'grow'
        and sow < sow_prom
        and ciclo <= ciclo_75) \
        or (status_n1 == 'grow'
        and sow >= sow_prom
        and ciclo > ciclo_75):
        return 'grow'
    if (status_n1 == 'reward'
        and sow >= sow_prom
        and ciclo <= ciclo_75):
        return 'reward'
    if (status_n1 == 'reward'
        and ciclo > ciclo_75
        and sow >= sow_prom) \
        or (status_n1 == 'reward'
        and sow < sow_prom
        and sow == 0.0
        and ciclo > ciclo_75):
        return 'retain'
    if (status_n1 == 'reward'
        and sow < sow_prom
        and ciclo <= ciclo_75) \
        or (status_n1 == 'reward'
        and sow < sow_prom
        and sow > 0.0
        and ciclo > ciclo_75):
        return 'grow'

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------

def main():
    usuario = 'lifecycle_unipay'  # noqa: F841
    # parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']  # noqa: F841
    periodo: int = args['periodo']
    periodo_n1: int = args['periodo_n1']
    fecha_ini: str = args['fecha_ini']
    fecha_fin: str = args['fecha_fin']
    logging.info(f'execution_date: {execution_date}')

    # Set gbq client for all subsequent queries
    gbq_client = Client()

    # Periodo inicial ciclo vida unipay
    periodo_inicio = 202301

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

    # Ejecucion Queries
    if periodo > periodo_inicio:
        estados_n1 = readBigQuery(SQL_QUERIES['estados'].substitute(
        gcp_proyect = 'cl-bigdata-analytics-preprod',
        schema = 'TMP',
        periodo_n1 = periodo_n1
        ),
        user = usuario,
        gbq_client = gbq_client
        )

    tarjetas = readBigQuery(SQL_QUERIES['tarjetas_unipay'].substitute(
    gcp_proyect = 'cl-bigdata-analytics-preprod',
    schema = 'TMP',
    fecha_fin = fecha_fin,
    periodo = periodo
    ),
    user = usuario,
    gbq_client = gbq_client
    )

    compras = readBigQuery(SQL_QUERIES['compras'].substitute(
    gcp_proyect = 'cl-bigdata-analytics-preprod',
    schema = 'TMP',
    fecha_ini = fecha_ini,
    fecha_fin = fecha_fin
    ),
    user = usuario,
    gbq_client = gbq_client
    )

    compras_unipay = readBigQuery(SQL_QUERIES['compras_unipay'].substitute(
    gcp_proyect = 'cl-bigdata-analytics-preprod',
    schema = 'TMP',
    fecha_fin = fecha_fin
    ),
    user = usuario,
    gbq_client = gbq_client
    )

    logging.info(' ')
    logging.info('--------------------')
    logging.info('Termina la ejecucion de las queries')
    logging.info('--------------------')


    logging.info(' ')
    logging.info('--------------------')
    logging.info('Inicia el proceso del ajuste de la activacion y cierre de las tarjetas')
    logging.info('--------------------')

    compras['TOT_SALE_AMT'] = compras['TOT_SALE_AMT'].astype('int64')
    compras_unipay['TOT_SALE_AMT'] = compras_unipay['TOT_SALE_AMT'].astype('int64')

    compras['DATE_VALUE'] = pd.to_datetime(compras['DATE_VALUE'])
    compras_unipay['DATE_VALUE'] = pd.to_datetime(compras_unipay['DATE_VALUE'])

    tarjetas_copy = tarjetas.copy()
    tarjetas_copy['SUBSCRIPTION_DATE'] = pd.to_datetime(tarjetas_copy['SUBSCRIPTION_DATE'])
    tarjetas_copy['ACTIVATION_DATE'] = pd.to_datetime(tarjetas_copy['ACTIVATION_DATE'])
    tarjetas_copy['TERMINATION_DATE'] = pd.to_datetime(tarjetas_copy['TERMINATION_DATE'])
    tarjetas_copy['PERIODO2'] = pd.to_datetime(
        tarjetas_copy['PERIODO'].dropna().astype(int).astype(str) + '01',
        format='%Y%m%d',
        errors='coerce'
    ) + pd.DateOffset(months=1)

    # Verificar la fecha de cierre de las tarjetas
    conditions = [
        (~tarjetas_copy['TERMINATION_DATE'].isna()) &
        (~tarjetas_copy['PERIODO2'].isna()) &
        (tarjetas_copy['TERMINATION_DATE'].dt.strftime('%Y%m') > \
         tarjetas_copy['PERIODO2'].dt.strftime('%Y%m')), #PERIODO2

        (~tarjetas_copy['TERMINATION_DATE'].isna()) &
        (~tarjetas_copy['PERIODO2'].isna()) &
        (tarjetas_copy['TERMINATION_DATE'].dt.strftime('%Y%m') < \
         tarjetas_copy['PERIODO2'].dt.strftime('%Y%m')), #TERMINATION_DATE

        (tarjetas_copy['TERMINATION_DATE'].isna()) &
        (~tarjetas_copy['PERIODO2'].isna()), #PERIODO2

        (tarjetas_copy['TERMINATION_DATE'].isna()) &
        (tarjetas_copy['PERIODO2'].isna()), #SUBSCRIPTION_DATE

        (~tarjetas_copy['TERMINATION_DATE'].isna()) &
        (tarjetas_copy['PERIODO2'].isna()) &
        (tarjetas_copy['TERMINATION_DATE'].dt.strftime('%Y%m') > \
         tarjetas_copy['SUBSCRIPTION_DATE'].dt.strftime('%Y%m')), #SUBSCRIPTION_DATE

        (~tarjetas_copy['TERMINATION_DATE'].isna()) &
        (tarjetas_copy['PERIODO2'].isna()) &
        (tarjetas_copy['TERMINATION_DATE'].dt.strftime('%Y%m') == \
         tarjetas_copy['SUBSCRIPTION_DATE'].dt.strftime('%Y%m')) #TERMINATION_DATE
    ]

    choices = [
        tarjetas_copy['PERIODO2'],
        tarjetas_copy['TERMINATION_DATE'],
        tarjetas_copy['PERIODO2'],
        tarjetas_copy['SUBSCRIPTION_DATE'],
        tarjetas_copy['SUBSCRIPTION_DATE'],
        tarjetas_copy['TERMINATION_DATE']
    ]

    tarjetas_copy['CLOSED_DATE'] = np.select(
        conditions,
        choices,
        default=tarjetas_copy['TERMINATION_DATE']
    )

    tarjetas_copy['AUX'] = pd.to_datetime(tarjetas_copy['CLOSED_DATE'].fillna('2100-01-01'))

    tarjetas_copy = tarjetas_copy[[
        'CUSTOMER_KEY',
        'CARD_ID',
        'SUBSCRIPTION_DATE',
        'ACTIVATION_DATE',
        'CLOSED_DATE',
        'CREDIT_LIMIT',
        'AUX']
    ]

    # Asignar la compra a la tarjeta que esta activa en ese periodo
    # al dataframe compras
    df_merged = pd.merge(  # noqa: PD015
        compras,
        tarjetas_copy,
        on='CUSTOMER_KEY',
        how='inner'
    )

    df_activas = df_merged[
        (df_merged['DATE_VALUE'] >= df_merged['SUBSCRIPTION_DATE']) &
        (df_merged['DATE_VALUE'] < df_merged['AUX'])
    ]

    compras_ajust = df_activas.sort_values(
        by=['MARKET_BASKET_KEY',
            'CUSTOMER_KEY',
            'SUBSCRIPTION_DATE',
            'ACTIVATION_DATE',
            'AUX',
            'DATE_VALUE'],
        ascending=False).drop_duplicates(subset=['MARKET_BASKET_KEY'], keep='first')

    compras_ajust = compras_ajust[[
        'CUSTOMER_KEY',
        'CARD_ID',
        'SUBSCRIPTION_DATE',
        'ACTIVATION_DATE',
        'CLOSED_DATE',
        'CREDIT_LIMIT',
        'MARKET_BASKET_KEY',
        'DATE_VALUE',
        'TNDR_TP_ID',
        'TOT_SALE_AMT']]

    # Asignar la compra a la tarjeta que esta activa en ese periodo
    # al dataframe compras_unipay
    df_merged = pd.merge(  # noqa: PD015
        compras_unipay,
        tarjetas_copy,
        on='CUSTOMER_KEY',
        how='inner'
    )

    df_activas = df_merged[
        (df_merged['DATE_VALUE'] >= df_merged['SUBSCRIPTION_DATE']) &
        (df_merged['DATE_VALUE'] < df_merged['AUX'])
    ]

    compras_unipay_ajust = df_activas.sort_values(
        by=['MARKET_BASKET_KEY',
            'CUSTOMER_KEY',
            'SUBSCRIPTION_DATE',
            'ACTIVATION_DATE',
            'AUX',
            'DATE_VALUE'],
        ascending=False).drop_duplicates(subset=['MARKET_BASKET_KEY'], keep='first')

    compras_unipay_ajust = compras_unipay_ajust[[
        'CUSTOMER_KEY',
        'CARD_ID',
        'SUBSCRIPTION_DATE',
        'ACTIVATION_DATE',
        'CLOSED_DATE',
        'CREDIT_LIMIT',
        'MARKET_BASKET_KEY',
        'DATE_VALUE',
        'TNDR_TP_ID',
        'TOT_SALE_AMT']]

    compras_unipay_ajust = compras_unipay_ajust.reset_index(drop=True)

    # Actualizacion de ACTIVATION_DATE para las tarjetas que se activaron
    # pero en UNICARD_CARD no se ve reflejado

    tarjetas_ajust = pd.concat([
        tarjetas_copy.query('SUBSCRIPTION_DATE < @fecha_ini \
        & (CLOSED_DATE  >= @fecha_ini | CLOSED_DATE.isna())'),
        tarjetas_copy.query('SUBSCRIPTION_DATE >= @fecha_ini \
        & SUBSCRIPTION_DATE < @fecha_fin')],
        ignore_index=True
    )

    idx = compras_unipay_ajust.groupby([
        'CUSTOMER_KEY',
        'CARD_ID'])['DATE_VALUE'].idxmin()

    activacion_tarjeta = compras_unipay_ajust.loc[idx]

    tarjetas_revision = tarjetas_ajust.query('ACTIVATION_DATE.isna()')

    tarjetas_revision = tarjetas_revision.merge(
    activacion_tarjeta[['CUSTOMER_KEY',
                        'CARD_ID',
                        'DATE_VALUE']],
    on=['CUSTOMER_KEY','CARD_ID'],
    how='inner'
    )

    tarjetas_revision['DIAS'] = tarjetas_revision['DATE_VALUE'] - \
        tarjetas_revision['SUBSCRIPTION_DATE']

    tarjetas_revision = tarjetas_revision[
        tarjetas_revision['DIAS'] <= pd.Timedelta(days=365)]

    fechas_dict = tarjetas_revision.set_index('CARD_ID')['DATE_VALUE'].to_dict()

    tarjetas_ajust.loc[
        tarjetas_ajust['ACTIVATION_DATE'].isna(),
        'ACTIVATION_DATE'] = tarjetas_ajust.loc[
        tarjetas_ajust['ACTIVATION_DATE'].isna(),
        'CARD_ID'].map(fechas_dict)

    tarjetas_ajust = tarjetas_ajust.drop(columns=['AUX'])

    # Actualizacion de ACTIVATION_DATE
    # para las tarjetas que salen activadas,
    # pero no tienen un registro de compra de unipay
    # solo para las tarjetas con SUBSCRIPCION_DATE >= 2023-01-01
    # debido a la fecha minima en la tabla FACT_PAYMENT

    compras_unipay_ajust_copy = compras_unipay_ajust.copy()

    primera_compra_unipay = compras_unipay_ajust_copy.sort_values(
        by=['CARD_ID','DATE_VALUE'],
        ascending=False).groupby(['CUSTOMER_KEY','CARD_ID']).tail(1).copy()

    primera_compra_unipay = primera_compra_unipay[[
        'DATE_VALUE',
        'CUSTOMER_KEY',
        'CARD_ID']]

    tarjetas_ajust = pd.merge(  # noqa: PD015
        tarjetas_ajust,
        primera_compra_unipay,
        on=['CUSTOMER_KEY','CARD_ID'],
        how='left'
    )

    conditions = [
        (tarjetas_ajust['ACTIVATION_DATE'] < fecha_ini),

        (tarjetas_ajust['ACTIVATION_DATE'] >= fecha_fin),

        (tarjetas_ajust['ACTIVATION_DATE'] >= fecha_ini) &
        (tarjetas_ajust['ACTIVATION_DATE'] < fecha_fin) &
        (~tarjetas_ajust['ACTIVATION_DATE'].isna()) &
        (tarjetas_ajust['DATE_VALUE'].isna()),

        (tarjetas_ajust['ACTIVATION_DATE'] >= fecha_ini) &
        (tarjetas_ajust['ACTIVATION_DATE'] < fecha_fin) &
        (tarjetas_ajust['ACTIVATION_DATE'] != tarjetas_ajust['DATE_VALUE'])
    ]

    choices = [
        tarjetas_ajust['ACTIVATION_DATE'],
        tarjetas_ajust['ACTIVATION_DATE'],
        tarjetas_ajust['DATE_VALUE'],
        tarjetas_ajust['DATE_VALUE']
    ]

    tarjetas_ajust['ACTIVATION_DATE'] = np.select(
        conditions,
        choices,
        default=tarjetas_ajust['ACTIVATION_DATE']
    )

    tarjetas_ajust = tarjetas_ajust.drop(columns=['DATE_VALUE'])

    logging.info(' ')
    logging.info('--------------------')
    logging.info('Termina el proceso del ajuste de la activacion y cierre de las tarjetas')
    logging.info('--------------------')

    logging.info(' ')
    logging.info('--------------------')
    logging.info('Inicia el proceso del calculo de estado')
    logging.info('--------------------')

    if periodo == periodo_inicio:
        tarjetas_ajust['ACTIVATION_DATE'] = pd.to_datetime(
            np.where(tarjetas_ajust['ACTIVATION_DATE'].dt.strftime('%Y%m') > \
                    str(periodo),pd.NaT,tarjetas_ajust['ACTIVATION_DATE'])
            )
        tarjetas_ajust['CLOSED_DATE'] = pd.to_datetime(
            np.where(tarjetas_ajust['CLOSED_DATE'].dt.strftime('%Y%m') > \
                    str(periodo),pd.NaT,tarjetas_ajust['CLOSED_DATE'])
            )

        tarjetas_ajust['DIAS'] = np.where(
            pd.isna(tarjetas_ajust['ACTIVATION_DATE']),
            (pd.Timestamp(fecha_ini) + MonthEnd(0) - tarjetas_ajust['SUBSCRIPTION_DATE']).dt.days,
            0
        )

        conditions = [
            (tarjetas_ajust['CLOSED_DATE'].dt.strftime('%Y%m') == str(periodo)), #closed

            (tarjetas_ajust['SUBSCRIPTION_DATE'].dt.strftime('%Y%m') == str(periodo)) &
            (tarjetas_ajust['ACTIVATION_DATE'].dt.strftime('%Y%m') == str(periodo)), #grow

            (tarjetas_ajust['SUBSCRIPTION_DATE'].dt.strftime('%Y%m') == str(periodo)) &
            (pd.isna(tarjetas_ajust['ACTIVATION_DATE'])), #onboard

            (tarjetas_ajust['SUBSCRIPTION_DATE'].dt.strftime('%Y%m') < str(periodo)) &
            (tarjetas_ajust['ACTIVATION_DATE'].dt.strftime('%Y%m') <= str(periodo)), #grow

            (tarjetas_ajust['SUBSCRIPTION_DATE'].dt.strftime('%Y%m') < str(periodo)) &
            (pd.isna(tarjetas_ajust['ACTIVATION_DATE'])) &
            (tarjetas_ajust['DIAS'] <= 28), #onboard

            (tarjetas_ajust['SUBSCRIPTION_DATE'].dt.strftime('%Y%m') < str(periodo)) &
            (pd.isna(tarjetas_ajust['ACTIVATION_DATE'])) &
            (tarjetas_ajust['DIAS'] > 28) #never_activated
        ]

        choices = [
            'closed',
            'grow',
            'onboard',
            'grow',
            'onboard',
            'never_activated'
        ]

        tarjetas_ajust['STATUS'] = np.select(
            conditions,
            choices,
            default='sin_estado')

        tarjetas_ajust['MONTHID'] = str(periodo)

        estado_clientes = tarjetas_ajust[[
            'CUSTOMER_KEY',
            'CARD_ID',
            'STATUS',
            'MONTHID']]

        createTableFromJSON(
            table_ddl_json_path = os.path.join('gbq_objects', 'lifecycle_unipay_tmp.json'),
            project = proyecto,
            gbq_client = gbq_client,
            if_exists = 'ignore'
        )

    elif periodo > periodo_inicio:
        # Tarjetas UNIPAY
        tarjetas_ajust_copy = tarjetas_ajust.copy()

        tarjetas_ajust_copy['ACTIVATION_DATE'] = pd.to_datetime(
            np.where(tarjetas_ajust_copy['ACTIVATION_DATE'].dt.strftime('%Y%m') > \
                     str(periodo),pd.NaT,tarjetas_ajust_copy['ACTIVATION_DATE']))

        tarjetas_ajust_copy['CLOSED_DATE'] = pd.to_datetime(
            np.where(tarjetas_ajust_copy['CLOSED_DATE'].dt.strftime('%Y%m') > \
                     str(periodo),pd.NaT,tarjetas_ajust_copy['CLOSED_DATE']))

        tarjetas_ajust_copy['CLOSED_DATE_YYYYMM'] = \
            tarjetas_ajust_copy['CLOSED_DATE'].dt.strftime('%Y%m')

        tarjetas_ajust_copy['DIAS'] = np.where(
        pd.isna(tarjetas_ajust_copy['ACTIVATION_DATE']),
        (pd.Timestamp(fecha_ini) + MonthEnd(0) - tarjetas_ajust_copy['SUBSCRIPTION_DATE']).dt.days,
         0
        )

        # Compras realizadas por los clientes en el mes anterior al actual
        # Se agregan los clientes que no han realizado
        # transacciones el mes anterior
        compras_ajust_copy = compras_ajust.copy()

        compras_ajust_copy['DATE_VALUE'] = pd.to_datetime(compras_ajust_copy['DATE_VALUE'])

        compras_ajust_copy['TOT_SALE_AMT_UNIPAY'] = 0

        compras_ajust_copy['TOT_SALE_AMT_UNIPAY'] = compras_ajust_copy['TOT_SALE_AMT'].where(
            compras_ajust_copy['TNDR_TP_ID'].isin(['35','68','78']), 0)

        no_compraron = tarjetas_ajust_copy[~tarjetas_ajust_copy['CARD_ID'].isin(
            compras_ajust_copy['CARD_ID'])]

        compras_ajust_copy = pd.concat([compras_ajust_copy, no_compraron], ignore_index=True)

        compras_ajust_copy['TOT_SALE_AMT'] = compras_ajust_copy['TOT_SALE_AMT'].fillna(0.0)

        compras_ajust_copy['TOT_SALE_AMT_UNIPAY'] = \
            compras_ajust_copy['TOT_SALE_AMT_UNIPAY'].fillna(0.0)

        compras_ajust_copy = compras_ajust_copy[[
            'CUSTOMER_KEY',
            'CARD_ID',
            'DATE_VALUE',
            'TNDR_TP_ID',
            'TOT_SALE_AMT',
            'TOT_SALE_AMT_UNIPAY']]

        # Calculo del SOW de los clientes
        sow = compras_ajust_copy.groupby(['CUSTOMER_KEY','CARD_ID']).agg(
            TOT_SALE_AMT = ('TOT_SALE_AMT','sum'),
            TOT_SALE_AMT_UNIPAY = ('TOT_SALE_AMT_UNIPAY','sum')
        )

        sow = sow.reset_index()

        sow['SOW'] = round((sow['TOT_SALE_AMT_UNIPAY'] / sow['TOT_SALE_AMT']) * 100,0)
        # Si el cliente no compro con UNIPAY en el mes anterior al actual
        # el SOW es igual a 0.0
        sow['SOW'] = sow['SOW'].fillna(0.0)

        # Calculo del CICLO de los clientes
        compras_unipay_ajust_copy = compras_unipay_ajust.copy()

        compras_unipay_ajust_copy = compras_unipay_ajust_copy[
            compras_unipay_ajust_copy['CARD_ID'].notna()]
        compras_unipay_ajust_copy['DATE_VALUE'] = pd.to_datetime(
            compras_unipay_ajust_copy['DATE_VALUE'])

        ciclo = compras_unipay_ajust_copy.sort_values(
            by=['CARD_ID','DATE_VALUE'],
            ascending=True).groupby(['CUSTOMER_KEY','CARD_ID']).tail(1).copy()

        ciclo = ciclo[['DATE_VALUE','CUSTOMER_KEY','CARD_ID']]

        ciclo['PENULTIMA_COMPRA'] = compras_unipay_ajust_copy.sort_values(
            by=['CARD_ID','DATE_VALUE'],
            ascending=True).groupby(['CUSTOMER_KEY','CARD_ID'])['DATE_VALUE'].shift(1)

        ciclo = ciclo.rename(columns={'DATE_VALUE':'ULTIMA_COMPRA'})

        ciclo = ciclo.reset_index(drop=True)

        # Esto se utilizo para el periodo = 202302,
        # debido a las tarjetas de años anteriores para no tener problemas,
        # dado que se le asigno el estado grow en el periodo 202301
        if periodo == 202302:
            ciclo = pd.merge(  # noqa: PD015
                ciclo,
                estados_n1,
                on=['CUSTOMER_KEY','CARD_ID'],
                how='left')
            mask = (
                ciclo['ULTIMA_COMPRA'].dt.strftime('%Y%m') == str(periodo)
            ) & (
                ciclo['STATUS'] == 'grow'
            ) & (
                ciclo['PENULTIMA_COMPRA'].isna()
            )
            ciclo.loc[mask, 'PENULTIMA_COMPRA'] = pd.Timestamp('2023-01-15')
            ciclo = ciclo[[
                'CUSTOMER_KEY',
                'CARD_ID',
                'ULTIMA_COMPRA',
                'PENULTIMA_COMPRA']]

        # Si el cliente no compro con UNIPAY el mes anterior al mes actual
        # el CICLO queda como 500.0
        ciclo['CICLO'] = np.where(
            (pd.to_datetime(ciclo['ULTIMA_COMPRA']).dt.strftime('%Y%m') == str(periodo)),
            (ciclo['ULTIMA_COMPRA'] - ciclo['PENULTIMA_COMPRA']).dt.days,500.0
        )

        # Se realiza un merge entre sow, ciclo, TARJETAS_COPY y ESTADOS_N1
        # para obtener el sow, ciclo, CREDIT LIMIT
        # y el ESTADO (mes anterior) del cliente
        riesgo = pd.merge(  # noqa: PD015
            sow,
            ciclo,
            on=['CUSTOMER_KEY','CARD_ID'],
            how='left')

        riesgo['CICLO'] = riesgo['CICLO'].fillna(500.0)

        riesgo = pd.merge(  # noqa: PD015
            riesgo,
            tarjetas_ajust_copy,
            on=['CUSTOMER_KEY','CARD_ID'],
            how='left')

        riesgo = pd.merge(  # noqa: PD015
            riesgo,
            estados_n1,
            on=['CUSTOMER_KEY','CARD_ID'],
            how='left')

        riesgo = riesgo[[
            'CUSTOMER_KEY',
            'CARD_ID',
            'SUBSCRIPTION_DATE',
            'ACTIVATION_DATE',
            'STATUS',
            'SOW',
            'CICLO',
            'CREDIT_LIMIT',
            'DIAS']]

        riesgo['STATUS'] = riesgo['STATUS'].fillna('sin_estado')

        # Tarjetas que se cerraron en el periodo calculado
        tarjetas_cerradas = tarjetas_ajust_copy[
            tarjetas_ajust_copy['CLOSED_DATE_YYYYMM'] == str(periodo)]

        # Obtenemos las tarjetas que no se cerraron en el mes anterior
        tarjetas_clientes = riesgo[~riesgo['CARD_ID'].isin(tarjetas_cerradas['CARD_ID'])]

        # Se asigna el valor del sow_PROM y ciclo_75
        # dado el CREDIT_LIMIT de la tarjeta
        conditions = [
            (tarjetas_clientes['CREDIT_LIMIT'] >= 30000.0) &
            (tarjetas_clientes['CREDIT_LIMIT'] < 50000.0),

            (tarjetas_clientes['CREDIT_LIMIT'] >= 50000.0) &
            (tarjetas_clientes['CREDIT_LIMIT'] < 100000.0),

            (tarjetas_clientes['CREDIT_LIMIT'] >= 100000.0) &
            (tarjetas_clientes['CREDIT_LIMIT'] < 150000.0),

            (tarjetas_clientes['CREDIT_LIMIT'] >= 150000.0) &
            (tarjetas_clientes['CREDIT_LIMIT'] < 200000.0),

            (tarjetas_clientes['CREDIT_LIMIT'] >= 200000.0) &
            (tarjetas_clientes['CREDIT_LIMIT'] < 300000.0),

            (tarjetas_clientes['CREDIT_LIMIT'] >= 300000.0) &
            (tarjetas_clientes['CREDIT_LIMIT'] < 600000.0),

            (tarjetas_clientes['CREDIT_LIMIT'] >= 600000.0)
        ]

        sow_choices = [
            10.0,
            9.0,
            21.0,
            26.0,
            29.0,
            36.0,
            47.0
        ]

        ciclo_choices = [
            60.0,
            55.0,
            41.0,
            35.0,
            30.0,
            24.0,
            12.0
        ]

        tarjetas_clientes['SOW_PROM'] = np.select(
            conditions,
            sow_choices,
            default=0.0
        )

        tarjetas_clientes['CICLO_75'] = np.select(
            conditions,
            ciclo_choices,
            default=0.0
        )

        # Tarjetas que no se cerraron
        v_cambio_estado = np.vectorize(cambio_estado)

        conditions_lc = [
            (tarjetas_clientes['STATUS'].isin(['grow','reward'])), #funcion

            ((tarjetas_clientes['STATUS'] == 'retain') &
             (tarjetas_clientes['SOW'] > 0.0)).astype(bool), #grow

            ((tarjetas_clientes['STATUS'] == 'retain') &
             (tarjetas_clientes['SOW'] == 0.0)).astype(bool), #lapsed

            ((tarjetas_clientes['STATUS'] == 'lapsed') &
             (tarjetas_clientes['SOW'] > 0.0)).astype(bool), #grow

            ((tarjetas_clientes['STATUS'] == 'lapsed') &
             (tarjetas_clientes['SOW'] == 0.0)).astype(bool), #lapsed

            ((tarjetas_clientes['STATUS'] == 'never_activated') &
             (tarjetas_clientes['SOW'] > 0.0)).astype(bool), #grow

            ((tarjetas_clientes['STATUS'] == 'never_activated') &
             (tarjetas_clientes['SOW'] == 0.0)).astype(bool), #never_activated

            ((tarjetas_clientes['STATUS'] == 'onboard') &
             (pd.isna(tarjetas_clientes['ACTIVATION_DATE'])) &
             (tarjetas_clientes['DIAS'] <= 28)).astype(bool), #onboard

            ((tarjetas_clientes['STATUS'] == 'onboard') &
             (pd.isna(tarjetas_clientes['ACTIVATION_DATE'])) &
             (tarjetas_clientes['DIAS'] > 28)).astype(bool), #never_activated

            ((tarjetas_clientes['STATUS'] == 'onboard') &
             (~pd.isna(tarjetas_clientes['ACTIVATION_DATE']))).astype(bool), #grow

            ((tarjetas_clientes['STATUS'] == 'sin_estado') &
             (tarjetas_clientes['SOW'] == 0.0)).astype(bool), #onboard

            ((tarjetas_clientes['STATUS'] == 'sin_estado') &
             (tarjetas_clientes['SOW'] > 0.0)).astype(bool) #grow
        ]

        estado_choices = [
            v_cambio_estado(
                tarjetas_clientes['STATUS'],
                tarjetas_clientes['SOW'],
                tarjetas_clientes['SOW_PROM'],
                tarjetas_clientes['CICLO'],
                tarjetas_clientes['CICLO_75']
                ),
                np.full(len(tarjetas_clientes), 'grow'),
                np.full(len(tarjetas_clientes), 'lapsed'),
                np.full(len(tarjetas_clientes), 'grow'),
                np.full(len(tarjetas_clientes), 'lapsed'),
                np.full(len(tarjetas_clientes), 'grow'),
                np.full(len(tarjetas_clientes), 'never_activated'),
                np.full(len(tarjetas_clientes), 'onboard'),
                np.full(len(tarjetas_clientes), 'never_activated'),
                np.full(len(tarjetas_clientes), 'grow'),
                np.full(len(tarjetas_clientes), 'onboard'),
                np.full(len(tarjetas_clientes), 'grow')
        ]

        tarjetas_clientes['STATUS_NEW'] = np.select(
            conditions_lc,
            estado_choices,
            default='sin_estado'
        )

        # Tarjetas cerradas
        tarjetas_cerradas['STATUS_NEW'] = 'closed'

        # Creamos un Dataframe vacio, para guardar
        # el cliente, tarjeta, estado y periodo
        estado_clientes = pd.DataFrame(
            columns = [
                'CUSTOMER_KEY',
                'CARD_ID',
                'STATUS',
                'MONTHID'
            ]
        )

        estado_clientes = pd.concat(
            [tarjetas_clientes,tarjetas_cerradas], ignore_index=True
        )

        estado_clientes = estado_clientes[[
            'CUSTOMER_KEY',
            'CARD_ID',
            'STATUS']]

        estado_clientes['MONTHID'] = periodo

    logging.info(' ')
    logging.info('--------------------')
    logging.info('Termina el proceso del calculo de estado')
    logging.info('--------------------')


    uploadFrame(
    estado_clientes[['CUSTOMER_KEY','CARD_ID','STATUS','MONTHID']],
    table_ddl_json_path=os.path.join('gbq_objects','lifecycle_unipay_tmp.json'),
    project=proyecto,
    gbq_client=gbq_client,
    if_exists='append')

if __name__ == '__main__':
    main()






