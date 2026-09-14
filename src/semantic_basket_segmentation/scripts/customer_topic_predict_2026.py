# Default
from __future__ import annotations

import gc
import io  # noqa: F401

# Pip
import os
import logging
import argparse
import posixpath  # noqa: F401
import unicodedata  # noqa: F401
from string import Template  # noqa: F401
from logging import config
from textwrap import dedent  # noqa: F401
from functools import partial  # noqa: F401

import numpy as np  # noqa: F401
import pandas as pd
from google.cloud import storage, bigquery  # noqa: F401
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


# -------------------------------------------------------------------------
# SQL Queries
# -------------------------------------------------------------------------

SQL_QUERIES = QueryDict({
    'baskets_topics_k9':
    """
    SELECT
        CUSTOMER_KEY,
        AVG(SALUDABLES) AS SALUDABLES,
        AVG(DESPENSA) AS DESPENSA,
        AVG(CELEBRACION) AS CELEBRACION,
        AVG(COLACIONES_GALLETAS) AS COLACIONES_GALLETAS,
        AVG(PANADERIA_DESAYUNOS) AS PANADERIA_DESAYUNOS,
        AVG(FRUTAS_VERDURAS) AS FRUTAS_VERDURAS,
        AVG(LIMPIEZA_HIGIENE) AS LIMPIEZA_HIGIENE,
        AVG(LACTEOS_POSTRES) AS LACTEOS_POSTRES,
        AVG(SNACKS_BEBIDAS) AS SNACKS_BEBIDAS
    FROM `${gcp_project}.SEMANTIC_BASKET_SEGMENTATION.SEMANTIC_BASKET_TOPIC_K9`
    WHERE CUSTOMER_KEY IN (
        SELECT DISTINCT CUSTOMER_KEY
        FROM `${gcp_project}.SEMANTIC_BASKET_SEGMENTATION.SEMANTIC_BASKET_TOPIC_K9`
        WHERE FECHA_CARGA = '${fecha_carga}'
    )
    AND FECHA_CARGA >= '${fecha_carga_2m}'
    AND FECHA_CARGA <= '${fecha_carga}'
    GROUP BY CUSTOMER_KEY
    """,

    'baskets_topics_k6':
    """
    SELECT
        CUSTOMER_KEY,
        AVG(FRUTAS_VERDURAS) AS FRUTAS_VERDURAS,
        AVG(SALUDABLES) AS SALUDABLES,
        AVG(COLACIONES_GALLETAS) AS COLACIONES_GALLETAS,
        AVG(HOGAR) AS HOGAR,
        AVG(CELEBRACION) AS CELEBRACION,
        AVG(PANADERIA_DESAYUNOS) AS PANADERIA_DESAYUNOS
    FROM `${gcp_project}.SEMANTIC_BASKET_SEGMENTATION.SEMANTIC_BASKET_TOPIC_K6`
    WHERE CUSTOMER_KEY IN (
        SELECT DISTINCT CUSTOMER_KEY
        FROM `${gcp_project}.SEMANTIC_BASKET_SEGMENTATION.SEMANTIC_BASKET_TOPIC_K6`
        WHERE FECHA_CARGA = '${fecha_carga}'
    )
    AND FECHA_CARGA >= '${fecha_carga_2m}'
    AND FECHA_CARGA <= '${fecha_carga}'
    GROUP BY CUSTOMER_KEY
    """
})


# -------------------------------------------------------------------------
# Functions
# -------------------------------------------------------------------------

def clasificar_dominante(row):
    valores = row.sort_values(ascending=False)
    p1 = valores.iloc[0]
    p2 = valores.iloc[1]

    if (p1 - p2 > 0.1):
        return valores.index[0]
    return 'MIXTO'


# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------

def main() -> None:
    usuario = 'semantic_basket_segmentation'
    # parse input variables
    args = vars(parser.parse_args())
    gcp_project: str = args['project_id']
    execution_date: str = args['execution_date']

    execution_date = pd.to_datetime(execution_date[:8] + '01').strftime('%Y-%m-%d')
    execution_date_2m = (
        pd.to_datetime(execution_date[:8] + '01') - pd.DateOffset(months=2)
    ).strftime('%Y-%m-%d')
    monthid = pd.to_datetime(execution_date[:8] + '01').strftime('%Y%m')

    logging.info(f'execution_date: {execution_date}')
    logging.info(f'execution_date_2m: {execution_date_2m}')
    logging.info(f'monthid: {monthid}')

    # Set gbq client for all subsequent queries
    gbq_client = Client()

    logging.info('Inicia Proceso Calculo Topicos K9')

    # Get the baskets for every customer in the month
    baskets_topics_k9 = readBigQuery(SQL_QUERIES['baskets_topics_k9'].substitute(
        gcp_project = gcp_project,
        fecha_carga = execution_date,
        fecha_carga_2m = execution_date_2m
        ),
    user = usuario,
    gbq_client = gbq_client
    )

    baskets_topics_k9_numeric = baskets_topics_k9.select_dtypes(include=['float64'])

    baskets_topics_k9['TOPIC'] = baskets_topics_k9_numeric.apply(
        clasificar_dominante, axis=1
    )

    baskets_topics_k9['MONTHID'] = monthid
    baskets_topics_k9['FECHA_CARGA'] = execution_date

    deleteFromTable(
        table_ref = f'{gcp_project}.SEMANTIC_BASKET_SEGMENTATION.SEMANTIC_CUSTOMER_TOPIC_K9',
        where_clause = f"FECHA_CARGA = '{execution_date}'",
        gbq_client = gbq_client,
    )

    uploadFrame(
        baskets_topics_k9[['CUSTOMER_KEY','TOPIC','MONTHID','FECHA_CARGA']],
        table_ddl_json_path=os.path.join('gbq_objects','semantic_customer_topic_k9.json'),
        project=gcp_project,
        gbq_client=gbq_client,
        if_exists='append'
    )

    del baskets_topics_k9
    del baskets_topics_k9_numeric
    gc.collect()

    logging.info('Finaliza Proceso Calculo Topicos K9')

    logging.info('Inicia Proceso Calculo Topicos K6')

    # Get the baskets for every customer (mixed) in the month
    baskets_topics_k6 = readBigQuery(SQL_QUERIES['baskets_topics_k6'].substitute(
        gcp_project = gcp_project,
        fecha_carga = execution_date,
        fecha_carga_2m = execution_date_2m
        ),
    user = usuario,
    gbq_client = gbq_client
    )

    baskets_topics_k6_numeric = baskets_topics_k6.select_dtypes(include=['float64'])

    baskets_topics_k6['TOPIC'] = baskets_topics_k6_numeric.apply(
        clasificar_dominante, axis=1
    )

    baskets_topics_k6['MONTHID'] = monthid
    baskets_topics_k6['FECHA_CARGA'] = execution_date

    deleteFromTable(
        table_ref = f'{gcp_project}.SEMANTIC_BASKET_SEGMENTATION.SEMANTIC_CUSTOMER_TOPIC_K6',
        where_clause=f"FECHA_CARGA = '{execution_date}'",
        gbq_client=gbq_client,
    )

    uploadFrame(
        baskets_topics_k6[['CUSTOMER_KEY','TOPIC','MONTHID','FECHA_CARGA']],
        table_ddl_json_path=os.path.join('gbq_objects','semantic_customer_topic_k6.json'),
        project=gcp_project,
        gbq_client=gbq_client,
        if_exists='append'
    )

    logging.info('Finaliza Proceso Calculo Topicos K6')


if __name__ == '__main__':
    main()

