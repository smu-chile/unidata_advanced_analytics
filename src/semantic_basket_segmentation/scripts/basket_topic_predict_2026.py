# Default
from __future__ import annotations

import gc
import io  # noqa: F401

# Pip
import os
import re
import logging
import argparse
import posixpath  # noqa: F401
import unicodedata
from string import Template  # noqa: F401
from logging import config
from textwrap import dedent  # noqa: F401
from functools import partial  # noqa: F401
from itertools import islice  # noqa: F401

import numpy as np
import spacy
import joblib
import pandas as pd
from google.cloud import storage, bigquery  # noqa: F401
from scipy.sparse import vstack  # noqa: F401
from google.cloud.bigquery import Client
from sklearn.decomposition import LatentDirichletAllocation  # noqa: F401

from common.constants import LOGGING_CONFIG  # noqa: F401
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
    'semantic_customer_baskets_k9':
    """
    SELECT
        a.CUSTOMER_KEY,
        c.TXN_KEY,
        a.BASKET_VALUE,
        MAX(a.TRANSACTION_DATE) TRANSACTION_DATE,
        CASE EXTRACT(DAYOFWEEK FROM a.TRANSACTION_DATE)
            WHEN 1 THEN 'DOMINGO'
            WHEN 2 THEN 'LUNES'
            WHEN 3 THEN 'MARTES'
            WHEN 4 THEN 'MIERCOLES'
            WHEN 5 THEN 'JUEVES'
            WHEN 6 THEN 'VIERNES'
            WHEN 7 THEN 'SABADO'
        END AS WEEKDAY_NAME,
        array_agg(distinct SUB_CATEGORY_DESCRIPTION) as SUB_CATEGORY_DESCRIPTION

    FROM (
        SELECT *
        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_BASKET`
        WHERE
            TRANSACTION_DATE >= CAST(FORMAT_DATE('%Y-%m-%d', DATE_SUB(CAST('${transaction_date}' AS DATE), INTERVAL 1 MONTH)) AS DATE)
            AND TRANSACTION_DATE < CAST(FORMAT_DATE('%Y-%m-%d', CAST('${transaction_date}' AS DATE)) AS DATE)
    ) a

    INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE` b
    ON a.STORE_ID = b.STORE_ID

    INNER JOIN (
        SELECT *
        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM`
        WHERE
            TRANSACTION_DATE >= CAST(FORMAT_DATE('%Y-%m-%d', DATE_SUB(CAST('${transaction_date}' AS DATE), INTERVAL 1 MONTH)) AS DATE)
            AND TRANSACTION_DATE < CAST(FORMAT_DATE('%Y-%m-%d', CAST('${transaction_date}' AS DATE)) AS DATE)
    ) c
    ON a.TXN_KEY = c.TXN_KEY

    INNER JOIN (
        SELECT
            SKU_PRODUCT,
            MAX(NEG_DSC) as BUSINESS_NAME,
            MAX(GRUPO_DSC) as SUB_CATEGORY_DESCRIPTION
        FROM `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT`
        GROUP BY 1
    ) e
    ON c.SKU_PRODUCT = e.SKU_PRODUCT

    LEFT JOIN (
        SELECT *
        FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
        WHERE canal_venta IN ('PEDIDOS YA','UBER EATS','RAPPI','RAPPI TURBO')
    ) outlier
    ON a.MARKET_BASKET_KEY = outlier.MARKET_BASKET_KEY

    WHERE
        outlier.MARKET_BASKET_KEY IS NULL
        AND b.store_banner = 'Unimarc'
        AND a.channel IN ('SALA')
        AND e.business_name NOT IN ('SERVICIOS COMERCIALES', 'NO RETAIL')
        AND c.value > 0
        AND c.transaction_type IN ('TN','TF','BX','B','BE','F','NC')
        AND a.itm_txn_fcn_tp_dsc = 'V'
    GROUP BY 1, 2, 3, 5
    """,  # noqa: E501

    'semantic_customer_baskets_k6':
    """
    WITH BASE AS (
    SELECT
        *,
        -- Verificamos si hay uniformidad absoluta (Regla 1)
        (SALUDABLES = DESPENSA
        AND DESPENSA = CELEBRACION
        AND CELEBRACION = COLACIONES_GALLETAS
        AND COLACIONES_GALLETAS = PANADERIA_DESAYUNOS
        AND PANADERIA_DESAYUNOS = FRUTAS_VERDURAS
        AND FRUTAS_VERDURAS = LIMPIEZA_HIGIENE
        AND LIMPIEZA_HIGIENE = LACTEOS_POSTRES
        AND LACTEOS_POSTRES = SNACKS_BEBIDAS) AS es_uniforme
    FROM `${gcp_project}.SEMANTIC_BASKET_SEGMENTATION.SEMANTIC_BASKET_TOPIC_K9`
    WHERE FECHA_CARGA = '${fecha_carga}'
    )

    SELECT
    CUSTOMER_KEY,
    TXN_KEY,
    BASKET_VALUE,
    TRANSACTION_DATE,
    WEEKDAY_NAME,
    SUB_CATEGORY_DESCRIPTION,

    -- Tópicos que permanecen iguales
    FRUTAS_VERDURAS,
    SALUDABLES,
    COLACIONES_GALLETAS,

    -- Lógica HOGAR: DESPENSA, LIMPIEZA_HIGIENE
    CASE
        WHEN es_uniforme THEN DESPENSA
        WHEN ((DESPENSA > ${umbral} OR LIMPIEZA_HIGIENE > ${umbral})) THEN
        (IF(DESPENSA > ${umbral}, DESPENSA, 0) + IF(LIMPIEZA_HIGIENE > ${umbral}, LIMPIEZA_HIGIENE, 0))
        ELSE GREATEST(DESPENSA, LIMPIEZA_HIGIENE)
    END AS HOGAR,

    -- Lógica CELEBRACION: CELEBRACION, SNACKS_BEBIDAS
    CASE
        WHEN es_uniforme THEN CELEBRACION
        WHEN (CELEBRACION > ${umbral} OR SNACKS_BEBIDAS > ${umbral}) THEN
        (IF(CELEBRACION > ${umbral}, CELEBRACION, 0) + IF(SNACKS_BEBIDAS > ${umbral}, SNACKS_BEBIDAS, 0))
        ELSE GREATEST(CELEBRACION, SNACKS_BEBIDAS)
    END AS CELEBRACION,

    -- Lógica PANADERIA_DESAYUNOS: PANADERIA_DESAYUNOS, LACTEOS_POSTRES
    CASE
        WHEN es_uniforme THEN PANADERIA_DESAYUNOS
        WHEN (PANADERIA_DESAYUNOS > ${umbral} OR LACTEOS_POSTRES > ${umbral}) THEN
        (IF(PANADERIA_DESAYUNOS > ${umbral}, PANADERIA_DESAYUNOS, 0) + IF(LACTEOS_POSTRES > ${umbral}, LACTEOS_POSTRES, 0))
        ELSE GREATEST(PANADERIA_DESAYUNOS, LACTEOS_POSTRES)
    END AS PANADERIA_DESAYUNOS,

    MONTHID,
    FECHA_CARGA

    FROM BASE
    """  # noqa: E501
})

# -------------------------------------------------------------------------
# Functions
# -------------------------------------------------------------------------

# Funcion para cargar el modelo LDA y Vectorizer desde Cloud Storage
def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

# Funcion de limpieza de las subcategorias
def limpieza_pre_spacy(texto):
    texto = unicodedata.normalize('NFD', texto).encode('ascii', 'ignore').decode('utf-8')
    texto = re.sub(r'[,./\-0-9]', ' ', texto).lower()
    return texto.strip()


# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------

def main() -> None:
    usuario = 'semantic_basket_segmentation_ecommerce'
    # parse input variables
    args = vars(parser.parse_args())
    gcp_project: str = args['project_id']
    execution_date: str = args['execution_date']

    execution_date = pd.to_datetime(execution_date[:8] + '01').strftime('%Y-%m-%d')
    monthid = pd.to_datetime(execution_date[:8] + '01').strftime('%Y%m')

    umbral = 2.0/9 # Factor / N° Topicos
    batch_size = 50000

    logging.info(f'execution_date: {execution_date}')
    logging.info(f'monthid: {monthid}')
    logging.info(f'umbral: {umbral}')
    logging.info(f'batch_size: {batch_size}')

    # Set gbq client for all subsequent queries
    gbq_client = Client()

    logging.info('Inicia Proceso Calculo Topicos K9')

    # Configuración
    bucket_name = 'cl-bigdata-analytics-preprod-us-sandbox-models'
    remote_folder = 'SEMANTIC_BASKET_SEGMENTATION/MODEL_2026/'
    local_dir = '/tmp/lda_model/'  # noqa: S108

    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    files_to_download = [
        'lda_model_k9_vectorizer_stopwords4_20260820.pkl',
        'vectorizer_stopwords4_20260820.pkl'
    ]

    for file_name in files_to_download:
        download_blob(bucket_name, f'{remote_folder}{file_name}', f'{local_dir}{file_name}')

    lda_model = joblib.load(os.path.join(local_dir, 'lda_model_k9_vectorizer_stopwords4_20260820.pkl'))  # noqa: E501
    vectorizer = joblib.load(os.path.join(local_dir, 'vectorizer_stopwords4_20260820.pkl'))

    logging.info(f'LDA Model: {lda_model}')
    logging.info(f'Vectorizer: {vectorizer}')

    # Get the baskets for every customer in the month
    semantic_baskets_k9 = readBigQuery(SQL_QUERIES['semantic_customer_baskets_k9'].substitute(
        gcp_project = gcp_project,
        transaction_date = execution_date
        ),
    user = usuario,
    gbq_client = gbq_client
    )

    baskets_pre_limpias = [
        [limpieza_pre_spacy(item) for item in basket]
        for basket in semantic_baskets_k9['SUB_CATEGORY_DESCRIPTION']
    ]

    nlp = spacy.load('es_core_news_sm', disable=['parser', 'ner', 'morphologizer'])

    productos_flat = []
    indices = []

    for basket in baskets_pre_limpias:
        start = len(productos_flat)
        productos_flat.extend(basket)
        end = len(productos_flat)
        indices.append((start, end))

    logging.info(f'Total subcategorías a lematizar: {len(productos_flat):,}')

    def lematizar_producto(doc):
        lemas = [token.lemma_ for token in doc if not token.is_space and len(token.lemma_) > 1]
        return '_'.join(lemas) if lemas else None

    productos_unicos = list(set(productos_flat))
    logging.info(f'Subcategorías únicas a lematizar: {len(productos_unicos):,}')

    logging.info('Lematizando con nlp.pipe()...')

    lema_dict = {}
    for texto_original, doc in zip(
        productos_unicos,
        nlp.pipe(productos_unicos, batch_size=batch_size, n_process=-1)
    ):
        lema_dict[texto_original] = lematizar_producto(doc)

    productos_lematizados = [lema_dict[p] for p in productos_flat]

    logging.info(f'Lematización completa: {len(productos_lematizados):,} productos procesados')

    baskets_ready = []

    for start, end in indices:
        productos_canasta = [
            p for p in productos_lematizados[start:end]
            if p is not None
        ]
        if productos_canasta:
            baskets_ready.append(productos_canasta)

    logging.info(f'Canastas listas: {len(baskets_ready):,}')

    corpus_sklearn = [' '.join(canasta) for canasta in baskets_ready]

    logging.info(f'Documentos: {len(corpus_sklearn):,}')


    dtm = vectorizer.transform(corpus_sklearn)
    topic_probabilities = lda_model.transform(dtm)

    df_probabilidades = pd.DataFrame(
        topic_probabilities,
        columns = [f'topico_{i}' for i in range(topic_probabilities.shape[1])],
        index = semantic_baskets_k9.index
    )

    semantic_baskets_k9 = pd.concat([semantic_baskets_k9, df_probabilidades], axis=1)

    del topic_probabilities, df_probabilidades
    gc.collect()

    semantic_baskets_k9['MONTHID'] = monthid
    semantic_baskets_k9['FECHA_CARGA'] = execution_date
    semantic_baskets_k9['SUB_CATEGORY_DESCRIPTION'] = semantic_baskets_k9['SUB_CATEGORY_DESCRIPTION'].apply(lambda x: ', '.join(x.tolist()) if isinstance(x, np.ndarray) else x)  # noqa: E501

    deleteFromTable(
        table_ref=f'{gcp_project}.SEMANTIC_BASKET_SEGMENTATION.SEMANTIC_BASKET_TOPIC_K9',
        where_clause=f"FECHA_CARGA = '{execution_date}'",
        gbq_client=gbq_client,
    )

    uploadFrame(
        semantic_baskets_k9,
        table_ddl_json_path=os.path.join('gbq_objects','semantic_basket_topic_k9.json'),
        project=gcp_project,
        gbq_client=gbq_client,
        if_exists='append'
    )

    logging.info('Finaliza Proceso Calculo Topicos K9')

    logging.info('Inicio Proceso Calculo Topicos K6')

    semantic_baskets_k6 = readBigQuery(SQL_QUERIES['semantic_customer_baskets_k6'].substitute(
        gcp_project = gcp_project,
        fecha_carga = execution_date,
        umbral = umbral
        ),
    user = usuario,
    gbq_client = gbq_client
    )

    topicos = [
        'FRUTAS_VERDURAS',
        'SALUDABLES',
        'COLACIONES_GALLETAS',
        'HOGAR',
        'CELEBRACION',
        'PANADERIA_DESAYUNOS'
    ]

    suma_fila = semantic_baskets_k6[topicos].sum(axis=1)
    semantic_baskets_k6[topicos] = semantic_baskets_k6[topicos].div(suma_fila, axis=0)

    deleteFromTable(
        table_ref=f'{gcp_project}.SEMANTIC_BASKET_SEGMENTATION.SEMANTIC_BASKET_TOPIC_K6',
        where_clause=f"FECHA_CARGA = '{execution_date}'",
        gbq_client=gbq_client,
    )

    uploadFrame(
        semantic_baskets_k6,
        table_ddl_json_path=os.path.join('gbq_objects','semantic_basket_topic_k6.json'),
        project=gcp_project,
        gbq_client=gbq_client,
        if_exists='append'
    )


if __name__ == '__main__':
    main()
