# Default
from __future__ import annotations

# Pip
import os
import logging
import argparse
from string import Template  # noqa: F401
from logging import config  # noqa: F401
from textwrap import dedent  # noqa: F401
from functools import partial

import numpy as np
import pandas as pd
from gensim import corpora
from google.cloud import storage, bigquery  # noqa: F401
from gensim.models import LdaModel
from gensim.matutils import corpus2csc
from google.cloud.bigquery import Client

from common.constants import LOGGING_CONFIG  # noqa: F401
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    uploadFrame,
    readBigQuery,
    deleteFromTable,
)


# -------------------------------------------------------------------------
# Config
# -------------------------------------------------------------------------
# Parser
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
    '--batch_size', default=1000000, type=int,
    help='Maximum number of transactions to be classified at a given moment'
)


# -------------------------------------------------------------------------
# SQL Queries
# -------------------------------------------------------------------------

SQL_QUERIES = QueryDict({
    'semantic_customer_baskets':
    """
    SELECT
        a.CUSTOMER_KEY,
        c.MARKET_BASKET_KEY,
        MAX(a.TRANSACTION_DATE) TRANSACTION_DATE,
        array_agg(distinct SUB_CATEGORY_DESCRIPTION) as SUB_CATEGORY_DESCRIPTION

    FROM (
        SELECT *
        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_BASKET`
        WHERE
            TRANSACTION_DATE >=  CAST(FORMAT_DATE('%Y-%m-%d', DATE_SUB(CAST('${transaction_date}' AS DATE), INTERVAL 1 MONTH)) AS DATE)
            AND TRANSACTION_DATE < CAST(FORMAT_DATE('%Y-%m-%d', CAST('${transaction_date}' AS DATE)) AS DATE)
    ) a

    INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE` b
    ON a.STORE_ID = b.STORE_ID

    INNER JOIN (
        SELECT *
        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM`
        WHERE
            TRANSACTION_DATE >=  CAST(FORMAT_DATE('%Y-%m-%d', DATE_SUB(CAST('${transaction_date}' AS DATE), INTERVAL 1 MONTH)) AS DATE)
            AND TRANSACTION_DATE < CAST(FORMAT_DATE('%Y-%m-%d', CAST('${transaction_date}' AS DATE)) AS DATE)
    ) c
    ON a.MARKET_BASKET_KEY = c.MARKET_BASKET_KEY

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
        AND a.channel IN ('SALA','E-COMMERCE')
        AND e.business_name NOT IN ('SERVICIOS COMERCIALES', 'NO RETAIL')
        AND c.value > 0
        AND c.transaction_type IN ('TN','TF','BX','B','BE','F','NC')
        AND a.itm_txn_fcn_tp_dsc = 'V'
    GROUP BY 1, 2
    """ # noqa: E501
})

# -------------------------------------------------------------------------
# Functions
# -------------------------------------------------------------------------
def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)


# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    usuario = 'basket_topic_predict'
    # parse input variables
    args = vars(parser.parse_args())
    gcp_project: str = args['project_id']
    execution_date: str = args['execution_date']
    batch_size: int = args['batch_size']

    logging.info(f'execution_date: {execution_date}')

    # Set gbq client for all subsequent queries
    gbq_client = Client()

    # Configuración
    bucket_name = 'cl-bigdata-analytics-preprod-us-sandbox-models'
    remote_folder = 'SEMANTIC_BASKET_SEGMENTATION/WEIGHTS/'
    local_dir = '/tmp/lda_model/'  # noqa: S108

    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    files_to_download = [
        'LDA_Topics_6.model',
        'LDA_Topics_6.model.id2word',
        'LDA_Topics_6.model.state',
        'LDA_Topics_6.model.expElogbeta.npy',
        'kmeans_Topicos_7C_20230719.pkl'
    ]

    for file_name in files_to_download:
        download_blob(bucket_name, f'{remote_folder}{file_name}', f'{local_dir}{file_name}')

    lda = LdaModel.load(os.path.join(local_dir, 'LDA_Topics_6.model'))
    dictionary = corpora.Dictionary.load(os.path.join(local_dir, 'LDA_Topics_6.model.id2word'))

    # Get the baskets for every customer (mixed) in the month
    semantic_baskets = readBigQuery(SQL_QUERIES['semantic_customer_baskets'].substitute(
        gcp_project = gcp_project,
        transaction_date = execution_date
        ),
    user = usuario,
    gbq_client = gbq_client
    ).reset_index(drop=True)

    total_batches = np.ceil(len(semantic_baskets) / batch_size)
    logging.info(f'Total batches: {total_batches}')

    semantic_baskets_batches = semantic_baskets.groupby(
    np.arange(len(semantic_baskets)) // batch_size
    )

    deleteFromTable(
    table_ref='cl-bigdata-analytics-preprod.SEMANTIC_BASKET_SEGMENTATION.SEMANTIC_BASKET_TOPIC',
    where_clause=f"FECHA_CARGA = '{execution_date}'",
    gbq_client=gbq_client,
    )


    # Retrieve
    for n_batch, semantic_baskets_batch in semantic_baskets_batches:
        logging.info(f'Actual batch: {n_batch + 1}')

        # Construct the predicted DataFrame
        topic_baskets = pd.DataFrame(
            # Uncompress the LDA predictions from a column of
            # [(topic0, percent_of_topic0_in_doc0, topic1,
            # percent_of_topic1_in_doc0, ...), ...]
            # to a matrix with dim [n_topics, n_docs]
            corpus2csc(
                # Get the SKU descriptions
                semantic_baskets_batch[
                    'SUB_CATEGORY_DESCRIPTION'
                # Vectorize as Bag of Words (BoW)
                ].copy().apply(
                    dictionary.doc2bow
                # Predict the topics with LDA
                ).apply(
                    partial(lda.get_document_topics, minimum_probability=0)
                )
            # Transpose to dim [n_docs, n_topics]
            ).T.toarray(),
            index=semantic_baskets_batch.index
        # Add the customer_id, idx, basket_id and transaction_date cols
        ).join(
            semantic_baskets_batch[
                ['CUSTOMER_KEY', 'MARKET_BASKET_KEY', 'TRANSACTION_DATE']
            ],
            how='inner'
        )

        topic_baskets['FECHA_CARGA'] = execution_date

        uploadFrame(
        topic_baskets[['CUSTOMER_KEY','MARKET_BASKET_KEY','TRANSACTION_DATE',
                    0,1,2,3,4,5,'FECHA_CARGA']],
        table_ddl_json_path=os.path.join('gbq_objects','semantic_basket_topic.json'),
        project=gcp_project,
        gbq_client=gbq_client,
        if_exists='append')




