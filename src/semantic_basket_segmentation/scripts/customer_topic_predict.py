# Default
from __future__ import annotations

# Pip
import io
import os
import pickle
import logging
import argparse
from string import Template  # noqa: F401
from logging import config  # noqa: F401
from textwrap import dedent  # noqa: F401
from functools import partial  # noqa: F401

import numpy as np
import pandas as pd
import sklearn  # noqa: F401
from google.cloud import storage, bigquery  # noqa: F401
from google.cloud.bigquery import Client

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    uploadFrame,
    readBigQuery,
    deleteFromTable,
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
    '--rollback_months', default=12, type=int,
    help='Number of months of transactions to look behind'
)
parser.add_argument(
    '--batch_size', default=1000000, type=int,
    help='Maximum number of transactions to be classified at a given moment'
)


# -------------------------------------------------------------------------
# SQL Queries
# -------------------------------------------------------------------------

SQL_QUERIES = QueryDict({
    'mean_topic_baskets_idx':
    """
    SELECT
        DENSE_RANK() OVER (ORDER BY CUSTOMER_KEY) AS CUSTOMER_KEY_IDX,
        CUSTOMER_KEY,
        AVG(TOPIC_0) AS TOPIC_0,
        AVG(TOPIC_1) AS TOPIC_1,
        AVG(TOPIC_2) AS TOPIC_2,
        AVG(TOPIC_3) AS TOPIC_3,
        AVG(TOPIC_4) AS TOPIC_4,
        AVG(TOPIC_5) AS TOPIC_5
    FROM ${gcp_project}.SEMANTIC_BASKET_SEGMENTATION.SEMANTIC_BASKET_TOPIC
    WHERE
        MONTHID >= '${partition_start}'
        AND MONTHID <= '${partition_end}'
    GROUP BY CUSTOMER_KEY
    """,

    'max_idxs':
    """
    SELECT MAX(CUSTOMER_KEY_IDX) AS MAX_IDX
    FROM ${gcp_project}.TMP.TMP_MEAN_TOPIC_BASKETS_IDX
    """,

    'semantic_baskets_idx_batch':
    """
    SELECT
        CUSTOMER_KEY,
        TOPIC_0,
        TOPIC_1,
        TOPIC_2,
        TOPIC_3,
        TOPIC_4,
        TOPIC_5
    FROM ${gcp_project}.TMP.TMP_MEAN_TOPIC_BASKETS_IDX
    WHERE
        CUSTOMER_KEY_IDX >= ${idx_start}
        AND CUSTOMER_KEY_IDX < ${idx_end}
    """
})

# -------------------------------------------------------------------------
# Functions
# -------------------------------------------------------------------------
def load_pickle_from_gcs(bucket_name, source_blob_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    buffer = io.BytesIO()
    blob.download_to_file(buffer)

    buffer.seek(0)
    return pickle.load(buffer)  # noqa: S301

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    usuario = 'semantic_basket_segmentation'
    # parse input variables
    args = vars(parser.parse_args())
    gcp_project: str = args['project_id']
    execution_date: str = args['execution_date']
    rollback_months: int = args['rollback_months']
    batch_size: int = args['batch_size']

    execution_date = pd.to_datetime(execution_date[:8] + '01').strftime('%Y-%m-%d')
    monthid = pd.to_datetime(execution_date[:8] + '01').strftime('%Y%m')

    # Construct the baskets dates to be reviewed
    partition_start = (pd.to_datetime(execution_date)
                        - pd.offsets.DateOffset(months=rollback_months)).strftime('%Y%m')
    partition_end = (pd.to_datetime(execution_date)
                        - pd.offsets.DateOffset(months=1)).strftime('%Y%m')

    logging.info(f'execution_date: {execution_date}')
    logging.info(f'monthid: {monthid}')
    logging.info(f'partition_start: {partition_start}')
    logging.info(f'partition_end: {partition_end}')

    # Set gbq client for all subsequent queries
    gbq_client = Client()

    bucket_name = 'cl-bigdata-analytics-preprod-us-sandbox-models'
    source_blob_name = 'SEMANTIC_BASKET_SEGMENTATION/WEIGHTS/kmeans_Topicos_7C_20230719_V2.pkl'

    model = load_pickle_from_gcs(
        bucket_name=bucket_name,
        source_blob_name=source_blob_name
    )

    # Mapping topic_id to semantic name
    lda_cluster_dict = {
        0: 'Snacks Salados, Cerveza y Carnes',
        1: 'Galletas y colaciones',
        2: 'Saludables',
        3: 'Verduras y Frutas',
        4: 'Limpieza e Higene Personal',
        5: 'Despensa'
    }

    # Mapping cluster_id to semantic name
    kmeans_cluster_dict = {
        0: lda_cluster_dict[5],
        1: 'Mixto',
        2: lda_cluster_dict[1],
        3: lda_cluster_dict[2],
        4: lda_cluster_dict[0],
        5: lda_cluster_dict[3],
        6: lda_cluster_dict[4]
    }

    # Recreate the indexed and averaged topic basket table
    _ = createTableAsSelect(
    query=SQL_QUERIES['mean_topic_baskets_idx'].substitute(
        gcp_project = gcp_project,
        partition_start=partition_start,
        partition_end=partition_end
        ),
    table_ref=f'{gcp_project}.TMP.TMP_MEAN_TOPIC_BASKETS_IDX',
    gbq_client=gbq_client,
    use_legacy_sql = False,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition = 'WRITE_TRUNCATE'
    )

    # Get the number of clients in the table
    max_idx = readBigQuery(SQL_QUERIES['max_idxs'].substitute(
        gcp_project = gcp_project
        ),
    user = usuario,
    gbq_client = gbq_client
    )['MAX_IDX'].iloc[0]

    # Total number of batches
    total_batches = int(np.ceil(max_idx / batch_size))

    # Remove past run if needed
    deleteFromTable(
    table_ref='cl-bigdata-analytics-preprod.SEMANTIC_BASKET_SEGMENTATION.SEMANTIC_CUSTOMER_TOPIC',
    where_clause=f"FECHA_CARGA = '{execution_date}'",
    gbq_client=gbq_client,
    )

    # Iterate through batches
    for n_batch in range(total_batches):
        print('--------------------------------------------------------')
        print(f'Batch {n_batch + 1} of {total_batches}')
        print('--------------------------------------------------------')
        print(f'Batch indexes: [{n_batch * batch_size}, {(n_batch * batch_size) + batch_size}[')

        # Get the topic baskets for a batch of customers
        mean_topic_baskets_batch = readBigQuery(
            SQL_QUERIES['semantic_baskets_idx_batch'].substitute(
                gcp_project = gcp_project,
                idx_start=n_batch * batch_size,
                idx_end=(n_batch * batch_size) + batch_size,
            ),
        user = usuario,
        gbq_client = gbq_client
        )

        # Calculate the principal topic
        mean_topic_baskets_batch['MAIN_TOPIC'] = model.predict(
            mean_topic_baskets_batch[
                [f'TOPIC_{i}' for i in range(6)]
            # TODO(ecastrot): Remove when new model is trainned
            ].rename(
                columns={f'TOPIC_{k}': v for k, v in lda_cluster_dict.items()}
            )[model.feature_names_in_]
        )

        # Change the kmeans cluster number to semantic value
        mean_topic_baskets_batch['MAIN_TOPIC'] = mean_topic_baskets_batch[
            'MAIN_TOPIC'
        ].replace(
            kmeans_cluster_dict
        )

        mean_topic_baskets_batch['MONTHID'] = monthid
        mean_topic_baskets_batch['FECHA_CARGA'] = execution_date

        # Upload
        uploadFrame(
        mean_topic_baskets_batch[['CUSTOMER_KEY','MAIN_TOPIC',
                                'MONTHID','FECHA_CARGA']],
        table_ddl_json_path=os.path.join('gbq_objects','semantic_customer_topic.json'),
        project=gcp_project,
        gbq_client=gbq_client,
        if_exists='append'
        )


if __name__ == '__main__':
    main()




