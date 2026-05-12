import os
import logging
import argparse
from logging import config

from google.cloud.bigquery import Client

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import uploadFrame, readBigQuery


# -------------------------------------------------------------------------
# Config
# -------------------------------------------------------------------------
config.dictConfig(LOGGING_CONFIG)
parser = argparse.ArgumentParser()
parser.add_argument(
    '--project_id',
    type=str,
    help='GCP project'
)
parser.add_argument(
    '--execution_date',
    type=str,
    help='Execution date'
)

# -------------------------------------------------------------------------
# SQL
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({

    'dim_product':
    """
    SELECT *
    FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_PRODUCT`
    LIMIT 100
    """
})

# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main() -> None:
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    logging.info('Starting DIM_PRODUCT ingestion...')
    gbq_client = Client(project=gcp_project_id)
    logging.info('Reading BigQuery source table...')
    df_dim_product = readBigQuery(
        SQL_QUERIES['dim_product'].substitute(),
        user='ilopeze',
        gbq_client=gbq_client
    )

    logging.info(f'Total rows extracted: {len(df_dim_product)}')
    logging.info('Uploading RAW table...')
    uploadFrame(
        df=df_dim_product,
        table_ddl_json_path=os.path.join(
            'gbq_objects',
            'dim_product_raw.json'
        ),
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='replace'
    )

    logging.info('DIM_PRODUCT_RAW upload completed!')

if __name__ == '__main__':
    main()
