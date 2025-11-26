# Default
import os
import logging
import argparse
import platform
from logging import config

# pip
import boto3
from google.cloud.bigquery import Client


# Local testing support
if 'windows' in platform.platform().lower():
    import sys
    sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
# Own
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

parser.add_argument(
    '--partition_value', type=str,
    help='AWS Table partition value'
)


# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'sales_item': """
    MARKET_BASKET_KEY AS MARKET_BASKET_KEY_GCP,
    TXN_KEY AS BASKET_ID,
    NULL AS MARKET_BASKET_KEY,
    ITM_TXN_FCN_TP_DSC,
    STORE_ID,
    TRANSACTION_DATE,
    TRANSACTION_TIME,
    SKU_PRODUCT AS PRODUCT_ID,
    EAN AS UPC,
    UNIT_PRICE,
    QUANTITY,
    VALUE,
    WEIGHT,
    UNIDAD_DE_MEDIDA AS UNIT_OF_MEASURE,
    NULL AS PROMOTION_FLAG,
    PDA_CUSTOMER_KEY AS CUSTOMER_KEY,
    TRANSACTION_TYPE,
    DISCOUNT_VALUE,
    QUANTITY_SU
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_SALES_ITEM
    left join cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID id using (customer_key)
    WHERE TRANSACTION_DATE = '${execution_date}%'
    """,
})

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parameters
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    partition_value: str = args['partition_value']

    # Load data from SharePoint to pandas DataFrame
    logging.info('Load the file to DataFrame')

    sales_item = readBigQuery(
        query=SQL_QUERIES['sales_item'].substitute(
            execution_date = execution_date
        ),
        user='csotob',
        gbq_client = Client()
    )
    column_types = ['str', 'Int64', 'str', 'str', 'str', 'str', 'str',
                    'str', 'float', 'float', 'float', 'float', 'str', 'str',
                    'Int64', 'str', 'float', 'float']
    if sales_item.shape[1] != len(column_types):
        err_msg = ("Enforced column types list doesn't have "
                    'the same column size than DataFrame. '
                    f'df col lenght is {sales_item.shape[1]} while type '
                    f'list is {len(column_types)}')
        raise Exception(err_msg)
    # Formatting
    sales_item = sales_item.astype(dict(zip(sales_item.columns, column_types)))

    landing_bucket = 'smu-datalake-test-landing'
    landing_path = 'views/datascience'
    table_name = 'TMP_LAB_SMU_SALES_ITEM_GCP'

    sales_item.to_csv(f'{table_name}_{partition_value}.csv.gz',
                        header=False, index=False, sep='|', compression='gzip',
                        mode='w')

    # Upload to S3
    s3_client = boto3.client('s3')
    landing_uri = f'{landing_path}/{table_name}/{table_name}_{partition_value}.csv.gz'
    s3_client.upload_file(f'{table_name}_{partition_value}.csv.gz',
                              landing_bucket, landing_uri)
    logging.info(f'File successfully uploaded to: {landing_uri}')
    os.remove(f'{table_name}_{partition_value}.csv.gz')


if __name__ == '__main__':
    main()
