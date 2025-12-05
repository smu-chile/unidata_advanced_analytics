# Default
import os
import logging
import argparse
import platform
from logging import config

import boto3

# pip
from google.cloud.bigquery import Client

from common.gcp_extended.secretsmanager import getSecret


# Local testing support
if 'windows' in platform.platform().lower():
    import sys
    sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.aws_extended.athena import moveDataframeToS3
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
    SELECT
    TO_BASE64(MARKET_BASKET_KEY) AS MARKET_BASKET_KEY_GCP,
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
    WHERE TRANSACTION_DATE = '${execution_date}'
    """,
    'sales_basket' : """
    SELECT
    TXN_KEY AS BASKET_ID,
    TO_BASE64(MARKET_BASKET_KEY) AS MARKET_BASKET_KEY_GCP,
    NULL AS MARKET_BASKET_KEY,
    ITM_TXN_FCN_TP_DSC,
    STORE_ID,
    TRANSACTION_DATE,
    TRANSACTION_TIME,
    FNC_DOC_TP_DSC,
    TERMINAL_NUMBER AS LANE_NUMBER,
    NULL AS TRANSACTION_NUMBER,
    BASKET_QUANTITY,
    TOTAL_VALUE,
    BASKET_VALUE,
    NULL AS BASKET_PROMOTION_FLAG,
    PDA_CUSTOMER_KEY AS CUSTOMER_KEY,
    NULL AS TENDER_TYPE,
    CHANNEL
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_SALES_BASKET
    left join cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID id using (customer_key)
    WHERE TRANSACTION_DATE = '${execution_date}'
    """,
    'sales_discount' : """
    SELECT
    BASKET_ID,
    UPC,
    GEOPROMOTION_ID,
    DISCOUNT_VALUE,
    FECHA_DESCUENTO
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_SALES_DISCOUNT
    WHERE FECHA_DESCUENTO = '${execution_date}'
    """
})

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parameters
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    execution_date: str = args['execution_date']
    partition_value: str = args['partition_value']

    landing_bucket = 'smu-datalake-test-landing'
    landing_path = 'views/datascience'

    boto3_session=boto3.Session(
                **getSecret(
                    project=gcp_project_id,
                    secret_name='bdaa_aws_credentials'  # noqa: S106
                )
            )
    # Load data from SharePoint to pandas DataFrame

    fact_list = ['sales_item','sales_basket','sales_discount']
    column_types = {
        'sales_item' : ['str', 'str', 'Int64', 'str', 'str', 'str', 'str', 'str',
                    'str', 'float', 'float', 'float', 'float', 'str', 'str',
                    'Int64', 'str', 'float', 'float'],
        'sales_basket' : ['str', 'str', 'Int64', 'str', 'str', 'str', 'str', 'str',
                           'str', 'str', 'float', 'float', 'float', 'str',
                           'Int64', 'str', 'str'],
        'sales_discount' : ['str', 'str', 'str', 'float', 'str']

    }
    table_names = {
        'sales_item' : 'TMP_LAB_SMU_SALES_ITEM_GCP',
        'sales_basket' : 'TMP_LAB_SMU_SALES_BASKET_GCP',
        'sales_discount' : 'TMP_LAB_SMU_SALES_DISCOUNT_GCP'
    }

    for fact_table in fact_list :
        logging.info(f'Load the file {fact_table} to DataFrame')

        df_fact = readBigQuery(
            query=SQL_QUERIES[fact_table].substitute(
                execution_date = execution_date
            ),
            user='csotob',
            gbq_client = Client()
        )

        logging.info(f'UpLoad the file {fact_table} to S3')

        moveDataframeToS3(df_file=df_fact,
                        landing_bucket=landing_bucket,
                        landing_path=landing_path,
                        table_name=table_names[fact_table],
                        partition_value=partition_value,
                        column_types=column_types[fact_table],
                        boto3_session=boto3_session
                        )

        logging.info(f'File successfully uploaded to: {landing_bucket}/{landing_path}')



if __name__ == '__main__':
    main()
