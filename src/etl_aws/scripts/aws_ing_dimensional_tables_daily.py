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




# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
SQL_QUERIES = {
    'customer_app_users': """
    SELECT
    PDA_CUSTOMER_KEY as CUSTOMER_ID,
    MONTH_ID,
    CREATED_AT,
    CHANNEL,
    GRANT_TYPE,
    MAIL,
    MOBILE,
    PUSH,
    UNIMARC_LOGGED,
    OK_MARKET_LOGGED,
    MAYORISTA_10_LOGGED,
    ALVI_LOGGED,
    UNIMARC_LOGGED_AT,
    OKM_LOGGED_AT,
    M10_LOGGED_AT,
    ALVI_LOGGED_AT,
    LOAD_DATE
    FROM
    cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_APP_USERS
    left join cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID id using (customer_key)
    """
}

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parameters
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']

    landing_bucket = 'smu-datalake-test-landing'
    landing_path = 'views/datascience'
    dimensional_list = ['customer_app_users']

    column_types ={
        'customer_app_users' :  ['Int64', 'Int64', 'str', 'str',
                                 'str', 'Int64', 'Int64', 'Int64',
                                 'Int64', 'Int64', 'Int64', 'Int64',
                                 'str', 'str', 'str', 'str', 'str'],
    }

    table_names ={
        'customer_app_users'  : 'TMP_LAB_SMU_FACT_MONTH_CUSTOMER_APP_USERS_GCP'
    }

    boto3_session=boto3.Session(
            **getSecret(
                project=gcp_project_id,
                secret_name='bdaa_aws_credentials'  # noqa: S106
            )
        )

    for dimensional_table in dimensional_list :
        # Read BigQuery Query
        logging.info(f'Loading BQ Result into Dataframe for {dimensional_table}')
        table_df = readBigQuery(
            query=SQL_QUERIES[dimensional_table],
            user='csotob',
            gbq_client = Client()
        )


        # Upload to S3
        logging.info(f'Uploading {dimensional_table} to S3')
        moveDataframeToS3(df_file=table_df,
                        landing_bucket=landing_bucket,
                        landing_path=landing_path,
                        table_name=table_names[dimensional_table],
                        column_types=column_types[dimensional_table],
                        boto3_session=boto3_session
                        )

    logging.info(f'File successfully uploaded to: {landing_bucket}/{landing_path}')



if __name__ == '__main__':
    main()
