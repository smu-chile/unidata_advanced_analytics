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

parser.add_argument(
    '--partition_value', type=str,
    help='AWS Table partition value'
)


# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
SQL_QUERIES = {
    'dim_cycle_dh': """
    SELECT
    ORGANIZATION_ID,
    CAMPAIGN_ID,
    CAMPAIGN_TYPE_ID,
    CYCLE_ID,
    CYCLE_NUMBER,
    CYCLE_DESCRIPTION,
    START_DATE,
    END_DATE,
    SOURCE_ID,
    LOAD_DATE,
    LOAD_OWNER
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_CYCLE_DH
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
    # Load data from SharePoint to pandas DataFrame
    logging.info('Load the file to DataFrame')

    sales_item = readBigQuery(
        query=SQL_QUERIES['dim_cycle_dh'],
        user='csotob',
        gbq_client = Client()
    )
    column_types =  ['Int64','Int64', 'Int64', 'Int64', 'Int64',
                      'str', 'str', 'str', 'Int64', 'str', 'str']

    table_name = 'TMP_LAB_SMU_DIM_CYCLE_DH_GCP'

    # Upload to S3
    boto3_session=boto3.Session(
            **getSecret(
                project=gcp_project_id,
                secret_name='bdaa_aws_credentials'  # noqa: S106
            )
        )
    moveDataframeToS3(df_file=sales_item,
                      landing_bucket=landing_bucket,
                      landing_path=landing_path,
                      table_name=table_name,
                      column_types=column_types,
                      boto3_session=boto3_session
                      )

    logging.info(f'File successfully uploaded to: {landing_bucket}/{landing_path}')



if __name__ == '__main__':
    main()
