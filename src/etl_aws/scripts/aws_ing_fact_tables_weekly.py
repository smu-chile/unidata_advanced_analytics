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
    '--partition_week', type=str,
    help='AWS table partition week'
)


# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'customer_organization_outlier': """
    SELECT
        ORGANIZATION_ID,
        WEEK_ISO_ID,
        PDA_CUSTOMER_KEY as CUSTOMER_ID
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_WEEK_CUSTOMER_ORGANIZATION_OUTLIER ou
    left join cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID id
    on id.customer_key = ou.customer_id
    WHERE WEEK_ISO_ID = ${partition_id}
    """
})

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parameters
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    partition_week: str = args['partition_week']

    landing_bucket = 'smu-datalake-test-landing'
    landing_path = 'views/datascience'

    boto3_session=boto3.Session(
                **getSecret(
                    project=gcp_project_id,
                    secret_name='bdaa_aws_credentials'  # noqa: S106
                )
            )
    # Load data from SharePoint to pandas DataFrame

    fact_list = ['customer_organization_outlier']
    column_types = { 'customer_organization_outlier' : ['Int64','Int64', 'Int64']
    }
    table_names = {
        'customer_organization_outlier' : 'TMP_LAB_SMU_FACT_WEEK_CUSTOMER_ORGANIZATION_OUTLIER_GCP'
    }

    for fact_table in fact_list :
        logging.info(f'Load the file {fact_table} to DataFrame')

        df_fact = readBigQuery(
            query=SQL_QUERIES[fact_table].substitute(
                partition_value = partition_week
            ),
            user='csotob',
            gbq_client = Client()
        )

        logging.info(f'UpLoad the file {fact_table} to S3')

        moveDataframeToS3(df_file=df_fact,
                        landing_bucket=landing_bucket,
                        landing_path=landing_path,
                        table_name=table_names[fact_table],
                        partition_value=partition_week,
                        column_types=column_types[fact_table],
                        boto3_session=boto3_session
                        )

        logging.info(f'File successfully uploaded to: {landing_bucket}/{landing_path}')



if __name__ == '__main__':
    main()
