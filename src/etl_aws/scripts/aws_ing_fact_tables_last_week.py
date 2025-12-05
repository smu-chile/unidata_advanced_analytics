# Default
import os
import logging
import argparse
import platform
from logging import config
from datetime import datetime as dt
from datetime import timedelta

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
    'customer_cycle': """
    SELECT
    CYCLE_ID,
    CUSTOMER_ID,
    INSERT_DATE,
    TARGET_GROUP,
    DH_SCORE,
    FECHA_INICIO_CYCLE,
    PERIODO_INICIO_CYCLE
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_CUSTOMER_CYCLE_DH
    WHERE FECHA_INICIO_CYCLE =  '${execution_date}'
    """,
    'customer_offer_cycle' : """
    SELECT
    CYCLE_ID,
    CUSTOMER_ID,
    OFFER_ID,
    OFFER_RANK,
    EAN,
    FECHA_INICIO_CYCLE,
    PERIODO_INICIO_CYCLE
    FROM  cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_CUSTOMER_OFFER_CYCLE_DH
    WHERE FECHA_INICIO_CYCLE = '${execution_date}'
    """,
    'offer_cycle' : """
    SELECT
    CYCLE_ID,
    OFFER_ID,
    DISCOUNT_ID,
    OFFER_DESCRIPTION,
    OFFER_TYPE_ID,
    OFFER_MAX_USES,
    DISCOUNT_PERC,
    CONTENIDO_MECANICA,
    FECHA_INICIO_CYCLE,
    PERIODO_INICIO_CYCLE
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_OFFER_CYCLE_DH
    WHERE FECHA_INICIO_CYCLE = '${execution_date}'
    """
})

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parameters
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    execution_date: str = args['project_id']
    partition_value: str = args['partition_value']
    execution_date = dt.strftime(
        dt.strptime(execution_date, '%Y-%m-%d') - timedelta(days=8),  # noqa: DTZ007
        '%Y-%m-%d'
    )
    partition_value = dt.strftime(
        dt.strptime(partition_value, '%Y%m%d') - timedelta(days=8),  # noqa: DTZ007
         '%Y%m%d'
    )
    landing_bucket = 'smu-datalake-test-landing'
    landing_path = 'views/datascience'

    boto3_session=boto3.Session(
                **getSecret(
                    project=gcp_project_id,
                    secret_name='bdaa_aws_credentials'  # noqa: S106
                )
            )
    # Load data from SharePoint to pandas DataFrame

    fact_list = ['customer_cycle','customer_offer_cycle','offer_cycle']
    column_types = {
        'customer_cycle' : ['Int64', 'Int64', 'str', 'Int64', 'Int64', 'str', 'str'],
        'customer_offer_cycle' :  ['Int64', 'Int64', 'Int64', 'Int64', 'Int64', 'str', 'str'],
        'offer_cycle' :  ['Int64', 'Int64', 'str', 'str', 'Int64', 'Int64', 'Int64', 'str',
                           'str', 'str']

    }
    table_names = {
        'customer_cycle' : 'TMP_LAB_SMU_FACT_CUSTOMER_CYCLE_GCP',
        'customer_offer_cycle' : 'TMP_LAB_SMU_FACT_CUSTOMER_OFFER_CYCLE_GCP',
        'offer_cycle' : 'TMP_LAB_SMU_FACT_OFFER_CYCLE_GCP'
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
