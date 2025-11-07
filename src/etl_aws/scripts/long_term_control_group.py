# Default
import os
import logging
import argparse
from logging import config

# pip
from boto3 import Session
from google.cloud.bigquery import Client

# Own
import common.gcp_extended.bigquery as gbq_extended
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.aws_extended.athena import readAthenaQuery
from common.gcp_extended.secretsmanager import getSecret


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
SQL_QUERIES = QueryDict({
    'ltcg_data': """
    SELECT
        ORGANIZATION_ID AS ORG_IP_ID,
        CUSTOMER_ID AS CUSTOMER_KEY,
        INICIO_PERIODO,
        FIN_PERIODO,
        INICIO_VIGENCIA,
        FIN_VIGENCIA,
        FECHA_CARGA,
        SUBSTR(P_WEEK, 1, 4) AS P_YEAR
    FROM dev_perm.long_term_control_group
    WHERE p_week LIKE '${year}%'
    """,
})

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parameters
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    execution_date: str = args['execution_date']

    # Load data from SharePoint to pandas DataFrame
    logging.info('Load the file to DataFrame')

    ltcg_data = readAthenaQuery(
        query=SQL_QUERIES['ltcg_data'].substitute(
            year=execution_date[:4]
        ),
        user='ecastrot',
        database='dev_temp',
        workgroup='smu-datalake-test-workgroup',
        boto3_session=Session(
            **getSecret(
                project=gcp_project_id,
                secret_name='bdaa_aws_credentials'  # noqa: S106
            )
        ),
    )

    gbq_extended.uploadFrame(
        ltcg_data,
        table_ddl_json_path=os.path.join('gbq_objects', 'long_term_control_group.json'),
        project=gcp_project_id,
        gbq_client=Client(),
        if_exists='replace',
    )


if __name__ == '__main__':
    main()
