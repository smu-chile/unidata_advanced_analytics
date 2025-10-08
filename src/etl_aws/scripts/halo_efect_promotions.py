# Default
import logging
import argparse
from logging import config

# pip
import awswrangler as wr
from boto3 import Session

# Own
import common.office365_extended.sharepoint as sp_extended
from common.constants import LOGGING_CONFIG
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
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parameters
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']

    # Load data from SharePoint to pandas DataFrame
    logging.info('Load the file to DataFrame')
    sp_file = sp_extended.SharePointFile(
        **getSecret(
            'bdaa_sharepoint_credentials', 'cl-bigdata-analytics'
        ),
        server_relative_path=(
            '/sites/'
            'BigDatayAdvancedAnalytics/'
            'Documentos%20compartidos/'
            'Pricing/'
            'Evaluacion%20Promociones/'
            'promotions_to_evaluate.xlsx'
        )
    ).toFrame()

    # Clean the file
    logging.info('Cleanning the file')
    sp_file = sp_file.fillna(method='ffill', axis=0)

    # Data to AWS
    logging.info('Uploading to AWS')
    wr.s3.to_csv(
        sp_file,
        path=(
            's3://smu-datalake-test-landing/'
            'views/'
            'datascience/'
            'TMP_LAB_SMU_DIM_PROMOTIONS_TO_EVALUATE.csv.gz'
        ),
        header=False,
        index=False,
        sep='|',
        compression='gzip',
        boto3_session=Session(
            **getSecret(
                project=gcp_project_id,
                secret_name='bdaa_aws_credentials'  # noqa: S106
            )
        )
    )
    logging.info('Done! :)')


if __name__ == '__main__':
    main()
